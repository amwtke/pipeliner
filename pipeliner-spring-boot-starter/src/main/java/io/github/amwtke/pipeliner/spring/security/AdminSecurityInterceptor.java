package io.github.amwtke.pipeliner.spring.security;

import io.github.amwtke.pipeliner.spring.PipelinerAdminController;
import io.github.amwtke.pipeliner.spring.security.PipelinerSecurityProperties.Role;
import io.github.amwtke.pipeliner.spring.security.PipelinerSecurityProperties.TokenEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.UrlPathHelper;

/**
 * Token check for the admin API, applied as a Spring MVC interceptor so it does not depend on URL patterns that change with
 * {@code spring.mvc.servlet.path} / the context path: it guards every handler of {@link PipelinerAdminController} and anything mapped under
 * /pg within the DispatcherServlet. GET/HEAD are open unless readRequiresToken; all other methods need an ADMIN token.
 * The authenticated token name is exposed as request attribute {@link #PRINCIPAL_ATTR}.
 */
public class AdminSecurityInterceptor implements HandlerInterceptor {
    public static final String PRINCIPAL_ATTR = "pipeliner.admin.principal";
    public static final String ALT_HEADER = "X-PG-Token";
    private static final Logger log = LoggerFactory.getLogger(AdminSecurityInterceptor.class);
    private static final Logger AUDIT = LoggerFactory.getLogger("pipeliner.audit");

    private static final class Cred {
        final String name;
        final byte[] token;
        final Role role;
        Cred(String name, String token, Role role) {
            this.name = name; this.token = token.getBytes(StandardCharsets.UTF_8); this.role = role;
        }
    }

    private static final java.util.regex.Pattern WHITESPACE = java.util.regex.Pattern.compile("\\s");
    private static final UrlPathHelper PATHS = new UrlPathHelper();

    private final List<Cred> creds = new ArrayList<>();
    private final boolean readRequiresToken;
    private final String header;

    public AdminSecurityInterceptor(PipelinerSecurityProperties p) {
        this.readRequiresToken = p.isReadRequiresToken();
        this.header = p.getHeader() == null || p.getHeader().trim().isEmpty() ? "Authorization" : p.getHeader().trim();
        for (TokenEntry t : p.getTokens()) {
            if (t.getToken() == null) continue;
            String token = t.getToken().trim();
            if (token.isEmpty()) continue;
            String name = t.getName() == null || t.getName().isEmpty() ? "token" : t.getName();
            if (!token.equals(t.getToken())) {
                log.warn("pipeliner admin token '{}' had leading/trailing whitespace (e.g. a newline from a secret file); it was trimmed", name);
            }
            if (WHITESPACE.matcher(token).find()) {
                log.warn("pipeliner admin token '{}' contains inner whitespace; clients trim what they send, so it can never match", name);
            }
            creds.add(new Cred(name, token, t.getRole() == null ? Role.VIEWER : t.getRole()));
            if (token.length() < 16) {
                log.warn("pipeliner admin token '{}' is shorter than 16 characters; use a longer random secret", name);
            }
        }
        if (creds.isEmpty()) {
            log.warn("pipeliner.admin.security is enabled but no tokens are configured: mutating /pg endpoints are disabled (403)");
        }
    }

    /** True when the request is dispatched to the admin controller or to a path under /pg (relative to the servlet mapping). */
    static boolean guards(HttpServletRequest req, Object handler) {
        if (handler instanceof HandlerMethod && PipelinerAdminController.class.isAssignableFrom(((HandlerMethod) handler).getBeanType())) return true;
        String path = PATHS.getPathWithinServletMapping(req);
        return path.equals("/pg") || path.startsWith("/pg/");
    }

    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws IOException {
        if (!guards(req, handler)) return true;
        String m = req.getMethod();
        boolean read = "GET".equalsIgnoreCase(m) || "HEAD".equalsIgnoreCase(m);
        boolean optionsOrTrace = "OPTIONS".equalsIgnoreCase(m);
        String presented = presentedToken(req);
        Cred c = presented == null ? null : match(presented);
        if (c != null) req.setAttribute(PRINCIPAL_ATTR, c.name);

        if (optionsOrTrace) return true;
        if (read && !readRequiresToken) return true;
        if (!read && creds.isEmpty()) {
            reject(req, res, null, HttpServletResponse.SC_FORBIDDEN, "admin API is read-only: no tokens configured", false);
            return false;
        }
        if (c == null) {
            reject(req, res, null, HttpServletResponse.SC_UNAUTHORIZED,
                    presented == null ? "authentication required" : "invalid token", true);
            return false;
        }
        if (!read && c.role != Role.ADMIN) {
            reject(req, res, c.name, HttpServletResponse.SC_FORBIDDEN, "ADMIN role required", false);
            return false;
        }
        return true;
    }

    /** Rejected mutating calls are audited too (same logger as the controller's audit lines); the token is never logged. */
    private static void reject(HttpServletRequest req, HttpServletResponse res, String principal, int status, String msg,
                               boolean challenge) throws IOException {
        String m = req.getMethod();
        if (!"GET".equalsIgnoreCase(m) && !"HEAD".equalsIgnoreCase(m)) {
            AUDIT.info("principal={} method={} path={} status={} rejected=\"{}\" remote={}",
                    principal == null ? "anonymous" : principal, m, req.getRequestURI(), status, msg, req.getRemoteAddr());
        }
        reject(res, status, msg, challenge);
    }

    private String presentedToken(HttpServletRequest req) {
        String v = req.getHeader(header);
        if (v != null) {
            v = v.trim();
            if ("Authorization".equalsIgnoreCase(header)) {
                if (v.regionMatches(true, 0, "Bearer ", 0, 7)) return v.substring(7).trim();
            } else if (!v.isEmpty()) {
                return v.regionMatches(true, 0, "Bearer ", 0, 7) ? v.substring(7).trim() : v;
            }
        }
        String alt = req.getHeader(ALT_HEADER);
        return alt == null || alt.trim().isEmpty() ? null : alt.trim();
    }

    /** Compares against every configured token (no early exit) in constant time per comparison. */
    private Cred match(String presented) {
        byte[] p = presented.getBytes(StandardCharsets.UTF_8);
        Cred found = null;
        for (Cred c : creds) {
            if (MessageDigest.isEqual(c.token, p) && found == null) found = c;
        }
        return found;
    }

    private static void reject(HttpServletResponse res, int status, String msg, boolean challenge) throws IOException {
        res.setStatus(status);
        if (challenge) res.setHeader("WWW-Authenticate", "Bearer");
        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");
        res.getWriter().write("{\"error\": \"" + msg.replace("\"", "'") + "\"}");
    }
}
