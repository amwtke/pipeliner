package io.github.amwtke.pipeliner.spring.security;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Auth settings for the /pg admin API (prefix pipeliner.admin.security). */
@ConfigurationProperties(prefix = "pipeliner.admin.security")
public class PipelinerSecurityProperties {
    public enum Role { ADMIN, VIEWER }

    public static class TokenEntry {
        private String name;
        private String token;
        private Role role = Role.VIEWER;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getToken() { return token; }
        public void setToken(String token) { this.token = token; }
        public Role getRole() { return role; }
        public void setRole(Role role) { this.role = role; }
    }

    private boolean enabled = true;
    private List<TokenEntry> tokens = new ArrayList<>();
    private boolean readRequiresToken = false;
    private String header = "Authorization";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public List<TokenEntry> getTokens() { return tokens; }
    public void setTokens(List<TokenEntry> tokens) { this.tokens = tokens; }
    public boolean isReadRequiresToken() { return readRequiresToken; }
    public void setReadRequiresToken(boolean readRequiresToken) { this.readRequiresToken = readRequiresToken; }
    public String getHeader() { return header; }
    public void setHeader(String header) { this.header = header; }
}
