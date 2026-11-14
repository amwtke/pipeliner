package io.github.amwtke.pipeliner.spring;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.util.UrlPathHelper;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * The static UI under META-INF/resources/pg-ui is served at /pg-ui/. With pipeliner.admin.ui.enabled=false an interceptor answers 404 for
 * /pg-ui/** so the files are not reachable. It lives outside /pg and therefore outside the token check.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class PipelinerUiAutoConfiguration {

    /** Makes /pg-ui and /pg-ui/ serve index.html. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "pipeliner.admin.ui", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class Welcome implements WebMvcConfigurer {
        @Override
        public void addViewControllers(ViewControllerRegistry registry) {
            registry.addRedirectViewController("/pg-ui", "/pg-ui/");
            registry.addViewController("/pg-ui/").setViewName("forward:/pg-ui/index.html");
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "pipeliner.admin.ui", name = "enabled", havingValue = "false")
    static class Disabled {
        /** Answers 404 for /pg-ui[/**] relative to the servlet mapping, whatever spring.mvc.servlet.path is. */
        @Bean
        public WebMvcConfigurer pipelinerUiDisabledConfigurer() {
            return new WebMvcConfigurer() {
                @Override
                public void addInterceptors(InterceptorRegistry registry) {
                    registry.addInterceptor(new UiDisabledInterceptor()).order(Ordered.HIGHEST_PRECEDENCE + 5);
                }
            };
        }
    }

    static final class UiDisabledInterceptor implements HandlerInterceptor {
        private static final UrlPathHelper PATHS = new UrlPathHelper();

        @Override
        public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws IOException {
            String path = PATHS.getPathWithinServletMapping(req);
            if (path.equals("/pg-ui") || path.startsWith("/pg-ui/")) {
                res.sendError(HttpServletResponse.SC_NOT_FOUND);
                return false;
            }
            return true;
        }
    }
}
