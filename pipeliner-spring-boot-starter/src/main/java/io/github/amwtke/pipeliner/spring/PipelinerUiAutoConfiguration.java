package io.github.amwtke.pipeliner.spring;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * The static UI under META-INF/resources/pg-ui is served at /pg-ui/. With pipeliner.admin.ui.enabled=false a filter answers 404 for
 * /pg-ui/** so the files are not reachable. It lives outside /pg/* and therefore outside the token filter.
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
        @Bean
        public FilterRegistrationBean<javax.servlet.Filter> pipelinerUiDisabledFilter() {
            javax.servlet.Filter f = (req, res, chain) -> ((javax.servlet.http.HttpServletResponse) res).sendError(404);
            FilterRegistrationBean<javax.servlet.Filter> r = new FilterRegistrationBean<>(f);
            r.addUrlPatterns("/pg-ui", "/pg-ui/*");
            r.setOrder(Ordered.HIGHEST_PRECEDENCE + 5);
            return r;
        }
    }
}
