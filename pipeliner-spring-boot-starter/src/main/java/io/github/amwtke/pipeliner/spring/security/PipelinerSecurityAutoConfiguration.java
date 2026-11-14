package io.github.amwtke.pipeliner.spring.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Registers the token interceptor for the /pg admin API unless pipeliner.admin.security.enabled=false. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "pipeliner.admin.security", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(PipelinerSecurityProperties.class)
public class PipelinerSecurityAutoConfiguration {
    @Bean
    public AdminSecurityInterceptor pipelinerAdminSecurityInterceptor(PipelinerSecurityProperties props) {
        return new AdminSecurityInterceptor(props);
    }

    /** An MVC interceptor (not a servlet filter URL pattern), so it keeps working when spring.mvc.servlet.path or the context path change. */
    @Bean
    public WebMvcConfigurer pipelinerAdminSecurityConfigurer(AdminSecurityInterceptor interceptor) {
        return new WebMvcConfigurer() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                registry.addInterceptor(interceptor).order(Ordered.HIGHEST_PRECEDENCE + 10);
            }
        };
    }
}
