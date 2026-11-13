package io.github.amwtke.pipeliner.spring.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/** Registers the token filter in front of /pg/* unless pipeliner.admin.security.enabled=false. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "pipeliner.admin.security", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(PipelinerSecurityProperties.class)
public class PipelinerSecurityAutoConfiguration {
    @Bean
    public FilterRegistrationBean<AdminSecurityFilter> pipelinerAdminSecurityFilter(PipelinerSecurityProperties props) {
        FilterRegistrationBean<AdminSecurityFilter> r = new FilterRegistrationBean<>(new AdminSecurityFilter(props));
        r.addUrlPatterns("/pg/*");
        r.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
        return r;
    }
}
