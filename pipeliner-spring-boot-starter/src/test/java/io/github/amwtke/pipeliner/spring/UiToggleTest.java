package io.github.amwtke.pipeliner.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.boot.web.servlet.FilterRegistrationBean;

class UiToggleTest {
    final WebApplicationContextRunner runner = new WebApplicationContextRunner().withConfiguration(AutoConfigurations.of(PipelinerUiAutoConfiguration.class));

    @Test
    void enabledByDefaultNoBlockingFilter() {
        runner.run(ctx -> assertTrue(ctx.getBeansOfType(FilterRegistrationBean.class).isEmpty()));
    }

    @Test
    void disabledRegistersFilterAnswering404() {
        runner.withPropertyValues("pipeliner.admin.ui.enabled=false").run(ctx -> {
            assertEquals(1, ctx.getBeansOfType(FilterRegistrationBean.class).size());
            FilterRegistrationBean<?> r = ctx.getBean(FilterRegistrationBean.class);
            assertTrue(r.getUrlPatterns().contains("/pg-ui/*"));
            org.springframework.mock.web.MockHttpServletResponse res = new org.springframework.mock.web.MockHttpServletResponse();
            r.getFilter().doFilter(new org.springframework.mock.web.MockHttpServletRequest("GET", "/pg-ui/app.js"), res,
                    new org.springframework.mock.web.MockFilterChain());
            assertEquals(404, res.getStatus());
        });
    }

    @Test
    void staticFilesArePackaged() throws Exception {
        for (String f : new String[] {"index.html", "app.js", "style.css"}) {
            try (InputStream in = getClass().getResourceAsStream("/META-INF/resources/pg-ui/" + f)) {
                assertNotNull(in, f);
            }
        }
    }
}
