package io.github.amwtke.pipeliner.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;

class UiToggleTest {
    final WebApplicationContextRunner runner = new WebApplicationContextRunner().withConfiguration(AutoConfigurations.of(PipelinerUiAutoConfiguration.class));

    @Test
    void enabledByDefaultNoBlockingInterceptor() {
        runner.run(ctx -> assertFalse(ctx.containsBean("pipelinerUiDisabledConfigurer")));
    }

    @Test
    void disabledAnswers404UnderAnyServletPath() {
        runner.withPropertyValues("pipeliner.admin.ui.enabled=false").run(ctx -> {
            assertTrue(ctx.containsBean("pipelinerUiDisabledConfigurer"));
            PipelinerUiAutoConfiguration.UiDisabledInterceptor i = new PipelinerUiAutoConfiguration.UiDisabledInterceptor();
            for (String[] c : new String[][] {{"/pg-ui/app.js", ""}, {"/api/pg-ui/app.js", "/api"}, {"/api/pg-ui", "/api"}}) {
                org.springframework.mock.web.MockHttpServletRequest req = new org.springframework.mock.web.MockHttpServletRequest("GET", c[0]);
                req.setServletPath(c[1]);
                org.springframework.mock.web.MockHttpServletResponse res = new org.springframework.mock.web.MockHttpServletResponse();
                assertFalse(i.preHandle(req, res, new Object()), c[0]);
                assertEquals(404, res.getStatus(), c[0]);
            }
            org.springframework.mock.web.MockHttpServletRequest other = new org.springframework.mock.web.MockHttpServletRequest("GET", "/api/pg/graphs");
            other.setServletPath("/api");
            assertTrue(i.preHandle(other, new org.springframework.mock.web.MockHttpServletResponse(), new Object()));
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
