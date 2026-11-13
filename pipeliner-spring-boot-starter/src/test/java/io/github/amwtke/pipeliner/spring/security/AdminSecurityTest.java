package io.github.amwtke.pipeliner.spring.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import io.github.amwtke.pipeliner.core.engine.RuntimeConfig;
import io.github.amwtke.pipeliner.spring.PipelinerAdminController;
import java.util.Collections;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class AdminSecurityTest {
    static final String ADMIN = "admin-token-0123456789";
    static final String VIEW = "viewer-token-0123456789";

    PipelinerEngine engine;
    ListAppender<ILoggingEvent> audit;

    @BeforeEach
    void setUp() {
        engine = mock(PipelinerEngine.class);
        audit = new ListAppender<>();
        audit.start();
        ((Logger) LoggerFactory.getLogger("pipeliner.audit")).setLevel(ch.qos.logback.classic.Level.INFO);
        ((Logger) LoggerFactory.getLogger("pipeliner.audit")).addAppender(audit);
    }

    @AfterEach
    void tearDown() {
        ((Logger) LoggerFactory.getLogger("pipeliner.audit")).detachAppender(audit);
    }

    PipelinerSecurityProperties props(boolean withTokens) {
        PipelinerSecurityProperties p = new PipelinerSecurityProperties();
        if (withTokens) {
            p.getTokens().add(entry("ops", ADMIN, PipelinerSecurityProperties.Role.ADMIN));
            p.getTokens().add(entry("viewer", VIEW, PipelinerSecurityProperties.Role.VIEWER));
        }
        return p;
    }

    static PipelinerSecurityProperties.TokenEntry entry(String n, String t, PipelinerSecurityProperties.Role r) {
        PipelinerSecurityProperties.TokenEntry e = new PipelinerSecurityProperties.TokenEntry();
        e.setName(n); e.setToken(t); e.setRole(r);
        return e;
    }

    MockMvc mvc(PipelinerSecurityProperties p) {
        PipelinerAdminController c = new PipelinerAdminController(engine, new RuntimeConfig());
        return MockMvcBuilders.standaloneSetup(c).addFilter(new AdminSecurityFilter(p), "/pg/*").build();
    }

    @Test
    void getWithoutTokenOk() throws Exception {
        mvc(props(true)).perform(get("/pg/graphs")).andExpect(status().isOk());
    }

    @Test
    void postWithoutTokenIs401() throws Exception {
        mvc(props(true)).perform(post("/pg/rounds/r1/pause"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", "Bearer"))
                .andExpect(jsonPath("$.error").exists());
        verify(engine, never()).pause(anyString());
    }

    @Test
    void viewerPostIs403() throws Exception {
        mvc(props(true)).perform(post("/pg/rounds/r1/pause").header("Authorization", "Bearer " + VIEW))
                .andExpect(status().isForbidden());
        verify(engine, never()).pause(anyString());
    }

    @Test
    void adminPostPasses() throws Exception {
        mvc(props(true)).perform(post("/pg/rounds/r1/pause").header("Authorization", "Bearer " + ADMIN))
                .andExpect(status().isOk());
        verify(engine).pause("r1");
    }

    @Test
    void unknownTokenIs401() throws Exception {
        mvc(props(true)).perform(post("/pg/rounds/r1/pause").header("Authorization", "Bearer nope-nope-nope-nope"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void altHeaderWorks() throws Exception {
        mvc(props(true)).perform(post("/pg/rounds/r1/resume").header("X-PG-Token", ADMIN)).andExpect(status().isOk());
        verify(engine).resume("r1");
    }

    @Test
    void readRequiresToken() throws Exception {
        PipelinerSecurityProperties p = props(true);
        p.setReadRequiresToken(true);
        MockMvc m = mvc(p);
        m.perform(get("/pg/graphs")).andExpect(status().isUnauthorized());
        m.perform(get("/pg/graphs").header("Authorization", "Bearer " + VIEW)).andExpect(status().isOk());
        m.perform(get("/pg/graphs").header("Authorization", "Bearer " + ADMIN)).andExpect(status().isOk());
    }

    @Test
    void noTokensConfiguredMutationsForbidden() throws Exception {
        MockMvc m = mvc(props(false));
        m.perform(post("/pg/rounds/r1/pause")).andExpect(status().isForbidden());
        m.perform(post("/pg/rounds/r1/pause").header("Authorization", "Bearer whatever")).andExpect(status().isForbidden());
        m.perform(get("/pg/graphs")).andExpect(status().isOk());
        verify(engine, never()).pause(anyString());
    }

    @Test
    void disabledPassesEverything() {
        new org.springframework.boot.test.context.runner.WebApplicationContextRunner()
                .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(PipelinerSecurityAutoConfiguration.class))
                .withPropertyValues("pipeliner.admin.security.enabled=false")
                .run(ctx -> assertFalse(ctx.containsBean("pipelinerAdminSecurityFilter")));
        new org.springframework.boot.test.context.runner.WebApplicationContextRunner()
                .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(PipelinerSecurityAutoConfiguration.class))
                .run(ctx -> assertTrue(ctx.containsBean("pipelinerAdminSecurityFilter")));
    }

    @Test
    void disabledMeansOperatorParamUsedAndNoFilter() throws Exception {
        MockMvc m = MockMvcBuilders.standaloneSetup(new PipelinerAdminController(engine, new RuntimeConfig())).build();
        m.perform(post("/pg/gates/r1/g1/open?operator=bob")).andExpect(status().isOk());
        verify(engine).openGate("r1", "g1", "bob");
    }

    @Test
    void openGateUsesPrincipalAndAuditHasNoToken() throws Exception {
        mvc(props(true)).perform(post("/pg/gates/r1/g1/open?operator=mallory").header("Authorization", "Bearer " + ADMIN))
                .andExpect(status().isOk());
        verify(engine).openGate("r1", "g1", "ops");
        verify(engine, never()).openGate("r1", "g1", "mallory");
        String lines = audit.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.joining("\n"));
        assertTrue(lines.contains("principal=ops method=POST path=/pg/gates/r1/g1/open round=r1 gate=g1 status=200"), lines);
        assertFalse(lines.contains(ADMIN), lines);
        assertTrue(audit.list.size() == 1);
    }
}
