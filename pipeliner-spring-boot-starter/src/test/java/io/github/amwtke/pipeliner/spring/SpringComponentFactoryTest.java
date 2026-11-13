package io.github.amwtke.pipeliner.spring;

import static org.junit.jupiter.api.Assertions.*;

import io.github.amwtke.pipeliner.api.ComponentFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class SpringComponentFactoryTest {
    static final AtomicInteger DESTROYED = new AtomicInteger();

    static class Dep { final String name = "dep"; }

    /** Constructor injection, @Value and field injection; not a registered bean. */
    static class Runner {
        final Dep ctorDep;
        final String prop;
        @Autowired Dep fieldDep;
        Runner(Dep dep, @Value("${demo.x:7}") String prop) { this.ctorDep = dep; this.prop = prop; }
        @PreDestroy void bye() { DESTROYED.incrementAndGet(); }
    }

    @Configuration
    static class Cfg {
        @Bean Dep dep() { return new Dep(); }
    }

    @Test void autowiresAndDestroysFreshInstances() {
        new ApplicationContextRunner().withUserConfiguration(Cfg.class).withPropertyValues("demo.x=42").run(ctx -> {
            ComponentFactory f = new SpringComponentFactory(ctx.getAutowireCapableBeanFactory());
            Runner a = f.create(Runner.class), b = f.create(Runner.class);
            assertNotSame(a, b);
            assertSame(ctx.getBean(Dep.class), a.ctorDep);
            assertSame(ctx.getBean(Dep.class), a.fieldDep);
            assertEquals("42", a.prop);
            assertTrue(ctx.getBeansOfType(Runner.class).isEmpty(), "created beans are not registered");
            f.destroy(a);
            assertEquals(1, DESTROYED.get(), "destroyBean runs @PreDestroy");
        });
    }
}
