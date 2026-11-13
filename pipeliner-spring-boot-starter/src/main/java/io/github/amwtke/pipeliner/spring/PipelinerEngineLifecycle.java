package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

/** Starts the engine once the application is ready and stops it when the context closes. */
public class PipelinerEngineLifecycle implements ApplicationListener<ApplicationReadyEvent>, DisposableBean {
    private final PipelinerEngine engine;
    private volatile boolean started;

    public PipelinerEngineLifecycle(PipelinerEngine engine) {
        this.engine = engine;
    }

    @Override public void onApplicationEvent(ApplicationReadyEvent event) {
        engine.start();
        started = true;
    }

    @Override public void destroy() {
        if (started) engine.stop();
    }
}
