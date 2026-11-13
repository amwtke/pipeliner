package io.github.amwtke.pipeliner.spring;

import io.github.amwtke.pipeliner.api.ComponentFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

/** Creates type-based runners as autowired (non-registered) Spring beans, so they can use constructor/field injection and @Value. */
public class SpringComponentFactory implements ComponentFactory {
    private final AutowireCapableBeanFactory beans;

    public SpringComponentFactory(AutowireCapableBeanFactory beans) { this.beans = beans; }

    @Override public <T> T create(Class<T> type) { return beans.createBean(type); }

    @Override public void destroy(Object component) { beans.destroyBean(component); }
}
