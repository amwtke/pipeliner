package io.github.amwtke.pipeliner.core.yaml;

/** Resolves a bean name from YAML to a component instance (source, runner, sink, condition). */
public interface ComponentResolver {
    Object resolve(String name);
}
