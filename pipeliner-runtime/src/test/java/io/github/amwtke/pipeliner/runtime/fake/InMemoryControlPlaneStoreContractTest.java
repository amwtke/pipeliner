package io.github.amwtke.pipeliner.runtime.fake;

import io.github.amwtke.pipeliner.core.store.AbstractControlPlaneStoreContractTest;
import org.junit.jupiter.api.BeforeEach;

/** The shared ControlPlaneStore contract against the in-memory fake every runtime test relies on (it must behave like the real stores). */
class InMemoryControlPlaneStoreContractTest extends AbstractControlPlaneStoreContractTest {
    private static final InMemoryControlPlaneStore SHARED = new InMemoryControlPlaneStore();

    @BeforeEach
    void setUp() { store = SHARED; }
}
