package com.facebook.airlift.bootstrap;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;

public class DestroyExceptionInstance
{
    @Inject
    public DestroyExceptionInstance(DependentInstance ignored)
    {
        // the constructor exists to force injection of the dependent instance
    }

    @PreDestroy
    public void preDestroyExceptionOne()
    {
        TestLifeCycleManager.note("preDestroyExceptionOne");
        throw new IllegalStateException("preDestroyExceptionOne");
    }

    @PreDestroy
    public void preDestroyExceptionTwo()
    {
        TestLifeCycleManager.note("preDestroyExceptionTwo");
        throw new IllegalStateException("preDestroyExceptionTwo");
    }
}
