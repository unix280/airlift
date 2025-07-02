package com.facebook.airlift.bootstrap;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import javax.inject.Provider;

public class BarProvider
        implements Provider<BarInstance>
{
    @PostConstruct
    public void postDependentInstance()
    {
        TestLifeCycleManager.note("postBarProvider");
    }

    @PreDestroy
    public void preDependentInstance()
    {
        TestLifeCycleManager.note("preBarProvider");
    }

    @Override
    public BarInstance get()
    {
        return new BarInstance();
    }
}
