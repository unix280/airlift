package com.facebook.airlift.http.client.jetty;

import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.util.ExceptionUtil;
import org.eclipse.jetty.util.thread.AutoLock;
import org.eclipse.jetty.util.thread.SerializedInvoker;

import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractContentProvider
        implements Request.Content
{
    protected final AtomicReference<Throwable> failed = new AtomicReference<>(null);
    protected final AtomicReference<Runnable> demand = new AtomicReference<>(null);
    private final SerializedInvoker invoker = new SerializedInvoker(BodyGeneratorContentProvider.class);
    private final AutoLock lock = new AutoLock();

    @Override
    public void demand(Runnable runnable)
    {
        try (AutoLock ignored = lock.lock()) {
            if (demand.get() != null) {
                throw new IllegalStateException("demand pending");
            }
            demand.set(runnable);
        }
        invoker.run(this::invokeDemandCallback);
    }

    private void invokeDemandCallback()
    {
        Runnable demandCallback;
        try (AutoLock ignored = lock.lock()) {
            demandCallback = demand.get();
            demand.set(null);
        }
        if (demandCallback != null) {
            ExceptionUtil.run(demandCallback, this::fail);
        }
    }

    @Override
    public void fail(Throwable throwable)
    {
        try (AutoLock ignored = lock.lock()) {
            if (failed.get() != null) {
                return;
            }
            failed.set(throwable);
        }
    }
}
