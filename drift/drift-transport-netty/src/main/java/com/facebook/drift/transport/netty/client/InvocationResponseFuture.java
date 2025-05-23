/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.drift.transport.netty.client;

import com.facebook.airlift.log.Logger;
import com.facebook.drift.TException;
import com.facebook.drift.protocol.TTransportException;
import com.facebook.drift.transport.client.ConnectionFailedException;
import com.facebook.drift.transport.client.InvokeRequest;
import com.facebook.drift.transport.netty.client.ConnectionManager.ConnectionParameters;
import com.facebook.drift.transport.netty.client.ThriftClientHandler.ThriftRequest;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class InvocationResponseFuture
        extends AbstractFuture<Object>
{
    private static final Logger log = Logger.get(InvocationResponseFuture.class);

    private final InvokeRequest request;
    private final ConnectionParameters connectionParameters;
    private final ConnectionManager connectionManager;

    @GuardedBy("this")
    private Future<Channel> connectionFuture;

    @GuardedBy("this")
    private ThriftRequest thriftRequest;

    static InvocationResponseFuture createInvocationResponseFuture(InvokeRequest request, ConnectionParameters connectionParameters, ConnectionManager connectionManager)
            throws TException
    {
        Optional<Boolean> encryptionRequired = request.getAddress().isEncryptionRequired();
        if (encryptionRequired.isPresent() && encryptionRequired.get() && !connectionParameters.getSslContextParameters().isPresent()) {
            throw new TException(format(
                    "Encrypted connection is requested to connect to %s but encryption is not configured for this Drift client instance",
                    request.getAddress().getHostAndPort()));
        }

        if (encryptionRequired.isPresent() && !encryptionRequired.get()) {
            log.debug("Disabling SSL method %s address %s request %s", request.getMethod(), request.getAddress(), request.toString());
            connectionParameters = new ConnectionParameters(
                    connectionParameters.getTransport(),
                    connectionParameters.getProtocol(),
                    connectionParameters.getMaxFrameSize(),
                    connectionParameters.getConnectTimeout(),
                    connectionParameters.getRequestTimeout(),
                    connectionParameters.getSocksProxy(),
                    Optional.empty(),
                    connectionParameters.isTcpNoDelayEnabled(),
                    connectionParameters.isReuseAddressEnabled());
        }

        InvocationResponseFuture future = new InvocationResponseFuture(request, connectionParameters, connectionManager);
        // invocation can not be started from constructor, because it may start threads that can call back into the unpublished object
        future.tryConnect();
        return future;
    }

    private InvocationResponseFuture(InvokeRequest request, ConnectionParameters connectionParameters, ConnectionManager connectionManager)
    {
        this.request = requireNonNull(request, "request is null");
        this.connectionParameters = requireNonNull(connectionParameters, "connectionConfig is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");

        // if this invocation is canceled, cancel the tasks
        super.addListener(() -> {
            if (super.isCancelled()) {
                onCancel(wasInterrupted());
            }
        }, directExecutor());
    }

    private synchronized void tryConnect()
    {
        try {
            connectionFuture = connectionManager.getConnection(connectionParameters, request.getAddress().getHostAndPort());
            connectionFuture.addListener(channelFuture -> {
                try {
                    if (channelFuture.isSuccess()) {
                        // Netty future listener generic type declaration requires a cast when used with a lambda
                        tryInvocation((Channel) channelFuture.getNow());
                    }
                    else {
                        fatalError(new ConnectionFailedException(request.getAddress(), channelFuture.cause()));
                    }
                }
                catch (Throwable t) {
                    fatalError(t);
                }
            });
        }
        catch (Throwable t) {
            fatalError(t);
        }
    }

    private synchronized void tryInvocation(Channel channel)
    {
        // is request already canceled
        if (isCancelled()) {
            connectionManager.returnConnection(channel);
            return;
        }

        try {
            thriftRequest = new ThriftRequest(request.getMethod(), request.getParameters(), request.getHeaders());
            Futures.addCallback(thriftRequest, new FutureCallback<Object>()
                    {
                        @Override
                        public void onSuccess(Object result)
                        {
                            try {
                                connectionManager.returnConnection(channel);
                                set(result);
                            }
                            catch (Throwable t) {
                                fatalError(t);
                            }
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                            try {
                                connectionManager.returnConnection(channel);
                            }
                            finally {
                                fatalError(t);
                            }
                        }
                    },
                    directExecutor());

            ChannelFuture sendFuture = channel.writeAndFlush(thriftRequest);
            sendFuture.addListener(channelFuture -> {
                try {
                    if (!channelFuture.isSuccess()) {
                        fatalError(channelFuture.cause());
                    }
                }
                catch (Throwable t) {
                    fatalError(t);
                }
            });
        }
        catch (Throwable t) {
            try {
                connectionManager.returnConnection(channel);
            }
            finally {
                fatalError(t);
            }
        }
    }

    private synchronized void onCancel(boolean wasInterrupted)
    {
        if (connectionFuture != null) {
            connectionFuture.cancel(wasInterrupted);
        }
        if (thriftRequest != null) {
            thriftRequest.cancel(wasInterrupted);
        }
    }

    private void fatalError(Throwable throwable)
    {
        if (throwable instanceof IOException) {
            throwable = new TTransportException(throwable);
        }
        // exception in the future is expected to be a TException
        if (!(throwable instanceof Error) && !(throwable instanceof TException)) {
            throwable = new TException(throwable);
        }
        setException(throwable);
    }
}
