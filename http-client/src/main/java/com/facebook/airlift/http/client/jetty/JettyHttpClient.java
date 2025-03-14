package com.facebook.airlift.http.client.jetty;

import com.facebook.airlift.http.client.BodyGenerator;
import com.facebook.airlift.http.client.FileBodyGenerator;
import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.airlift.http.client.HttpRequestFilter;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.RequestStats;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.StaticBodyGenerator;
import com.facebook.airlift.http.client.jetty.HttpClientLogger.RequestInfo;
import com.facebook.airlift.http.client.jetty.HttpClientLogger.ResponseInfo;
import com.facebook.airlift.http.client.spnego.KerberosConfig;
import com.facebook.airlift.http.client.spnego.SpnegoAuthentication;
import com.facebook.airlift.http.client.spnego.SpnegoAuthenticationProtocolHandler;
import com.facebook.airlift.http.client.spnego.SpnegoAuthenticationStore;
import com.facebook.airlift.security.pem.PemReader;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import jakarta.annotation.PreDestroy;
import org.eclipse.jetty.client.AbstractConnectionPool;
import org.eclipse.jetty.client.AuthenticationStore;
import org.eclipse.jetty.client.ConnectionPoolAccessor;
import org.eclipse.jetty.client.Destination;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.InputStreamResponseListener;
import org.eclipse.jetty.client.PathRequestContent;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.client.Socks4Proxy;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;
import org.eclipse.jetty.client.transport.HttpChannel;
import org.eclipse.jetty.client.transport.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.transport.HttpConnection;
import org.eclipse.jetty.client.transport.HttpDestination;
import org.eclipse.jetty.client.transport.HttpExchange;
import org.eclipse.jetty.client.transport.HttpRequest;
import org.eclipse.jetty.http.HttpCookieStore;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.transport.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.eclipse.jetty.io.ConnectionStatistics;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.util.thread.Scheduler;
import org.eclipse.jetty.util.thread.Sweeper;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.airlift.http.client.jetty.AuthorizationPreservingHttpClient.setPreserveAuthorization;
import static com.facebook.airlift.http.utils.jetty.ConcurrentScheduler.createConcurrentScheduler;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.eclipse.jetty.client.transport.HttpChannelAccessor.getHttpConnectionChannels;

public class JettyHttpClient
        implements com.facebook.airlift.http.client.HttpClient
{
    private static final String STATS_KEY = "airlift_stats";
    private static final long SWEEP_PERIOD_MILLIS = 5000;

    private static final AtomicLong NAME_COUNTER = new AtomicLong();

    private final HttpClient httpClient;
    private final long maxContentLength;
    private final long requestTimeoutMillis;
    private final long idleTimeoutMillis;
    private final int requestBufferSizeInBytes;
    private final boolean recordRequestComplete;
    private final boolean logEnabled;
    private final QueuedThreadPoolMBean queuedThreadPoolMBean;
    private final ConnectionStats connectionStats;
    private final RequestStats stats = new RequestStats();
    private final CachedDistribution queuedRequestsPerDestination;
    private final CachedDistribution activeConnectionsPerDestination;
    private final CachedDistribution idleConnectionsPerDestination;

    private final CachedDistribution currentQueuedTime;
    private final CachedDistribution currentRequestTime;
    private final CachedDistribution currentRequestSendTime;
    private final CachedDistribution currentResponseWaitTime;
    private final CachedDistribution currentResponseProcessTime;

    private final List<HttpRequestFilter> requestFilters;
    private final Exception creationLocation = new Exception();
    private final String name;

    private final HttpClientLogger requestLogger;
    private final JettyClientDiagnostics clientDiagnostics;

    public JettyHttpClient()
    {
        this(new HttpClientConfig());
    }

    public JettyHttpClient(HttpClientConfig config)
    {
        this(uniqueName(), config);
    }

    public JettyHttpClient(String name, HttpClientConfig config)
    {
        this(name, config, new KerberosConfig(), ImmutableList.of());
    }

    public JettyHttpClient(
            String name,
            HttpClientConfig config,
            KerberosConfig kerberosConfig,
            Iterable<? extends HttpRequestFilter> requestFilters)
    {
        this.name = requireNonNull(name, "name is null");

        requireNonNull(config, "config is null");
        requireNonNull(requestFilters, "requestFilters is null");

        maxContentLength = config.getMaxContentLength().toBytes();
        requestTimeoutMillis = config.getRequestTimeout().toMillis();
        idleTimeoutMillis = config.getIdleTimeout().toMillis();
        requestBufferSizeInBytes = toIntExact(config.getRequestBufferSize().toBytes());
        recordRequestComplete = config.getRecordRequestComplete();

        creationLocation.fillInStackTrace();

        SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
        sslContextFactory.setEndpointIdentificationAlgorithm("HTTPS");
        if (config.getKeyStorePath() != null) {
            Optional<KeyStore> pemKeyStore = tryLoadPemKeyStore(config);
            if (pemKeyStore.isPresent()) {
                sslContextFactory.setKeyStore(pemKeyStore.orElseThrow());
                sslContextFactory.setKeyStorePassword("");
            }
            else {
                sslContextFactory.setKeyStorePath(config.getKeyStorePath());
                sslContextFactory.setKeyStorePassword(config.getKeyStorePassword());
            }
        }
        if (config.getTrustStorePath() != null) {
            Optional<KeyStore> pemTrustStore = tryLoadPemTrustStore(config);
            if (pemTrustStore.isPresent()) {
                sslContextFactory.setTrustStore(pemTrustStore.orElseThrow());
                sslContextFactory.setTrustStorePassword("");
            }
            else {
                sslContextFactory.setTrustStorePath(config.getTrustStorePath());
                sslContextFactory.setTrustStorePassword(config.getTrustStorePassword());
            }
        }
        sslContextFactory.setSecureRandomAlgorithm(config.getSecureRandomAlgorithm());
        List<String> includedCipherSuites = config.getHttpsIncludedCipherSuites();
        List<String> excludedCipherSuites = config.getHttpsExcludedCipherSuites();
        sslContextFactory.setIncludeCipherSuites(includedCipherSuites.toArray(new String[0]));
        sslContextFactory.setExcludeCipherSuites(excludedCipherSuites.toArray(new String[0]));

        HttpClientTransport transport;
        if (config.isHttp2Enabled()) {
            HTTP2Client client = new HTTP2Client();
            client.setInitialSessionRecvWindow(toIntExact(config.getHttp2InitialSessionReceiveWindowSize().toBytes()));
            client.setInitialStreamRecvWindow(toIntExact(config.getHttp2InitialStreamReceiveWindowSize().toBytes()));
            client.setInputBufferSize(toIntExact(config.getHttp2InputBufferSize().toBytes()));
            client.setSelectors(config.getSelectorCount());
            transport = new HttpClientTransportOverHTTP2(client);
        }
        else {
            transport = new HttpClientTransportOverHTTP(config.getSelectorCount());
        }

        httpClient = new AuthorizationPreservingHttpClient(transport, sslContextFactory);

        // request and response buffer size
        httpClient.setRequestBufferSize(toIntExact(config.getRequestBufferSize().toBytes()));
        httpClient.setResponseBufferSize(toIntExact(config.getResponseBufferSize().toBytes()));

        // Kerberos authentication
        if (config.getAuthenticationEnabled()) {
            AuthenticationStore store = new SpnegoAuthenticationStore(new SpnegoAuthentication(
                    kerberosConfig.getKeytab(),
                    kerberosConfig.getConfig(),
                    kerberosConfig.getCredentialCache(),
                    config.getKerberosServicePrincipalPattern(),
                    config.getKerberosPrincipal(),
                    config.getKerberosRemoteServiceName(),
                    config.getKerberosNameType(),
                    kerberosConfig.isUseCanonicalHostname()));
            httpClient.setAuthenticationStore(store);
            httpClient.getProtocolHandlers().remove(WWWAuthenticationProtocolHandler.NAME);
            httpClient.getProtocolHandlers().put(new SpnegoAuthenticationProtocolHandler(httpClient));
        }

        httpClient.setMaxConnectionsPerDestination(config.getMaxConnectionsPerServer());
        httpClient.setMaxRequestsQueuedPerDestination(config.getMaxRequestsQueuedPerDestination());

        // disable cookies
        httpClient.setHttpCookieStore(new HttpCookieStore.Empty());

        // remove default user agent
        httpClient.setUserAgentField(null);

        // timeouts
        httpClient.setIdleTimeout(idleTimeoutMillis);
        httpClient.setConnectBlocking(true);
        httpClient.setConnectTimeout(config.getConnectTimeout().toMillis());
        httpClient.setAddressResolutionTimeout(config.getConnectTimeout().toMillis());
        httpClient.setDestinationIdleTimeout(idleTimeoutMillis);

        httpClient.setConnectBlocking(config.isConnectBlocking());

        HostAndPort socksProxy = config.getSocksProxy();
        if (socksProxy != null) {
            httpClient.getProxyConfiguration().addProxy(new Socks4Proxy(socksProxy.getHost(), socksProxy.getPortOrDefault(1080)));
        }

        httpClient.setByteBufferPool(new ArrayByteBufferPool.Tracking());
        QueuedThreadPool queuedThreadPool = createExecutor(name, config.getMinThreads(), config.getMaxThreads());
        httpClient.setExecutor(queuedThreadPool);
        // add queuedThreadPool as a managed bean to get its state in the client dumps
        httpClient.addBean(queuedThreadPool, true);
        httpClient.setScheduler(createScheduler(name, config.getTimeoutConcurrency(), config.getTimeoutThreads()));

        httpClient.setSocketAddressResolver(new JettyAsyncSocketAddressResolver(
                httpClient.getExecutor(),
                httpClient.getScheduler(),
                config.getConnectTimeout().toMillis()));

        // Jetty client connections can sometimes get stuck while closing which reduces
        // the available connections.  The Jetty Sweeper periodically scans the active
        // connection pool looking for connections in the closed state, and if a connection
        // is observed in the closed state multiple times, it logs, and destroys the connection.
        httpClient.addBean(new Sweeper(httpClient.getScheduler(), SWEEP_PERIOD_MILLIS), true);

        // track connection statistics
        ConnectionStatistics connectionStats = new ConnectionStatistics();
        httpClient.addBean(connectionStats);
        this.connectionStats = new ConnectionStats(connectionStats);

        // configure logging
        this.logEnabled = config.isLogEnabled();
        if (logEnabled) {
            String logFilePath = Path.of(config.getLogPath(), format("%s-http-client.log", name)).toAbsolutePath().toString();
            requestLogger = new DefaultHttpClientLogger(
                    logFilePath,
                    config.getLogHistory(),
                    config.getLogQueueSize(),
                    config.getLogBufferSize(),
                    config.getLogFlushInterval(),
                    config.getLogMaxFileSize().toBytes(),
                    config.isLogCompressionEnabled());
        }
        else {
            requestLogger = new NoopLogger();
        }

        try {
            httpClient.start();

            // remove the GZIP encoding from the client
            // TODO: there should be a better way to to do this
            httpClient.getContentDecoderFactories().clear();
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        this.clientDiagnostics = new JettyClientDiagnostics();

        this.requestFilters = ImmutableList.copyOf(requestFilters);

        this.queuedThreadPoolMBean = new QueuedThreadPoolMBean((QueuedThreadPool) httpClient.getExecutor());

        this.activeConnectionsPerDestination = new ConnectionPoolDistribution(httpClient,
                (distribution, connectionPool) -> distribution.add(connectionPool.getActiveConnectionCount()));

        this.idleConnectionsPerDestination = new ConnectionPoolDistribution(httpClient,
                (distribution, connectionPool) -> distribution.add(connectionPool.getIdleConnectionCount()));

        this.queuedRequestsPerDestination = new DestinationDistribution(httpClient,
                (distribution, destination) -> distribution.add(destination.getHttpExchanges().size()));

        this.currentQueuedTime = new RequestDistribution(httpClient, (distribution, listener, now) -> {
            long started = listener.getRequestStarted();
            if (started == 0) {
                started = now;
            }
            distribution.add(NANOSECONDS.toMillis(started - listener.getCreated()));
        });

        this.currentRequestTime = new RequestDistribution(httpClient, (distribution, listener, now) -> {
            long started = listener.getRequestStarted();
            if (started == 0) {
                return;
            }
            long finished = listener.getResponseFinished();
            if (finished == 0) {
                finished = now;
            }
            distribution.add(NANOSECONDS.toMillis(finished - started));
        });

        this.currentRequestSendTime = new RequestDistribution(httpClient, (distribution, listener, now) -> {
            long started = listener.getRequestStarted();
            if (started == 0) {
                return;
            }
            long requestSent = listener.getRequestFinished();
            if (requestSent == 0) {
                requestSent = now;
            }
            distribution.add(NANOSECONDS.toMillis(requestSent - started));
        });

        this.currentResponseWaitTime = new RequestDistribution(httpClient, (distribution, listener, now) -> {
            long requestSent = listener.getRequestFinished();
            if (requestSent == 0) {
                return;
            }
            long responseStarted = listener.getResponseStarted();
            if (responseStarted == 0) {
                responseStarted = now;
            }
            distribution.add(NANOSECONDS.toMillis(responseStarted - requestSent));
        });

        this.currentResponseProcessTime = new RequestDistribution(httpClient, (distribution, listener, now) -> {
            long responseStarted = listener.getResponseStarted();
            if (responseStarted == 0) {
                return;
            }
            long finished = listener.getResponseFinished();
            if (finished == 0) {
                finished = now;
            }
            distribution.add(NANOSECONDS.toMillis(finished - responseStarted));
        });
    }

    private static Optional<KeyStore> tryLoadPemKeyStore(HttpClientConfig config)
    {
        File keyStoreFile = Path.of(config.getKeyStorePath()).toFile();
        try {
            if (!PemReader.isPem(keyStoreFile)) {
                return Optional.empty();
            }
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Error reading key store file: " + keyStoreFile, e);
        }

        try {
            return Optional.of(PemReader.loadKeyStore(keyStoreFile, keyStoreFile, Optional.ofNullable(config.getKeyStorePassword())));
        }
        catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Error loading PEM key store: " + keyStoreFile, e);
        }
    }

    private static Optional<KeyStore> tryLoadPemTrustStore(HttpClientConfig config)
    {
        File trustStoreFile = Path.of(config.getTrustStorePath()).toFile();
        try {
            if (!PemReader.isPem(trustStoreFile)) {
                return Optional.empty();
            }
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Error reading trust store file: " + trustStoreFile, e);
        }

        try {
            if (PemReader.readCertificateChain(trustStoreFile).isEmpty()) {
                throw new IllegalArgumentException("PEM trust store file does not contain any certificates: " + trustStoreFile);
            }
            return Optional.of(PemReader.loadTrustStore(trustStoreFile));
        }
        catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException("Error loading PEM trust store: " + trustStoreFile, e);
        }
    }

    private static QueuedThreadPool createExecutor(String name, int minThreads, int maxThreads)
    {
        try {
            QueuedThreadPool pool = new QueuedThreadPool(maxThreads, minThreads, 60000, null);
            pool.setName("http-client-" + name);
            pool.setDaemon(true);
            pool.start();
            pool.setStopTimeout(100);
            pool.setDetailedDump(true);
            return pool;
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private static Scheduler createScheduler(String name, int timeoutConcurrency, int timeoutThreads)
    {
        Scheduler scheduler;
        String threadName = "http-client-" + name + "-timeout";
        if ((timeoutConcurrency == 1) && (timeoutThreads == 1)) {
            scheduler = new ScheduledExecutorScheduler(threadName, true);
        }
        else {
            scheduler = createConcurrentScheduler(threadName, timeoutConcurrency, timeoutThreads);
        }

        try {
            scheduler.start();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        return scheduler;
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        long requestStart = System.nanoTime();

        // apply filters
        request = applyRequestFilters(request);

        // create jetty request and response listener
        JettyRequestListener requestListener = new JettyRequestListener(request.getUri());
        HttpRequest jettyRequest = buildJettyRequest(request, requestListener);
        InputStreamResponseListener listener = new InputStreamResponseListener()
        {
            @Override
            public void onContent(Response response, ByteBuffer content)
            {
                // ignore empty blocks
                if (content.remaining() == 0) {
                    return;
                }
                super.onContent(response, content);
            }
        };

        long requestTimestamp = System.currentTimeMillis();
        RequestInfo requestInfo = RequestInfo.from(jettyRequest, requestTimestamp);
        if (logEnabled) {
            addLoggingListener(jettyRequest, requestTimestamp);
        }

        // fire the request
        jettyRequest.send(listener);

        // wait for response to begin
        Response response;
        try {
            response = listener.get(httpClient.getIdleTimeout(), MILLISECONDS);
        }
        catch (InterruptedException e) {
            stats.recordRequestFailed();
            requestLogger.log(requestInfo, ResponseInfo.failed(Optional.empty(), Optional.of(e)));
            jettyRequest.abort(e);
            Thread.currentThread().interrupt();
            return responseHandler.handleException(request, e);
        }
        catch (TimeoutException e) {
            stats.recordRequestFailed();
            requestLogger.log(requestInfo, ResponseInfo.failed(Optional.empty(), Optional.of(e)));
            jettyRequest.abort(e);
            return responseHandler.handleException(request, e);
        }
        catch (ExecutionException e) {
            stats.recordRequestFailed();
            requestLogger.log(requestInfo, ResponseInfo.failed(Optional.empty(), Optional.of(e)));
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                return responseHandler.handleException(request, (Exception) cause);
            }
            else if ((cause instanceof NoClassDefFoundError) && cause.getMessage().endsWith("ALPNClientConnection")) {
                return responseHandler.handleException(request, new RuntimeException("HTTPS cannot be used when HTTP/2 is enabled", cause));
            }
            else {
                return responseHandler.handleException(request, new RuntimeException(cause));
            }
        }

        // process response
        long responseStart = System.nanoTime();

        JettyResponse jettyResponse = null;
        T value;
        try {
            jettyResponse = new JettyResponse(response, listener.getInputStream());
            value = responseHandler.handle(request, jettyResponse);
        }
        finally {
            if (recordRequestComplete) {
                recordRequestComplete(stats, request, requestStart, jettyResponse, responseStart);
            }
        }
        return value;
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        requireNonNull(request, "request is null");
        requireNonNull(responseHandler, "responseHandler is null");

        request = applyRequestFilters(request);

        HttpRequest jettyRequest = buildJettyRequest(request, new JettyRequestListener(request.getUri()));

        JettyResponseFuture<T, E> future = new JettyResponseFuture<>(request, jettyRequest, responseHandler, stats, recordRequestComplete);

        BufferingResponseListener listener = new BufferingResponseListener(future, Ints.saturatedCast(maxContentLength));

        long requestTimestamp = System.currentTimeMillis();

        if (logEnabled) {
            addLoggingListener(jettyRequest, requestTimestamp);
        }

        try {
            jettyRequest.send(listener);
        }
        catch (RuntimeException e) {
            if (!(e instanceof RejectedExecutionException)) {
                e = new RejectedExecutionException(e);
            }
            // normally this is a rejected execution exception because the client has been closed
            future.failed(e);
            requestLogger.log(RequestInfo.from(jettyRequest, requestTimestamp), ResponseInfo.failed(Optional.empty(), Optional.of(e)));
        }
        return future;
    }

    private void addLoggingListener(HttpRequest jettyRequest, long requestTimestamp)
    {
        HttpClientLoggingListener loggingListener = new HttpClientLoggingListener(jettyRequest, requestTimestamp, requestLogger);
        jettyRequest.listener(loggingListener);
        jettyRequest.onResponseBegin(loggingListener);
        jettyRequest.onComplete(loggingListener);
    }

    private Request applyRequestFilters(Request request)
    {
        for (HttpRequestFilter requestFilter : requestFilters) {
            request = requestFilter.filterRequest(request);
        }
        return request;
    }

    private HttpRequest buildJettyRequest(Request finalRequest, JettyRequestListener listener)
    {
        HttpRequest jettyRequest = (HttpRequest) httpClient.newRequest(finalRequest.getUri());
        jettyRequest.onRequestBegin(request -> listener.onRequestBegin());
        jettyRequest.onRequestSuccess(request -> listener.onRequestEnd());
        jettyRequest.onResponseBegin(response -> listener.onResponseBegin());
        jettyRequest.onComplete(result -> listener.onFinish());
        jettyRequest.onComplete(result -> {
            if (result.isFailed() && result.getFailure() instanceof TimeoutException) {
                clientDiagnostics.logDiagnosticsInfo(httpClient);
            }
        });

        jettyRequest.attribute(STATS_KEY, listener);

        jettyRequest.method(finalRequest.getMethod());

        for (Entry<String, String> entry : finalRequest.getHeaders().entries()) {
            jettyRequest.headers(headers -> headers.add(entry.getKey(), entry.getValue()));
        }

        BodyGenerator bodyGenerator = finalRequest.getBodyGenerator();
        if (bodyGenerator != null) {
            if (bodyGenerator instanceof StaticBodyGenerator) {
                StaticBodyGenerator staticBodyGenerator = (StaticBodyGenerator) bodyGenerator;
                jettyRequest.body(new ChunkedBytesContentProvider(staticBodyGenerator.getBody(), requestBufferSizeInBytes));
            }
            else if (bodyGenerator instanceof FileBodyGenerator) {
                Path path = ((FileBodyGenerator) bodyGenerator).getPath();
                PathRequestContent content;
                try {
                    content = new PathRequestContent(path);
                }
                catch (IOException e) {
                    throw new RuntimeException(String.format("Failed to create a PathRequestContent from path [%s]", path), e);
                }
                jettyRequest.body(content);
            }
            else {
                jettyRequest.body(new BodyGeneratorContentProvider(bodyGenerator, httpClient.getExecutor()));
            }
        }

        jettyRequest.followRedirects(finalRequest.isFollowRedirects());

        setPreserveAuthorization(jettyRequest, finalRequest.isPreserveAuthorizationOnRedirect());

        // timeouts
        jettyRequest.timeout(requestTimeoutMillis, MILLISECONDS);
        jettyRequest.idleTimeout(idleTimeoutMillis, MILLISECONDS);

        return jettyRequest;
    }

    public List<HttpRequestFilter> getRequestFilters()
    {
        return requestFilters;
    }

    public long getRequestTimeoutMillis()
    {
        return requestTimeoutMillis;
    }

    @Override
    @Managed
    @Flatten
    public RequestStats getStats()
    {
        return stats;
    }

    @Override
    public long getMaxContentLength()
    {
        return maxContentLength;
    }

    @Managed
    @Nested
    public QueuedThreadPoolMBean getThreadPool()
    {
        return queuedThreadPoolMBean;
    }

    @Managed
    @Nested
    public ConnectionStats getConnectionStats()
    {
        return connectionStats;
    }

    @Managed
    @Nested
    public CachedDistribution getActiveConnectionsPerDestination()
    {
        return activeConnectionsPerDestination;
    }

    @Managed
    @Nested
    public CachedDistribution getIdleConnectionsPerDestination()
    {
        return idleConnectionsPerDestination;
    }

    @Managed
    @Nested
    public CachedDistribution getQueuedRequestsPerDestination()
    {
        return queuedRequestsPerDestination;
    }

    @Managed
    @Nested
    public CachedDistribution getCurrentQueuedTime()
    {
        return currentQueuedTime;
    }

    @Managed
    @Nested
    public CachedDistribution getCurrentRequestTime()
    {
        return currentRequestTime;
    }

    @Managed
    @Nested
    public CachedDistribution getCurrentRequestSendTime()
    {
        return currentRequestSendTime;
    }

    @Managed
    @Nested
    public CachedDistribution getCurrentResponseWaitTime()
    {
        return currentResponseWaitTime;
    }

    @Managed
    @Nested
    public CachedDistribution getCurrentResponseProcessTime()
    {
        return currentResponseProcessTime;
    }

    @Managed
    public String dump()
    {
        return httpClient.dump();
    }

    @Managed
    public void dumpStdErr()
    {
        httpClient.dumpStdErr();
    }

    @Managed
    public String dumpAllDestinations()
    {
        return format("%s\t%s\t%s\t%s\t%s\n", "URI", "queued", "request", "wait", "response") +
                httpClient.getDestinations().stream()
                        .map(JettyHttpClient::dumpDestination)
                        .collect(Collectors.joining("\n"));
    }

    @Managed
    public int getLoggerQueueSize()
    {
        return requestLogger.getQueueSize();
    }

    private static String dumpDestination(Destination destination)
    {
        long now = System.nanoTime();
        return getRequestListenersForDestination(destination).stream()
                .map(listener -> dumpRequest(now, listener))
                .sorted()
                .collect(Collectors.joining("\n"));
    }

    static List<JettyRequestListener> getRequestListenersForDestination(Destination destination)
    {
        return getRequestForDestination(destination).stream()
                .map(request -> request.getAttributes().get(STATS_KEY))
                .map(JettyRequestListener.class::cast)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static List<org.eclipse.jetty.client.Request> getRequestForDestination(Destination destination)
    {
        HttpDestination httpDestination = (HttpDestination) destination;
        Queue<HttpExchange> httpExchanges = httpDestination.getHttpExchanges();

        List<org.eclipse.jetty.client.Request> requests = httpExchanges.stream()
                .map(HttpExchange::getRequest)
                .collect(Collectors.toList());

        if (httpDestination.getConnectionPool() instanceof AbstractConnectionPool) {
            AbstractConnectionPool abstractPool = (AbstractConnectionPool) httpDestination.getConnectionPool();
            ConnectionPoolAccessor.getActiveConnections(abstractPool)
                    .stream()
                    .filter(HttpConnection.class::isInstance)
                    .map(HttpConnection.class::cast)
                    .flatMap(connection -> stream(getHttpConnectionChannels(connection)))
                    .map(HttpChannel::getHttpExchange)
                    .map(HttpExchange::getRequest)
                    .filter(Objects::nonNull)
                    .forEach(requests::add);
        }

        return requests.stream()
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private static String dumpRequest(long now, JettyRequestListener listener)
    {
        long created = listener.getCreated();
        long requestStarted = listener.getRequestStarted();
        if (requestStarted == 0) {
            requestStarted = now;
        }
        long requestFinished = listener.getRequestFinished();
        if (requestFinished == 0) {
            requestFinished = now;
        }
        long responseStarted = listener.getResponseStarted();
        if (responseStarted == 0) {
            responseStarted = now;
        }
        long finished = listener.getResponseFinished();
        if (finished == 0) {
            finished = now;
        }
        return format("%s\t%.1f\t%.1f\t%.1f\t%.1f",
                listener.getUri(),
                nanosToMillis(requestStarted - created),
                nanosToMillis(requestFinished - requestStarted),
                nanosToMillis(responseStarted - requestFinished),
                nanosToMillis(finished - responseStarted));
    }

    private static double nanosToMillis(long nanos)
    {
        return new Duration(nanos, NANOSECONDS).getValue(MILLISECONDS);
    }

    @PreDestroy
    @Override
    public void close()
    {
        // client must be destroyed before the pools or
        // you will create a several second busy wait loop
        closeQuietly(httpClient);
        closeQuietly((LifeCycle) httpClient.getExecutor());
        closeQuietly(httpClient.getScheduler());
        requestLogger.close();
    }

    @Override
    public boolean isClosed()
    {
        return !httpClient.isRunning();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(name)
                .toString();
    }

    @SuppressWarnings("UnusedDeclaration")
    public StackTraceElement[] getCreationLocation()
    {
        return creationLocation.getStackTrace();
    }

    private static void closeQuietly(LifeCycle service)
    {
        try {
            if (service != null) {
                service.stop();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception ignored) {
        }
    }

    private static String uniqueName()
    {
        return "anonymous" + NAME_COUNTER.incrementAndGet();
    }

    static void recordRequestComplete(RequestStats requestStats, Request request, long requestStart, JettyResponse response, long responseStart)
    {
        if (response == null) {
            return;
        }

        Duration responseProcessingTime = Duration.nanosSince(responseStart);
        Duration requestProcessingTime = new Duration(responseStart - requestStart, NANOSECONDS);

        requestStats.recordResponseReceived(request.getMethod(),
                response.getStatusCode(),
                response.getBytesRead(),
                response.getBytesRead(),
                requestProcessingTime,
                responseProcessingTime);
    }
}
