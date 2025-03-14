/*
 * Copyright 2010 Proofpoint, Inc.
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
package com.facebook.airlift.http.server;

import com.facebook.airlift.event.client.InMemoryEventClient;
import com.facebook.airlift.tracetoken.TraceTokenManager;
import com.google.common.collect.ImmutableList;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.DoubleSummaryStatistics;
import java.util.List;

import static com.facebook.airlift.http.server.TraceTokenFilter.TRACETOKEN_HEADER;
import static com.google.common.io.Files.asCharSource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.eclipse.jetty.http.HttpVersion.HTTP_2;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestDelimitedRequestLog
{
    private static final DateTimeFormatter ISO_FORMATTER = ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault());

    private File file;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        file = File.createTempFile(getClass().getName(), ".log");
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        if (!file.delete()) {
            throw new IOException("Error deleting " + file.getAbsolutePath());
        }
    }

    @Test
    public void testTraceTokenHeader()
            throws Exception
    {
        Request request = setupMockRequest();
        ConnectionMetaData connectionMetaData = mock(ConnectionMetaData.class);
        TraceTokenManager tokenManager = new TraceTokenManager();
        InMemoryEventClient eventClient = new InMemoryEventClient();
        DelimitedRequestLog logger = new DelimitedRequestLog(
                file.getAbsolutePath(),
                1,
                256,
                Long.MAX_VALUE,
                tokenManager,
                eventClient,
                new SystemCurrentTimeMillisProvider(),
                false);
        String token = "test-trace-token";
        HttpFields.Mutable headers = HttpFields.build()
                .add(TRACETOKEN_HEADER, token);
        when(connectionMetaData.getHttpVersion()).thenReturn(HTTP_2);
        when(request.getConnectionMetaData()).thenReturn(connectionMetaData);
        when(request.getHeaders()).thenReturn(headers);
        // log a request without a token set by tokenManager
        logger.log(request, 0, 200, headers, 0, 0, 0, new DoubleSummaryStats(new DoubleSummaryStatistics()));
        // create and set a new token with tokenManager
        tokenManager.createAndRegisterNewRequestToken();
        logger.log(request, 0, 200, headers, 0, 0, 0, new DoubleSummaryStats(new DoubleSummaryStatistics()));
        // clear the token HTTP header
        when(request.getHeaders()).thenReturn(HttpFields.EMPTY);
        logger.log(request, 0, 200, HttpFields.EMPTY, 0, 0, 0, new DoubleSummaryStats(new DoubleSummaryStatistics()));
        logger.stop();

        List<Object> events = eventClient.getEvents();
        assertEquals(events.size(), 3);
        // first two events should have the token set from the header
        for (int i = 0; i < 2; i++) {
            assertEquals(((HttpRequestEvent) events.get(i)).getTraceToken(), token);
        }
        // last event should have the token set by the tokenManager
        assertEquals(((HttpRequestEvent) events.get(2)).getTraceToken(), tokenManager.getCurrentRequestToken());
    }

    @Test
    public void testWriteLog()
            throws Exception
    {
        Request request = setupMockRequest();
        Response response = mock(Response.class);
        ConnectionMetaData connectionMetaData = mock(ConnectionMetaData.class);
        Request.AuthenticationState authenticationState = mock(Request.AuthenticationState.class);
        Principal principal = mock(Principal.class);

        long timeToFirstByte = 456;
        long timeToLastByte = 3453;
        long now = System.currentTimeMillis();
        long timestamp = now - timeToLastByte;
        String user = "martin";
        String agent = "HttpClient 4.0";
        String referrer = "http://www.google.com";
        String ip = "4.4.4.4";
        String protocol = "protocol";
        String method = "GET";
        long requestSize = 5432;
        String requestContentType = "request/type";
        long responseSize = 32311;
        int responseCode = 200;
        String responseContentType = "response/type";
        HttpURI uri = HttpURI.build("http://www.example.com/aaa+bbb/ccc?param=hello%20there&other=true");
        long beginToDispatchMillis = 333;
        long firstToLastContentTimeInMillis = 444;
        long afterHandleMillis = 555;
        DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
        stats.accept(1);
        stats.accept(3);
        DoubleSummaryStats responseContentInterarrivalStats = new DoubleSummaryStats(stats);

        TraceTokenManager tokenManager = new TraceTokenManager();
        InMemoryEventClient eventClient = new InMemoryEventClient();
        MockCurrentTimeMillisProvider currentTimeMillisProvider = new MockCurrentTimeMillisProvider(timestamp + timeToLastByte);
        DelimitedRequestLog logger = new DelimitedRequestLog(file.getAbsolutePath(), 1, 256, Long.MAX_VALUE, tokenManager, eventClient, currentTimeMillisProvider, false);
        HttpFields.Mutable headers = HttpFields.build()
                .add("User-Agent", agent)
                .add("Referer", referrer)
                .add("X-FORWARDED-FOR", ImmutableList.of("1.1.1.1, 2.2.2.2", "3.3.3.3, " + ip))
                .add("X-FORWARDED-PROTO", protocol)
                .add("Content-Type", requestContentType);
        when(principal.getName()).thenReturn(user);
        when(authenticationState.getUserPrincipal()).thenReturn(principal);
        when(connectionMetaData.getProtocol()).thenReturn("unknown");
        when(connectionMetaData.getHttpVersion()).thenReturn(HTTP_2);
        when(request.getHeaders()).thenReturn(headers);
        when(request.getConnectionMetaData()).thenReturn(connectionMetaData);
        when(request.getHttpURI()).thenReturn(uri);
        when(request.getAttribute(TimingFilter.FIRST_BYTE_TIME)).thenReturn(timestamp + timeToFirstByte);
        when(request.getMethod()).thenReturn(method);
        when(response.getStatus()).thenReturn(responseCode);
        try (MockedStatic<Request> requestMock = mockStatic(Request.class)) {
            requestMock.when(() -> Request.getTimeStamp(request)).thenReturn(timestamp);
            requestMock.when(() -> Request.getRemoteAddr(request)).thenReturn("9.9.9.9");
            requestMock.when(() -> Request.getAuthenticationState(request)).thenReturn(authenticationState);
            requestMock.when(() -> Request.getContentBytesRead(request)).thenReturn(requestSize);
            HttpFields.Mutable responseHeaders = HttpFields.build()
                    .put("Content-Type", responseContentType);

            tokenManager.createAndRegisterNewRequestToken();
            logger.log(request, responseSize, responseCode, responseHeaders, beginToDispatchMillis, afterHandleMillis, firstToLastContentTimeInMillis, responseContentInterarrivalStats);
            logger.stop();
        }

        List<Object> events = eventClient.getEvents();
        assertEquals(events.size(), 1);
        HttpRequestEvent event = (HttpRequestEvent) events.get(0);

        assertEquals(event.getTimeStamp().toEpochMilli(), timestamp);
        assertEquals(event.getClientAddress(), ip);
        assertEquals(event.getProtocol(), protocol);
        assertEquals(event.getMethod(), method);
        assertEquals(event.getRequestUri(), uri.toString());
        assertEquals(event.getUser(), user);
        assertEquals(event.getAgent(), agent);
        assertEquals(event.getReferrer(), referrer);
        assertEquals(event.getRequestSize(), requestSize);
        assertEquals(event.getRequestContentType(), requestContentType);
        assertEquals(event.getResponseSize(), responseSize);
        assertEquals(event.getResponseCode(), responseCode);
        assertEquals(event.getResponseContentType(), responseContentType);
        assertEquals(event.getTimeToFirstByte(), (Long) timeToFirstByte);
        assertEquals(event.getTimeToLastByte(), timeToLastByte);
        assertEquals(event.getTraceToken(), tokenManager.getCurrentRequestToken());
        assertEquals(event.getBeginToDispatchMillis(), beginToDispatchMillis);
        assertEquals(event.getFirstToLastContentTimeInMillis(), firstToLastContentTimeInMillis);
        assertEquals(event.getResponseContentInterarrivalStats(), responseContentInterarrivalStats);

        String actual = asCharSource(file, UTF_8).read();
        String expected = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
                ISO_FORMATTER.format(Instant.ofEpochMilli(timestamp)),
                ip,
                method,
                uri,
                user,
                agent,
                responseSize,
                responseCode,
                requestSize,
                event.getTimeToLastByte(),
                tokenManager.getCurrentRequestToken(),
                HTTP_2,
                beginToDispatchMillis,
                afterHandleMillis,
                firstToLastContentTimeInMillis,
                format("%.2f, %.2f, %.2f, %d", stats.getMin(), stats.getAverage(), stats.getMax(), stats.getCount()));
        assertEquals(actual, expected);
    }

    @Test
    public void testNoXForwardedProto()
            throws Exception
    {
        Request request = setupMockRequest();
        ConnectionMetaData connectionMetaData = mock(ConnectionMetaData.class);
        String protocol = "protocol";

        when(request.getConnectionMetaData()).thenReturn(connectionMetaData);
        when(request.getHttpURI()).thenReturn(HttpURI.build("protocol://localhost:8080"));
        when(connectionMetaData.getHttpVersion()).thenReturn(HTTP_2);

        InMemoryEventClient eventClient = new InMemoryEventClient();
        DelimitedRequestLog logger = new DelimitedRequestLog(file.getAbsolutePath(), 1, 256, Long.MAX_VALUE, null, eventClient, false);
        logger.log(request, 200, 0, HttpFields.EMPTY, 0, 0, 0, new DoubleSummaryStats(new DoubleSummaryStatistics()));
        logger.stop();

        List<Object> events = eventClient.getEvents();
        assertEquals(events.size(), 1);
        HttpRequestEvent event = (HttpRequestEvent) events.get(0);

        assertEquals(event.getProtocol(), protocol);
    }

    @Test
    public void testNoTimeToFirstByte()
            throws Exception
    {
        Request request = setupMockRequest();

        InMemoryEventClient eventClient = new InMemoryEventClient();
        DelimitedRequestLog logger = new DelimitedRequestLog(file.getAbsolutePath(), 1, 256, Long.MAX_VALUE, null, eventClient, false);
        logger.log(request, 200, 0, HttpFields.EMPTY, 0, 0, 0, new DoubleSummaryStats(new DoubleSummaryStatistics()));
        logger.stop();

        List<Object> events = eventClient.getEvents();
        assertEquals(events.size(), 1);
        HttpRequestEvent event = (HttpRequestEvent) events.get(0);

        assertNull(event.getTimeToFirstByte());
    }

    private Request setupMockRequest()
    {
        Request request = mock(Request.class);
        ConnectionMetaData connectionMetaData = mock(ConnectionMetaData.class);
        when(request.getConnectionMetaData()).thenReturn(connectionMetaData);
        when(connectionMetaData.getHttpVersion()).thenReturn(HTTP_2);
        when(request.getHeaders()).thenReturn(HttpFields.EMPTY);
        when(request.getHttpURI()).thenReturn(HttpURI.build("http://localhost"));
        return request;
    }

    @Test
    public void testNoXForwardedFor()
            throws Exception
    {
        Request request = setupMockRequest();
        ConnectionMetaData connectionMetaData = mock(ConnectionMetaData.class);
        String clientIp = "1.1.1.1";

        when(connectionMetaData.getHttpVersion()).thenReturn(HTTP_2);
        when(request.getConnectionMetaData()).thenReturn(connectionMetaData);
        try (MockedStatic<Request> mockedRequest = mockStatic(Request.class)) {
            mockedRequest.when(() -> Request.getRemoteAddr(request)).thenReturn(clientIp);
            InMemoryEventClient eventClient = new InMemoryEventClient();
            DelimitedRequestLog logger = new DelimitedRequestLog(file.getAbsolutePath(), 1, 256, Long.MAX_VALUE, null, eventClient, false);
            logger.log(request, 200, 0, HttpFields.EMPTY, 0, 0, 0, new DoubleSummaryStats(new DoubleSummaryStatistics()));
            logger.stop();

            List<Object> events = eventClient.getEvents();
            assertEquals(events.size(), 1);
            HttpRequestEvent event = (HttpRequestEvent) events.get(0);

            assertEquals(event.getClientAddress(), clientIp);
        }
    }

    @Test
    public void testXForwardedForSkipPrivateAddresses()
            throws Exception
    {
        Request request = mock(Request.class);

        ConnectionMetaData connectionMetaData = mock(ConnectionMetaData.class);
        when(request.getConnectionMetaData()).thenReturn(connectionMetaData);
        when(connectionMetaData.getHttpVersion()).thenReturn(HTTP_2);
        when(request.getHttpURI()).thenReturn(HttpURI.build("http://localhost:8080/test"));
        String clientIp = "1.1.1.1";
        HttpFields.Mutable fields = HttpFields.build();
        ImmutableList.of(clientIp, "192.168.1.2, 172.16.0.1", "169.254.1.2, 127.1.2.3", "10.1.2.3")
                .forEach(ip -> fields.add("X-FORWARDED-FOR", ip));
        when(request.getHeaders()).thenReturn(fields);
        try (MockedStatic<Request> requestMockedStatic = mockStatic(Request.class)) {
            requestMockedStatic.when(() -> Request.getRemoteAddr(request)).thenReturn("9.9.9.9");

            InMemoryEventClient eventClient = new InMemoryEventClient();
            DelimitedRequestLog logger = new DelimitedRequestLog(file.getAbsolutePath(), 1, 256, Long.MAX_VALUE, null, eventClient, false);
            logger.log(request, 200, 0, fields, 0, 0, 0, new DoubleSummaryStats(new DoubleSummaryStatistics()));
            logger.stop();

            List<Object> events = eventClient.getEvents();
            assertEquals(events.size(), 1);
            HttpRequestEvent event = (HttpRequestEvent) events.get(0);

            assertEquals(event.getClientAddress(), clientIp);
        }
    }
}
