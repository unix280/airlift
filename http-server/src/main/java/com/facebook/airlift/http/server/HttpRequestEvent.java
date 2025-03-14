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

import com.facebook.airlift.event.client.EventField;
import com.facebook.airlift.event.client.EventType;
import com.facebook.airlift.tracetoken.TraceTokenManager;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Request.AuthenticationState;

import java.time.Instant;
import java.util.Optional;

import static com.facebook.airlift.event.client.EventField.EventFieldMapping.TIMESTAMP;
import static com.facebook.airlift.http.server.TraceTokenFilter.TRACETOKEN_HEADER;
import static java.lang.Math.max;
import static org.eclipse.jetty.server.Request.getAuthenticationState;
import static org.eclipse.jetty.server.Request.getRemoteAddr;

@EventType("HttpRequest")
public class HttpRequestEvent
{
    public static HttpRequestEvent createHttpRequestEvent(
            Request request,
            long responseSize,
            int responseCode,
            HttpFields responseFields,
            TraceTokenManager traceTokenManager,
            long currentTimeInMillis,
            long beginToDispatchMillis,
            long afterHandleMillis,
            long firstToLastContentTimeInMillis,
            DoubleSummaryStats responseContentInterarrivalStats)
    {
        String user = null;
        AuthenticationState authenticationState = getAuthenticationState(request);
        if (authenticationState != null && authenticationState.getUserPrincipal() != null) {
            user = authenticationState.getUserPrincipal().getName();
        }

        // This is required, because async responses are processed in a different thread.

        String token = null;
        if (request.getHeaders() != null) {
            token = request.getHeaders().get(TRACETOKEN_HEADER);
        }

        if (token == null && traceTokenManager != null) {
            token = traceTokenManager.getCurrentRequestToken();
        }

        long dispatchTime = Request.getTimeStamp(request);
        long timeToDispatch = max(dispatchTime - Request.getTimeStamp(request), 0);

        Long timeToFirstByte = null;
        Object firstByteTime = request.getAttribute(TimingFilter.FIRST_BYTE_TIME);
        if (firstByteTime instanceof Long) {
            Long time = (Long) firstByteTime;
            timeToFirstByte = max(time - Request.getTimeStamp(request), 0);
        }

        long timeToLastByte = max(currentTimeInMillis - Request.getTimeStamp(request), 0);

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (getRemoteAddr(request) != null) {
            builder.add(getRemoteAddr(request));
        }
        Optional.ofNullable(request.getHeaders())
                .ifPresent(headers ->
                        headers.getFields("X-FORWARDED-FOR")
                                .forEach(field -> {
                                    String forwardedFor = field.getValue();
                                    builder.addAll(Splitter.on(',').trimResults().omitEmptyStrings().split(forwardedFor));
                                }));
        String clientAddress = null;
        ImmutableList<String> clientAddresses = builder.build();
        for (String address : Lists.reverse(clientAddresses)) {
            try {
                if (!Inet4Networks.isPrivateNetworkAddress(address)) {
                    clientAddress = address;
                    break;
                }
            }
            catch (IllegalArgumentException ignored) {
            }
        }
        if (clientAddress == null) {
            clientAddress = getRemoteAddr(request);
        }

        String requestUri = null;
        if (request.getHttpURI() != null) {
            requestUri = request.getHttpURI().toString();
        }

        String method = request.getMethod();
        if (method != null) {
            method = method.toUpperCase();
        }

        String protocol = request.getHeaders().get("X-FORWARDED-PROTO");
        if (protocol == null) {
            protocol = request.getHttpURI().getScheme();
        }
        if (protocol != null) {
            protocol = protocol.toLowerCase();
        }
        HttpFields headers = request.getHeaders();
        return new HttpRequestEvent(
                Instant.ofEpochMilli(Request.getTimeStamp(request)),
                token,
                clientAddress,
                protocol,
                method,
                requestUri,
                user,
                headers.get("User-Agent"),
                headers.get("Referer"),
                Request.getContentBytesRead(request),
                headers.get("Content-Type"),
                responseSize,
                responseCode,
                responseFields.get("Content-Type"),
                timeToDispatch,
                timeToFirstByte,
                timeToLastByte,
                beginToDispatchMillis,
                afterHandleMillis,
                firstToLastContentTimeInMillis,
                responseContentInterarrivalStats,
                request.getConnectionMetaData().getHttpVersion());
    }

    private final Instant timeStamp;
    private final String traceToken;
    private final String clientAddress;
    private final String protocol;
    private final String method;
    private final String requestUri;
    private final String user;
    private final String agent;
    private final String referrer;
    private final long requestSize;
    private final String requestContentType;
    private final long responseSize;
    private final int responseCode;
    private final String responseContentType;
    private final long timeToDispatch;
    private final Long timeToFirstByte;
    private final long timeToLastByte;
    private final long beginToDispatchMillis;
    private final long afterDispatchMillis;
    private final long firstToLastContentTimeInMillis;
    private final DoubleSummaryStats responseContentInterarrivalStats;
    private final String protocolVersion;

    public HttpRequestEvent(
            Instant timeStamp,
            String traceToken,
            String clientAddress,
            String protocol,
            String method,
            String requestUri,
            String user,
            String agent,
            String referrer,
            long requestSize,
            String requestContentType,
            long responseSize,
            int responseCode,
            String responseContentType,
            long timeToDispatch,
            Long timeToFirstByte,
            long timeToLastByte,
            long beginToDispatchMillis,
            long afterDispatchMillis,
            long firstToLastContentTimeInMillis,
            DoubleSummaryStats responseContentInterarrivalStats,
            HttpVersion protocolVersion)
    {
        this.timeStamp = timeStamp;
        this.traceToken = traceToken;
        this.clientAddress = clientAddress;
        this.protocol = protocol;
        this.method = method;
        this.requestUri = requestUri;
        this.user = user;
        this.agent = agent;
        this.referrer = referrer;
        this.requestSize = requestSize;
        this.requestContentType = requestContentType;
        this.responseSize = responseSize;
        this.responseCode = responseCode;
        this.responseContentType = responseContentType;
        this.timeToDispatch = timeToDispatch;
        this.timeToFirstByte = timeToFirstByte;
        this.timeToLastByte = timeToLastByte;
        this.beginToDispatchMillis = beginToDispatchMillis;
        this.afterDispatchMillis = afterDispatchMillis;
        this.firstToLastContentTimeInMillis = firstToLastContentTimeInMillis;
        this.responseContentInterarrivalStats = responseContentInterarrivalStats;
        this.protocolVersion = protocolVersion.toString();
    }

    @EventField(fieldMapping = TIMESTAMP)
    public Instant getTimeStamp()
    {
        return timeStamp;
    }

    @EventField
    public String getTraceToken()
    {
        return traceToken;
    }

    @EventField
    public String getClientAddress()
    {
        return clientAddress;
    }

    @EventField
    public String getProtocol()
    {
        return protocol;
    }

    @EventField
    public String getMethod()
    {
        return method;
    }

    @EventField
    public String getRequestUri()
    {
        return requestUri;
    }

    @EventField
    public String getUser()
    {
        return user;
    }

    @EventField
    public String getAgent()
    {
        return agent;
    }

    @EventField
    public String getReferrer()
    {
        return referrer;
    }

    @EventField
    public long getRequestSize()
    {
        return requestSize;
    }

    @EventField
    public String getRequestContentType()
    {
        return requestContentType;
    }

    @EventField
    public long getResponseSize()
    {
        return responseSize;
    }

    @EventField
    public int getResponseCode()
    {
        return responseCode;
    }

    @EventField
    public String getResponseContentType()
    {
        return responseContentType;
    }

    @EventField
    public long getTimeToDispatch()
    {
        return timeToDispatch;
    }

    @EventField
    public Long getTimeToFirstByte()
    {
        return timeToFirstByte;
    }

    @EventField
    public long getTimeToLastByte()
    {
        return timeToLastByte;
    }

    @EventField
    public long getBeginToDispatchMillis()
    {
        return beginToDispatchMillis;
    }

    @EventField
    public long getAfterDispatchMillis()
    {
        return afterDispatchMillis;
    }

    @EventField
    public long getFirstToLastContentTimeInMillis()
    {
        return firstToLastContentTimeInMillis;
    }

    @EventField
    public DoubleSummaryStats getResponseContentInterarrivalStats()
    {
        return responseContentInterarrivalStats;
    }

    @EventField
    public String getProtocolVersion()
    {
        return protocolVersion;
    }
}
