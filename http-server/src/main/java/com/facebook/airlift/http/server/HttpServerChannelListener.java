/*
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

import jakarta.annotation.Nullable;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.EventsHandler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class HttpServerChannelListener
        extends EventsHandler
{
    private static final String REQUEST_BEFORE_HANDLE = HttpServerChannelListener.class.getName() + ".before_handle";
    private static final String REQUEST_RESPONSE_BEGIN = HttpServerChannelListener.class.getName() + ".response_begin";
    private static final String RESPONSE_CONTENT_TIMESTAMPS_ATTRIBUTE = HttpServerChannelListener.class.getName() + ".response_content_timestamps";
    private static final String RESPONSE_CONTENT_SIZES_ATTRIBUTE = HttpServerChannelListener.class.getName() + ".response_content_sizes";
    private static final String REQUEST_AFTER_HANDLE = HttpServerChannelListener.class.getName() + ".after_handle";

    private final DelimitedRequestLog logger;

    public HttpServerChannelListener(DelimitedRequestLog logger, Handler handler)
    {
        super(handler);
        this.logger = requireNonNull(logger, "logger is null");
    }

    @Override
    protected void onBeforeHandling(Request request)
    {
        request.setAttribute(REQUEST_BEFORE_HANDLE, System.nanoTime());
    }

    @Override
    protected void onResponseBegin(Request request, int status, HttpFields headers)
    {
        request.setAttribute(REQUEST_RESPONSE_BEGIN, System.nanoTime());
        if (request.getAttribute(RESPONSE_CONTENT_TIMESTAMPS_ATTRIBUTE) == null) {
            request.setAttribute(RESPONSE_CONTENT_TIMESTAMPS_ATTRIBUTE, new ArrayList<>());
        }
    }

    @Override
    protected void onResponseWrite(Request request, boolean last, ByteBuffer content)
    {
        if (request.getAttribute(RESPONSE_CONTENT_TIMESTAMPS_ATTRIBUTE) == null) {
            request.setAttribute(RESPONSE_CONTENT_TIMESTAMPS_ATTRIBUTE, new ArrayList<>());
        }
        List<Long> contentTimestamps = (List<Long>) request.getAttribute(RESPONSE_CONTENT_TIMESTAMPS_ATTRIBUTE);
        contentTimestamps.add(System.nanoTime());

        if (request.getAttribute(RESPONSE_CONTENT_SIZES_ATTRIBUTE) == null) {
            request.setAttribute(RESPONSE_CONTENT_SIZES_ATTRIBUTE, new AtomicLong(0L));
        }
        AtomicLong responseSize = (AtomicLong) request.getAttribute(RESPONSE_CONTENT_SIZES_ATTRIBUTE);
        responseSize.addAndGet(content.remaining());
    }

    @Override
    protected void onComplete(Request request, @Nullable Throwable failure)
    {
        onCompleteHelper(request, -1, HttpFields.EMPTY, failure);
    }

    @Override
    protected void onComplete(Request request, int status, HttpFields headers, @Nullable Throwable failure)
    {
        onCompleteHelper(request, status, headers, failure);
    }

    private void onCompleteHelper(Request request, int status, HttpFields headers, @Nullable Throwable failure)
    {
        long requestBeginTime = (Long) request.getAttribute(REQUEST_BEFORE_HANDLE);
        List<Long> contentTimestamps = Optional.ofNullable((List<Long>) request.getAttribute(RESPONSE_CONTENT_TIMESTAMPS_ATTRIBUTE))
                .orElseGet(ArrayList::new);
        long firstToLastContentTimeInMillis = -1;
        if (!contentTimestamps.isEmpty()) {
            firstToLastContentTimeInMillis = NANOSECONDS.toMillis(contentTimestamps.get(contentTimestamps.size() - 1) - contentTimestamps.get(0));
        }
        Long afterHandleMillis = (Long) request.getAttribute(REQUEST_AFTER_HANDLE);
        if (afterHandleMillis == null) {
            afterHandleMillis = NANOSECONDS.toMillis(System.nanoTime());
        }
        long responseSize = Optional.ofNullable((AtomicLong) request.getAttribute(RESPONSE_CONTENT_SIZES_ATTRIBUTE))
                .map(AtomicLong::get)
                .orElse(0L);
        logger.log(request,
                responseSize,
                status,
                headers,
                requestBeginTime,
                afterHandleMillis,
                firstToLastContentTimeInMillis,
                processContentTimestamps(contentTimestamps));
    }

    @Override
    protected void onAfterHandling(Request request, boolean handled, Throwable failure)
    {
        request.setAttribute(REQUEST_AFTER_HANDLE, System.nanoTime());
    }

    /**
     * Calculate the summary statistics for the interarrival time of the onResponseContent callbacks.
     */
    @Nullable
    private static DoubleSummaryStats processContentTimestamps(List<Long> contentTimestamps)
    {
        requireNonNull(contentTimestamps, "contentTimestamps is null");

        // no content (HTTP 204) or there was a single response chunk (so no interarrival time)
        if (contentTimestamps.size() == 0 || contentTimestamps.size() == 1) {
            return null;
        }

        DoubleSummaryStatistics statistics = new DoubleSummaryStatistics();
        long previousTimestamp = contentTimestamps.get(0);
        for (int i = 1; i < contentTimestamps.size(); i++) {
            long timestamp = contentTimestamps.get(i);
            statistics.accept(NANOSECONDS.toMillis(timestamp - previousTimestamp));
            previousTimestamp = timestamp;
        }
        return new DoubleSummaryStats(statistics);
    }
}
