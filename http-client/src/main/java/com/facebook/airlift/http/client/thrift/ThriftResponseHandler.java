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
package com.facebook.airlift.http.client.thrift;

import com.facebook.airlift.http.client.HeaderName;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.ResponseHandlerUtils;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteSource;
import com.google.common.net.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.validThriftMimeTypes;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.util.Objects.requireNonNull;
import static org.eclipse.jetty.http.HttpStatus.isSuccess;

public class ThriftResponseHandler<T>
        implements ResponseHandler<ThriftResponse<T>, RuntimeException>
{
    private final ThriftCodec<T> thriftCodec;

    public ThriftResponseHandler(ThriftCodec<T> thriftCodec)
    {
        this.thriftCodec = requireNonNull(thriftCodec, "thriftCodec is null");
    }

    @Override
    public ThriftResponse<T> handleException(Request request, Exception exception)
    {
        throw ResponseHandlerUtils.propagate(request, exception);
    }

    @Override
    public ThriftResponse<T> handle(Request request, Response response)
    {
        if (!isSuccess(response.getStatusCode())) {
            return createErrorResponse(response);
        }

        T value = null;
        IllegalArgumentException exception = null;
        try {
            Protocol protocol = getThriftProtocol(response.getHeaders());
            value = ThriftProtocolUtils.read(thriftCodec, protocol, response.getInputStream());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (Exception e) {
            exception = new IllegalArgumentException("Unable to create " + thriftCodec.getType() + " from THRIFT response", e);
        }
        return new ThriftResponse<>(response.getStatusCode(), null, response.getHeaders(), value, exception);
    }

    private ThriftResponse<T> createErrorResponse(Response response)
    {
        ByteSource byteSource = new ByteSource() {
            @Override
            public InputStream openStream() throws IOException
            {
                return response.getInputStream();
            }
        };
        try {
            String errorMessage = byteSource.asCharSource(StandardCharsets.UTF_8).read();
            return new ThriftResponse<>(response.getStatusCode(), errorMessage, response.getHeaders(), null, null);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Protocol getThriftProtocol(ListMultimap<HeaderName, String> headers)
    {
        HeaderName contentTypeHeader = HeaderName.of(CONTENT_TYPE);
        if (!headers.containsKey(contentTypeHeader) || headers.get(contentTypeHeader).size() != 1) {
            throw new IllegalArgumentException("Invalid response. Unable to create " + thriftCodec.getType() + " from THRIFT response");
        }

        MediaType mediaType = MediaType.parse(headers.get(contentTypeHeader).get(0));
        //Sample mimeType : application/x-thrift+binary
        String mimeType = mediaType.toString().toLowerCase();

        if (!validThriftMimeTypes.contains(mimeType)) {
            throw new IllegalArgumentException("Invalid response. Unable to create " + thriftCodec.getType() + " from THRIFT response");
        }

        String encodingType = mimeType.substring("application/x-thrift+".length());
        return Protocol.valueOf(encodingType.toUpperCase());
    }
}
