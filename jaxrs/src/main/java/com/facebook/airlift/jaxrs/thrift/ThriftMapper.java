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
package com.facebook.airlift.jaxrs.thrift;

import com.facebook.airlift.http.client.thrift.ThriftProtocolException;
import com.facebook.airlift.http.client.thrift.ThriftProtocolUtils;
import com.facebook.airlift.jaxrs.BaseMapper;
import com.facebook.airlift.log.Logger;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.TTransportException;
import com.facebook.drift.transport.netty.codec.Protocol;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_FB_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.validThriftMimeTypes;
import static com.facebook.airlift.jaxrs.thrift.ThriftMapper.APPLICATION_THRIFT_BINARY_QS;
import static com.facebook.airlift.jaxrs.thrift.ThriftMapper.APPLICATION_THRIFT_COMPACT_QS;
import static com.facebook.airlift.jaxrs.thrift.ThriftMapper.APPLICATION_THRIFT_FB_COMPACT_QS;
import static java.util.Objects.requireNonNull;

@Provider
@Consumes({APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
@Produces({APPLICATION_THRIFT_BINARY_QS, APPLICATION_THRIFT_COMPACT_QS, APPLICATION_THRIFT_FB_COMPACT_QS})
public class ThriftMapper
        extends BaseMapper
{
    public static final Logger log = Logger.get(ThriftMapper.class);
    static final String APPLICATION_THRIFT_BINARY_QS = "application/x-thrift+binary; qs=0.1";
    static final String APPLICATION_THRIFT_COMPACT_QS = "application/x-thrift+compact; qs=0.1";
    static final String APPLICATION_THRIFT_FB_COMPACT_QS = "application/x-thrift+fb_compact; qs=0.1";
    private final ThriftCodecManager thriftCodecManager;

    @Inject
    public ThriftMapper(ThriftCodecManager thriftCodecManager)
    {
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
    }

    @Override
    public Object readFrom(
            Class<Object> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            InputStream inputStream)
            throws IOException
    {
        try {
            ThriftCodec<?> codec = thriftCodecManager.getCodec(genericType);
            Object value = ThriftProtocolUtils.read(codec, getThriftProtocol(mediaType, codec), inputStream);
            return value;
        }
        catch (Exception e) {
            // we want to return a 400 for bad Thrift but not for a real IO exception
            if (e instanceof IOException && !(e instanceof ThriftProtocolException) && !(e instanceof EOFException)) {
                throw (IOException) e;
            }
            //The IOException is likely to be wrapped into a TTransportException
            if (e instanceof TTransportException && e.getCause() instanceof IOException && !(e.getCause() instanceof EOFException)) {
                throw (IOException) e.getCause();
            }
            // log the exception at debug so it can be viewed during development
            // Note: we are not logging at a higher level because this could cause a denial of service
            log.debug(e, "Invalid Thrift input for Java type %s", genericType);
            // Invalid thrift request. Throwing exception so the response code can be overridden using a mapper.
            throw new ThriftMapperParsingException(type, e);
        }
    }

    @Override
    public void writeTo(
            Object value,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream outputStream)
            throws IOException
    {
        try {
            ThriftCodec codec = thriftCodecManager.getCodec(type);
            ThriftProtocolUtils.write(value, codec, getThriftProtocol(mediaType, codec), outputStream);
        }
        catch (Exception e) {
            // handing EOFException same as JsonMapper
            if (e instanceof EOFException || (e instanceof TTransportException && e.getCause() instanceof EOFException)) {
                return;
            }
            log.debug(e, "Can not serialize to thrift for Java type %s", type);
            if (e instanceof ThriftProtocolException) {
                throw e;
            }
            throw new ThriftMapperParsingException(type, e);
        }
    }

    private Protocol getThriftProtocol(MediaType mediaType, ThriftCodec<?> thriftCodec)
    {
        String mimeType = mediaType.toString();

        if (!validThriftMimeTypes.contains(mimeType)) {
            throw new IllegalArgumentException("Invalid response. No protocol type specified. Unable to create " + thriftCodec.getType() + " from THRIFT response");
        }

        String encodingType = mimeType.substring("application/x-thrift+".length());
        return Protocol.valueOf(encodingType.toUpperCase());
    }
}
