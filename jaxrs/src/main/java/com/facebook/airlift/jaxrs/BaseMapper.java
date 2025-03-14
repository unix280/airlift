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
package com.facebook.airlift.jaxrs;

import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.MessageBodyReader;
import jakarta.ws.rs.ext.MessageBodyWriter;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Set;

// This code is based on JacksonJsonProvider
public abstract class BaseMapper
        implements MessageBodyReader<Object>, MessageBodyWriter<Object>
{
    /**
     * Looks like we need to worry about accidental
     * data binding for types we shouldn't be handling. This is
     * probably not a very good way to do it, but let's start by
     * blacklisting things we are not to handle.
     */
    private static final Set<Class<?>> IO_CLASSES = ImmutableSet.<Class<?>>builder()
            .add(InputStream.class)
            .add(java.io.Reader.class)
            .add(OutputStream.class)
            .add(java.io.Writer.class)
            .add(byte[].class)
            .add(char[].class)
            .add(jakarta.ws.rs.core.StreamingOutput.class)
            .add(Response.class)
            .build();

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return canReadOrWrite(type);
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return canReadOrWrite(type);
    }

    private static boolean canReadOrWrite(Class<?> type)
    {
        if (IO_CLASSES.contains(type)) {
            return false;
        }
        for (Class<?> ioClass : IO_CLASSES) {
            if (ioClass.isAssignableFrom(type)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public long getSize(Object value, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        // In general figuring output size requires actual writing; usually not
        // worth it to write everything twice.
        return -1;
    }
}
