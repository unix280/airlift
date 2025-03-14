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
package com.facebook.airlift.jaxrs;

import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
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

@Provider
@Consumes("application/x-jackson-smile")
@Produces("application/x-jackson-smile; qs=0.1")
public class SmileMapper
        extends BaseMapper
{
    public static final Logger log = Logger.get(SmileMapper.class);

    private final ObjectMapper objectMapper;

    @Inject
    public SmileMapper(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public Object readFrom(Class<Object> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, String> httpHeaders,
            InputStream inputStream)
            throws IOException
    {
        Object object;
        try {
            JsonParser jsonParser = new SmileFactory().createParser(inputStream);

            // Important: we are NOT to close the underlying stream after
            // mapping, so we need to instruct parser:
            jsonParser.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

            object = objectMapper.readValue(jsonParser, objectMapper.getTypeFactory().constructType(genericType));
        }
        catch (Exception e) {
            // we want to return a 400 for bad JSON but not for a real IO exception
            if (e instanceof IOException && !(e instanceof JsonProcessingException) && !(e instanceof EOFException)) {
                throw (IOException) e;
            }

            // log the exception at debug so it can be viewed during development
            // Note: we are not logging at a higher level because this could cause a denial of service
            log.debug(e, "Invalid json for Java type %s", type);

            // Invalid json request. Throwing exception so the response code can be overridden using a mapper.
            throw new JsonMapperParsingException(type, e);
        }
        return object;
    }

    @Override
    public void writeTo(Object value,
            Class<?> type,
            Type genericType,
            Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String, Object> httpHeaders,
            OutputStream outputStream)
            throws IOException
    {
        JsonGenerator jsonGenerator = new SmileFactory().createGenerator(outputStream);

        // Important: we are NOT to close the underlying stream after
        // mapping, so we need to instruct generator:
        jsonGenerator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

        // 04-Mar-2010, tatu: How about type we were given? (if any)
        JavaType rootType = null;
        if (genericType != null && value != null) {
            // 10-Jan-2011, tatu: as per [JACKSON-456], it's not safe to just force root
            //    type since it prevents polymorphic type serialization. Since we really
            //    just need this for generics, let's only use generic type if it's truly
            //    generic.
            if (genericType.getClass() != Class.class) { // generic types are other implementations of 'java.lang.reflect.Type'
                // This is still not exactly right; should root type be further
                // specialized with 'value.getClass()'? Let's see how well this works before
                // trying to come up with more complete solution.
                rootType = objectMapper.getTypeFactory().constructType(genericType);
                // 26-Feb-2011, tatu: To help with [JACKSON-518], we better recognize cases where
                //    type degenerates back into "Object.class" (as is the case with plain TypeVariable,
                //    for example), and not use that.
                //
                if (rootType.getRawClass() == Object.class) {
                    rootType = null;
                }
            }
        }

        if (rootType != null) {
            objectMapper.writerFor(rootType).writeValue(jsonGenerator, value);
        }
        else {
            objectMapper.writeValue(jsonGenerator, value);
        }
    }
}
