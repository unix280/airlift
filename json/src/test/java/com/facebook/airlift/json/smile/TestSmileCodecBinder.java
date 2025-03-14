package com.facebook.airlift.json.smile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import jakarta.inject.Inject;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static org.testng.Assert.assertNotNull;

public class TestSmileCodecBinder
{
    @Test
    public void ignoresRepeatedBinding()
    {
        Injector injector = Guice.createInjector(binder -> {
            smileCodecBinder(binder).bindSmileCodec(Integer.class);
            smileCodecBinder(binder).bindSmileCodec(Integer.class);
            binder.bind(ObjectMapper.class).annotatedWith(ForSmile.class).toProvider(SmileObjectMapperProvider.class);
            binder.bind(Dummy.class).in(Scopes.SINGLETON);
        });

        assertNotNull(injector.getInstance(Dummy.class).getCodec());
    }

    private static class Dummy
    {
        private final SmileCodec<Integer> codec;

        @Inject
        public Dummy(SmileCodec<Integer> codec)
        {
            this.codec = codec;
        }

        public SmileCodec<Integer> getCodec()
        {
            return codec;
        }
    }
}
