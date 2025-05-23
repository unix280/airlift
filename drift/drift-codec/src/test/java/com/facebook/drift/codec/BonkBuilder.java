/*
 * Copyright (C) 2012 Facebook, Inc.
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
package com.facebook.drift.codec;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.drift.codec.BonkBuilder.Builder;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
@ThriftStruct(value = "Bonk", builder = Builder.class)
public final class BonkBuilder
{
    private final String message;
    private final int type;

    public BonkBuilder(String message, int type)
    {
        this.message = message;
        this.type = type;
    }

    @ThriftField(1)
    public String getMessage()
    {
        return message;
    }

    @ThriftField(2)
    public int getType()
    {
        return type;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BonkBuilder that = (BonkBuilder) o;
        return type == that.type &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(message, type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("message", message)
                .add("type", type)
                .toString();
    }

    public static class Builder
    {
        private String message;
        private int type;

        @ThriftField
        public Builder setMessage(String message)
        {
            this.message = message;
            return this;
        }

        @ThriftField
        public Builder setType(int type)
        {
            this.type = type;
            return this;
        }

        @ThriftConstructor
        public BonkBuilder create()
        {
            return new BonkBuilder(message, type);
        }
    }
}
