/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.airlift.units;

// forked from com.google.common.base.Preconditions
final class Preconditions
{
    private Preconditions() { }

    static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    static void checkArgument(boolean expression, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (!expression) {
            throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    static void checkState(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalStateException(errorMessage);
        }
    }

    static void checkState(boolean expression, String errorMessageTemplate, Object... errorMessageArgs)
    {
        if (!expression) {
            throw new IllegalStateException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    private static String format(String template, Object... args)
    {
        template = String.valueOf(template); // null -> "null"

        // start substituting the arguments into the '%s' placeholders
        StringBuilder builder = new StringBuilder(template.length() + 16 * args.length);
        int templateStart = 0;
        int i = 0;
        while (i < args.length) {
            int placeholderStart = template.indexOf("%s", templateStart);
            if (placeholderStart == -1) {
                break;
            }
            builder.append(template.substring(templateStart, placeholderStart));
            builder.append(args[i++]);
            templateStart = placeholderStart + 2;
        }
        builder.append(template.substring(templateStart));

        // if we run out of placeholders, append the extra args in square braces
        if (i < args.length) {
            builder.append(" [");
            builder.append(args[i++]);
            while (i < args.length) {
                builder.append(", ");
                builder.append(args[i++]);
            }
            builder.append(']');
        }

        return builder.toString();
    }
}
