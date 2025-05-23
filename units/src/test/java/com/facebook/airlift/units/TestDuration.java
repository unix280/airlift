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
package com.facebook.airlift.units;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.testing.EquivalenceTester.comparisonTester;
import static com.facebook.airlift.units.Duration.succinctDuration;
import static com.facebook.airlift.units.Duration.succinctNanos;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

public class TestDuration
{
    @Test
    public void testSuccinctFactories()
    {
        assertEquals(succinctNanos(123), new Duration(123, NANOSECONDS));
        assertEquals(succinctNanos(123456), new Duration(123.456, MICROSECONDS));
        assertEquals(succinctNanos(SECONDS.toNanos(300)), new Duration(5, MINUTES));

        assertEquals(succinctDuration(123, NANOSECONDS), new Duration(123, NANOSECONDS));
        assertEquals(succinctDuration(123456, NANOSECONDS), new Duration(123.456, MICROSECONDS));
        assertEquals(succinctDuration(300, SECONDS), new Duration(5, MINUTES));
    }

    @Test
    public void testGetValue()
    {
        double millis = 12346789.0d;
        Duration duration = new Duration(millis, MILLISECONDS);
        assertThat(duration.getValue(MILLISECONDS)).isEqualTo(millis);
        assertThat(duration.getValue(SECONDS)).isCloseTo(millis / 1000, offset(0.001));
        assertThat(duration.getValue(MINUTES)).isCloseTo(millis / 1000 / 60, offset(0.001));
        assertThat(duration.getValue(HOURS)).isCloseTo(millis / 1000 / 60 / 60, offset(0.001));
        assertThat(duration.getValue(DAYS)).isCloseTo(millis / 1000 / 60 / 60 / 24, offset(0.001));

        double days = 3.0;
        duration = new Duration(days, DAYS);
        assertThat(duration.getValue(DAYS)).isEqualTo(days);
        assertThat(duration.getValue(HOURS)).isCloseTo(days * 24, offset(0.001));
        assertThat(duration.getValue(MINUTES)).isCloseTo(days * 24 * 60, offset(0.001));
        assertThat(duration.getValue(SECONDS)).isCloseTo(days * 24 * 60 * 60, offset(0.001));
        assertThat(duration.getValue(MILLISECONDS)).isCloseTo(days * 24 * 60 * 60 * 1000, offset(0.001));
    }

    @Test(dataProvider = "conversions")
    public void testConversions(TimeUnit unit, TimeUnit toTimeUnit, double factor)
    {
        Duration duration = new Duration(1, unit).convertTo(toTimeUnit);
        assertThat(duration.getUnit()).isEqualTo(toTimeUnit);
        assertThat(duration.getValue()).isCloseTo(factor, withPercentage(0.1));
        assertThat(duration.getValue(toTimeUnit)).isCloseTo(factor, withPercentage(0.1));
    }

    @Test(dataProvider = "conversions")
    public void testConvertToMostSuccinctDuration(TimeUnit unit, TimeUnit toTimeUnit, double factor)
    {
        Duration duration = new Duration(factor, toTimeUnit);
        Duration actual = duration.convertToMostSuccinctTimeUnit();
        assertThat(actual.getValue(toTimeUnit)).isCloseTo(factor, withPercentage(0.1));
        assertThat(actual.getValue(unit)).isCloseTo(1.0, offset(0.001));
        assertThat(actual.getUnit()).isEqualTo(unit);
    }

    @Test
    public void testEquivalence()
    {
        comparisonTester()
                .addLesserGroup(generateTimeBucket(0))
                .addGreaterGroup(generateTimeBucket(1))
                .addGreaterGroup(generateTimeBucket(123352))
                .addGreaterGroup(generateTimeBucket(Long.MAX_VALUE))
                .check();
    }

    private static List<Duration> generateTimeBucket(double seconds)
    {
        List<Duration> bucket = new ArrayList<>();
        bucket.add(new Duration(seconds * 1000 * 1000 * 1000, NANOSECONDS));
        bucket.add(new Duration(seconds * 1000 * 1000, MICROSECONDS));
        bucket.add(new Duration(seconds * 1000, MILLISECONDS));
        bucket.add(new Duration(seconds, SECONDS));
        bucket.add(new Duration(seconds / 60, MINUTES));
        bucket.add(new Duration(seconds / 60 / 60, HOURS));
        // skip days for larger values as this results in rounding errors
        if (seconds <= 1.0) {
            bucket.add(new Duration(seconds / 60 / 60 / 24, DAYS));
        }
        return bucket;
    }

    @Test(dataProvider = "printedValues")
    public void testToString(String expectedString, double value, TimeUnit unit)
    {
        assertEquals(new Duration(value, unit).toString(), expectedString);
    }

    @Test(dataProvider = "printedValues")
    public void testNonEnglishLocale(String expectedString, double value, TimeUnit unit)
            throws Exception
    {
        synchronized (Locale.class) {
            Locale previous = Locale.getDefault();
            Locale.setDefault(Locale.GERMAN);
            try {
                assertEquals(new Duration(value, unit).toString(), expectedString);
            }
            finally {
                Locale.setDefault(previous);
            }
        }
    }

    @Test(dataProvider = "parseableValues")
    public void testValueOf(String string, double expectedValue, TimeUnit expectedUnit)
    {
        Duration duration = Duration.valueOf(string);

        assertEquals(duration.getUnit(), expectedUnit);
        assertEquals(duration.getValue(), expectedValue);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "duration is null")
    public void testValueOfRejectsNull()
    {
        Duration.valueOf(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "duration is empty")
    public void testValueOfRejectsEmptyString()
    {
        Duration.valueOf("");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown time unit: kg")
    public void testValueOfRejectsInvalidUnit()
    {
        Duration.valueOf("1.234 kg");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "duration is not a valid.*")
    public void testValueOfRejectsInvalidNumber()
    {
        Duration.valueOf("1.2x4 s");
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "value is negative")
    public void testConstructorRejectsNegativeValue()
    {
        new Duration(-1, SECONDS);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "value is infinite")
    public void testConstructorRejectsInfiniteValue()
    {
        new Duration(Double.POSITIVE_INFINITY, SECONDS);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "value is infinite")
    public void testConstructorRejectsInfiniteValue2()
    {
        new Duration(Double.NEGATIVE_INFINITY, SECONDS);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "value is not a number")
    public void testConstructorRejectsNaN()
    {
        new Duration(Double.NaN, SECONDS);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "unit is null")
    public void testConstructorRejectsNullUnit()
    {
        new Duration(1, null);
    }

    @Test
    public void testEquals()
    {
        assertEquals(new Duration(12359.0d, MILLISECONDS), new Duration(12359.0d, MILLISECONDS));
        assertFalse(new Duration(12359.0d, MILLISECONDS).equals(new Duration(4444.0d, MILLISECONDS)));
    }

    @Test
    public void testHashCode()
    {
        assertEquals(new Duration(12359.0d, MILLISECONDS).hashCode(), new Duration(12359.0d, MILLISECONDS).hashCode());
        assertFalse(new Duration(12359.0d, MILLISECONDS).hashCode() == new Duration(4444.0d, MILLISECONDS).hashCode());
    }

    @Test
    public void testNanoConversions()
    {
        double nanos = 1.0d;
        Duration duration = new Duration(nanos, NANOSECONDS);
        assertThat(duration.getValue()).isEqualTo(nanos);
        assertThat(duration.getValue(NANOSECONDS)).isEqualTo(nanos);
        assertThat(duration.getValue(MILLISECONDS)).isEqualTo(nanos / 1000000);
        assertThat(duration.getValue(SECONDS)).isEqualTo(nanos / 1000000 / 1000);
        assertThat(duration.getValue(MINUTES)).isCloseTo(nanos / 1000000 / 1000 / 60, offset(1.0E10));
        assertThat(duration.getValue(HOURS)).isCloseTo(nanos / 1000000 / 1000 / 60 / 60, offset(1.0E10));
        assertThat(duration.getValue(DAYS)).isCloseTo(nanos / 1000000 / 1000 / 60 / 60 / 24, offset(1.0E10));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void invalidParameters()
    {
        failDurationConstruction(Double.NEGATIVE_INFINITY, MILLISECONDS);
        failDurationConstruction(Double.POSITIVE_INFINITY, MILLISECONDS);
        failDurationConstruction(Double.NaN, MILLISECONDS);
        failDurationConstruction(42, null);
        failDurationConstruction(-42, MILLISECONDS);

        Duration duration = new Duration(42, MILLISECONDS);
        try {
            duration.convertTo(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) {
            // ok
        }
        try {
            duration.toString(null);
            fail("Expected NullPointerException");
        }
        catch (NullPointerException e) {
            // ok
        }
    }

    @Test
    public void testJsonRoundTrip()
            throws Exception
    {
        assertJsonRoundTrip(new Duration(1.234, MILLISECONDS));
        assertJsonRoundTrip(new Duration(1.234, SECONDS));
        assertJsonRoundTrip(new Duration(1.234, MINUTES));
        assertJsonRoundTrip(new Duration(1.234, HOURS));
        assertJsonRoundTrip(new Duration(1.234, DAYS));
    }

    private static void assertJsonRoundTrip(Duration duration)
    {
        JsonCodec<Duration> durationCodec = JsonCodec.jsonCodec(Duration.class);
        String json = durationCodec.toJson(duration);
        Duration durationCopy = durationCodec.fromJson(json);

        assertThat(durationCopy.getValue(MILLISECONDS))
                .isCloseTo(duration.getValue(MILLISECONDS), withPercentage(1));
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void failDurationConstruction(double value, TimeUnit timeUnit)
    {
        try {
            new Duration(value, timeUnit);
            fail("Expected NullPointerException or IllegalArgumentException");
        }
        catch (NullPointerException | IllegalArgumentException e) {
            // ok
        }
    }

    @DataProvider(name = "parseableValues", parallel = true)
    private Object[][] parseableValues()
    {
        return new Object[][] {
                // spaces
                new Object[] {"1234 ns", 1234, NANOSECONDS},
                new Object[] {"1234 ms", 1234, MILLISECONDS},
                new Object[] {"1234 s", 1234, SECONDS},
                new Object[] {"1234 m", 1234, MINUTES},
                new Object[] {"1234 h", 1234, HOURS},
                new Object[] {"1234 d", 1234, DAYS},
                new Object[] {"1234.567 ns", 1234.567, NANOSECONDS},
                new Object[] {"1234.567 ms", 1234.567, MILLISECONDS},
                new Object[] {"1234.567 s", 1234.567, SECONDS},
                new Object[] {"1234.567 m", 1234.567, MINUTES},
                new Object[] {"1234.567 h", 1234.567, HOURS},
                new Object[] {"1234.567 d", 1234.567, DAYS},
                // no spaces
                new Object[] {"1234ns", 1234, NANOSECONDS},
                new Object[] {"1234ms", 1234, MILLISECONDS},
                new Object[] {"1234s", 1234, SECONDS},
                new Object[] {"1234m", 1234, MINUTES},
                new Object[] {"1234h", 1234, HOURS},
                new Object[] {"1234d", 1234, DAYS},
                new Object[] {"1234.567ns", 1234.567, NANOSECONDS},
                new Object[] {"1234.567ms", 1234.567, MILLISECONDS},
                new Object[] {"1234.567s", 1234.567, SECONDS},
                new Object[] {"1234.567m", 1234.567, MINUTES},
                new Object[] {"1234.567h", 1234.567, HOURS},
                new Object[] {"1234.567d", 1234.567, DAYS}
        };
    }

    @DataProvider(name = "printedValues", parallel = true)
    private Object[][] printedValues()
    {
        return new Object[][] {
                new Object[] {"1234.00ns", 1234, NANOSECONDS},
                new Object[] {"1234.00us", 1234, MICROSECONDS},
                new Object[] {"1234.00ms", 1234, MILLISECONDS},
                new Object[] {"1234.00s", 1234, SECONDS},
                new Object[] {"1234.00m", 1234, MINUTES},
                new Object[] {"1234.00h", 1234, HOURS},
                new Object[] {"1234.00d", 1234, DAYS},
                new Object[] {"1234.57ns", 1234.567, NANOSECONDS},
                new Object[] {"1234.57us", 1234.567, MICROSECONDS},
                new Object[] {"1234.57ms", 1234.567, MILLISECONDS},
                new Object[] {"1234.57s", 1234.567, SECONDS},
                new Object[] {"1234.57m", 1234.567, MINUTES},
                new Object[] {"1234.57h", 1234.567, HOURS},
                new Object[] {"1234.57d", 1234.567, DAYS}
        };
    }

    @DataProvider(name = "conversions", parallel = true)
    private Object[][] conversions()
    {
        return new Object[][] {
                new Object[] {NANOSECONDS, NANOSECONDS, 1.0},
                new Object[] {NANOSECONDS, MILLISECONDS, 1.0 / 1000_000},
                new Object[] {NANOSECONDS, SECONDS, 1.0 / 1000_000 / 1000},
                new Object[] {NANOSECONDS, MINUTES, 1.0 / 1000_000 / 1000 / 60},
                new Object[] {NANOSECONDS, HOURS, 1.0 / 1000_000 / 1000 / 60 / 60},
                new Object[] {NANOSECONDS, DAYS, 1.0 / 1000_000 / 1000 / 60 / 60 / 24},

                new Object[] {MILLISECONDS, NANOSECONDS, 1000000.0},
                new Object[] {MILLISECONDS, MILLISECONDS, 1.0},
                new Object[] {MILLISECONDS, SECONDS, 1.0 / 1000},
                new Object[] {MILLISECONDS, MINUTES, 1.0 / 1000 / 60},
                new Object[] {MILLISECONDS, HOURS, 1.0 / 1000 / 60 / 60},
                new Object[] {MILLISECONDS, DAYS, 1.0 / 1000 / 60 / 60 / 24},

                new Object[] {SECONDS, NANOSECONDS, 1000000.0 * 1000},
                new Object[] {SECONDS, MILLISECONDS, 1000},
                new Object[] {SECONDS, SECONDS, 1},
                new Object[] {SECONDS, MINUTES, 1.0 / 60},
                new Object[] {SECONDS, HOURS, 1.0 / 60 / 60},
                new Object[] {SECONDS, DAYS, 1.0 / 60 / 60 / 24},

                new Object[] {MINUTES, NANOSECONDS, 1000000.0 * 1000 * 60},
                new Object[] {MINUTES, MILLISECONDS, 1000 * 60},
                new Object[] {MINUTES, SECONDS, 60},
                new Object[] {MINUTES, MINUTES, 1},
                new Object[] {MINUTES, HOURS, 1.0 / 60},
                new Object[] {MINUTES, DAYS, 1.0 / 60 / 24},

                new Object[] {HOURS, NANOSECONDS, 1000000.0 * 1000 * 60 * 60},
                new Object[] {HOURS, MILLISECONDS, 1000 * 60 * 60},
                new Object[] {HOURS, SECONDS, 60 * 60},
                new Object[] {HOURS, MINUTES, 60},
                new Object[] {HOURS, HOURS, 1},
                new Object[] {HOURS, DAYS, 1.0 / 24},

                new Object[] {DAYS, NANOSECONDS, 1000000.0 * 1000 * 60 * 60 * 24},
                new Object[] {DAYS, MILLISECONDS, 1000 * 60 * 60 * 24},
                new Object[] {DAYS, SECONDS, 60 * 60 * 24},
                new Object[] {DAYS, MINUTES, 60 * 24},
                new Object[] {DAYS, HOURS, 24},
                new Object[] {DAYS, DAYS, 1},
        };
    }
}
