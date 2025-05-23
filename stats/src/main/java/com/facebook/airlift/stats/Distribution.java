package com.facebook.airlift.stats;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.weakref.jmx.Managed;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

@ThreadSafe
public class Distribution
{
    private static final double MAX_ERROR = 0.01;

    @GuardedBy("this")
    private final QuantileDigest digest;

    private final DecayCounter total;

    public Distribution()
    {
        digest = new QuantileDigest(MAX_ERROR);
        total = new DecayCounter(0);
    }

    public Distribution(double alpha)
    {
        digest = new QuantileDigest(MAX_ERROR, alpha);
        total = new DecayCounter(alpha);
    }

    public Distribution(Distribution distribution)
    {
        synchronized (distribution) {
            digest = new QuantileDigest(distribution.digest);
        }
        total = new DecayCounter(distribution.total.getAlpha());
        total.merge(distribution.total);
    }

    public synchronized void add(long value)
    {
        digest.add(value);
        total.add(value);
    }

    public synchronized void add(long value, long count)
    {
        digest.add(value, count);
        total.add(value * count);
    }

    @Managed
    public synchronized double getMaxError()
    {
        return digest.getConfidenceFactor();
    }

    @Managed
    public synchronized double getCount()
    {
        return digest.getCount();
    }

    @Managed
    public synchronized double getTotal()
    {
        return total.getCount();
    }

    @Managed
    public synchronized long getP01()
    {
        return digest.getQuantile(0.01);
    }

    @Managed
    public synchronized long getP05()
    {
        return digest.getQuantile(0.05);
    }

    @Managed
    public synchronized long getP10()
    {
        return digest.getQuantile(0.10);
    }

    @Managed
    public synchronized long getP25()
    {
        return digest.getQuantile(0.25);
    }

    @Managed
    public synchronized long getP50()
    {
        return digest.getQuantile(0.5);
    }

    @Managed
    public synchronized long getP75()
    {
        return digest.getQuantile(0.75);
    }

    @Managed
    public synchronized long getP90()
    {
        return digest.getQuantile(0.90);
    }

    @Managed
    public synchronized long getP95()
    {
        return digest.getQuantile(0.95);
    }

    @Managed
    public synchronized long getP99()
    {
        return digest.getQuantile(0.99);
    }

    @Managed
    public synchronized long getMin()
    {
        return digest.getMin();
    }

    @Managed
    public synchronized long getMax()
    {
        return digest.getMax();
    }

    @Managed
    public synchronized double getAvg()
    {
        return getTotal() / getCount();
    }

    @Managed
    public Map<Double, Long> getPercentiles()
    {
        List<Double> percentiles = new ArrayList<>(100);
        for (int i = 0; i < 100; ++i) {
            percentiles.add(i / 100.0);
        }

        List<Long> values;
        synchronized (this) {
            values = digest.getQuantiles(percentiles);
        }

        Map<Double, Long> result = new LinkedHashMap<>(values.size());
        for (int i = 0; i < percentiles.size(); ++i) {
            result.put(percentiles.get(i), values.get(i));
        }

        return result;
    }

    public synchronized List<Long> getPercentiles(List<Double> percentiles)
    {
        return digest.getQuantiles(percentiles);
    }

    public synchronized DistributionSnapshot snapshot()
    {
        List<Long> quantiles = digest.getQuantiles(ImmutableList.of(0.01, 0.05, 0.10, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99));
        return new DistributionSnapshot(
                getMaxError(),
                getCount(),
                getTotal(),
                quantiles.get(0),
                quantiles.get(1),
                quantiles.get(2),
                quantiles.get(3),
                quantiles.get(4),
                quantiles.get(5),
                quantiles.get(6),
                quantiles.get(7),
                quantiles.get(8),
                getMin(),
                getMax(),
                getAvg());
    }

    @ThriftStruct
    public static class DistributionSnapshot
    {
        private final double maxError;
        private final double count;
        private final double total;
        private final long p01;
        private final long p05;
        private final long p10;
        private final long p25;
        private final long p50;
        private final long p75;
        private final long p90;
        private final long p95;
        private final long p99;
        private final long min;
        private final long max;
        private final double avg;

        @JsonCreator
        @ThriftConstructor
        public DistributionSnapshot(
                @JsonProperty("maxError") double maxError,
                @JsonProperty("count") double count,
                @JsonProperty("total") double total,
                @JsonProperty("p01") long p01,
                @JsonProperty("p05") long p05,
                @JsonProperty("p10") long p10,
                @JsonProperty("p25") long p25,
                @JsonProperty("p50") long p50,
                @JsonProperty("p75") long p75,
                @JsonProperty("p90") long p90,
                @JsonProperty("p95") long p95,
                @JsonProperty("p99") long p99,
                @JsonProperty("min") long min,
                @JsonProperty("max") long max,
                @JsonProperty("avg") double avg)
        {
            this.maxError = maxError;
            this.count = count;
            this.total = total;
            this.p01 = p01;
            this.p05 = p05;
            this.p10 = p10;
            this.p25 = p25;
            this.p50 = p50;
            this.p75 = p75;
            this.p90 = p90;
            this.p95 = p95;
            this.p99 = p99;
            this.min = min;
            this.max = max;
            this.avg = avg;
        }

        @JsonProperty
        @ThriftField(1)
        public double getMaxError()
        {
            return maxError;
        }

        @JsonProperty
        @ThriftField(2)
        public double getCount()
        {
            return count;
        }

        @JsonProperty
        @ThriftField(3)
        public double getTotal()
        {
            return total;
        }

        @JsonProperty
        @ThriftField(4)
        public long getP01()
        {
            return p01;
        }

        @JsonProperty
        @ThriftField(5)
        public long getP05()
        {
            return p05;
        }

        @JsonProperty
        @ThriftField(6)
        public long getP10()
        {
            return p10;
        }

        @JsonProperty
        @ThriftField(7)
        public long getP25()
        {
            return p25;
        }

        @JsonProperty
        @ThriftField(8)
        public long getP50()
        {
            return p50;
        }

        @JsonProperty
        @ThriftField(9)
        public long getP75()
        {
            return p75;
        }

        @JsonProperty
        @ThriftField(10)
        public long getP90()
        {
            return p90;
        }

        @JsonProperty
        @ThriftField(11)
        public long getP95()
        {
            return p95;
        }

        @JsonProperty
        @ThriftField(12)
        public long getP99()
        {
            return p99;
        }

        @JsonProperty
        @ThriftField(13)
        public long getMin()
        {
            return min;
        }

        @JsonProperty
        @ThriftField(14)
        public long getMax()
        {
            return max;
        }

        @JsonProperty
        @ThriftField(15)
        public double getAvg()
        {
            return avg;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("maxError", maxError)
                    .add("count", count)
                    .add("total", total)
                    .add("p01", p01)
                    .add("p05", p05)
                    .add("p10", p10)
                    .add("p25", p25)
                    .add("p50", p50)
                    .add("p75", p75)
                    .add("p90", p90)
                    .add("p95", p95)
                    .add("p99", p99)
                    .add("min", min)
                    .add("max", max)
                    .add("avg", avg)
                    .toString();
        }
    }
}
