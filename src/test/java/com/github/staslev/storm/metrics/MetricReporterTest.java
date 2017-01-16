package com.github.staslev.storm.metrics;

import org.apache.storm.metric.api.IMetricsConsumer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MetricReporterTest {


    @Test
    public void extractMetrics() throws Exception {
        MetricReporter reporter = new MetricReporter();
        List<Metric> test = reporter.extractMetrics(new IMetricsConsumer.DataPoint("__emit-count.stream_message", Arrays.asList(2000, 2160, 2060, 2180, 1960, 2060)), "Test");
        assertEquals("[Metric{component='Test', operation='emit-count.stream_message', value=2000.0}]", test.toString());
    }

    @Test
    public void thatCanExtractCustomMetric() throws Exception {
        MetricReporter reporter = new MetricReporter();
        List<Metric> test = reporter.extractMetrics(new IMetricsConsumer.DataPoint("__emit-count.stream_message", Arrays.asList(2000, 2160, 2060, 2180, 1960, 2060)), "Test");
        assertEquals("[Metric{component='Test', operation='emit-count.stream_message', value=2000.0}]", test.toString());
    }

}