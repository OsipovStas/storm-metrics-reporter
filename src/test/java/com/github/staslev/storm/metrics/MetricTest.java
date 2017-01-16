package com.github.staslev.storm.metrics;

import org.junit.Assert;
import org.junit.Test;

public class MetricTest {
    @Test
    public void cleanNameFragment() throws Exception {
        Assert.assertEquals("fsfd_fdsf..dsf_fds_f", Metric.cleanNameFragment("fsfd___fdsf//dsf:fds:f")
        );
    }

}