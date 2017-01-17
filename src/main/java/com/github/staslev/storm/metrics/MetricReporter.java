package com.github.staslev.storm.metrics;

import com.google.common.collect.Lists;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A metric consumer implementation that reports storm metrics to Graphite.
 * The metrics to be reported are specified using a regular expression, so as to avoid burdening Graphite with
 * undesired metrics.
 * <br/>
 * This metric consumer also reports a capacity metric, computed for each taskId based on the number of executions
 * and per-execution latency reported by Storm internals.
 * <br/><br/><url>Inspired by <url>https://github.com/endgameinc/storm-metrics-statsd</url>
 */
public class MetricReporter implements IMetricsConsumer {

    public static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);

    private MetricMatcher allowedMetrics;
    private StormMetricProcessor stormMetricProcessor;

    private double value(final Object value) {
        return ((Number) value).doubleValue();
    }

    private Map<String, List<Metric>> toMetricsByComponent(Collection<DataPoint> dataPoints, TaskInfo taskInfo) {

        String component = Metric.cleanNameFragment(taskInfo.srcComponentId);
        List<Metric> metrics = dataPoints.stream().flatMap(p -> extractMetrics(p, component).stream()).collect(Collectors.toList());

        return Collections.singletonMap(component, metrics);
    }

    List<Metric> extractMetrics(final DataPoint dataPoint, final String component) {

        List<Metric> metrics = Lists.newArrayList();

        if (dataPoint.value instanceof Number) {
            metrics.add(new Metric(component, Metric.cleanNameFragment(dataPoint.name), value(dataPoint.value)));
        } else if (dataPoint.value instanceof Map) {
            @SuppressWarnings("rawtypes")
            final Map map = (Map) dataPoint.value;
            for (final Object subName : map.keySet()) {
                final Object subValue = map.get(subName);
                if (subValue instanceof Number) {
                    metrics.add(new Metric(component,
                            Metric.joinNameFragments(Metric.cleanNameFragment(dataPoint.name),
                                    Metric.cleanNameFragment(subName.toString())),
                            value(subValue)));
                } else if (subValue instanceof Map) {
                    metrics.addAll(extractMetrics(new DataPoint(Metric.joinNameFragments(dataPoint.name, subName.toString()), subValue),
                            component));
                }
            }
        } else if (dataPoint.value instanceof List) {
            @SuppressWarnings("rawtypes")
            List value = (List) dataPoint.value;
            if(!value.isEmpty()) {
                metrics.addAll(extractMetrics(new DataPoint(dataPoint.name, value.get(0)), component));
            }
        }

        return metrics;
    }

    @Override
    public void prepare(final Map stormConf,
                        final Object registrationArgument,
                        final TopologyContext context,
                        final IErrorReporter errorReporter) {

        @SuppressWarnings("unchecked")
        final MetricReporterConfig config = MetricReporterConfig.from((List<String>) registrationArgument);
        allowedMetrics = new MetricMatcher(config.getAllowedMetricNames());
        stormMetricProcessor = config.getStormMetricProcessor(stormConf);
    }

    @Override
    public void handleDataPoints(final TaskInfo taskInfo, final Collection<DataPoint> dataPoints) {
        LOG.info(String.format("Getting data points - %s  ### %s", taskInfoToString(taskInfo), dataPoints));
        Map<String, List<Metric>> component2metrics = toMetricsByComponent(dataPoints, taskInfo);
//        final List<Metric> capacityMetrics = CapacityCalculator.calculateCapacityMetrics(component2metrics,
//                taskInfo);
        LOG.debug("Parsed metrics " + component2metrics);
//        final Iterable<Metric> providedMetrics = Iterables.concat(component2metrics.values());
//        final Iterable<Metric> allMetrics = Iterables.concat(providedMetrics, capacityMetrics);

//        ImmutableList<Metric> metrics = FluentIterable.from(allMetrics).toList();
//        for (final Metric metric : metrics) {
//            stormMetricProcessor.process(metric, taskInfo);
//        }
    }

    @Override
    public void cleanup() {
    }

    public static String taskInfoToString(TaskInfo info) {
        final StringBuilder sb = new StringBuilder("TaskInfo{");
        sb.append("srcWorkerHost='").append(info.srcWorkerHost).append('\'');
        sb.append(", srcWorkerPort=").append(info.srcWorkerPort);
        sb.append(", srcComponentId='").append(info.srcComponentId).append('\'');
        sb.append(", srcTaskId=").append(info.srcTaskId);
        sb.append(", timestamp=").append(info.timestamp);
        sb.append(", updateIntervalSecs=").append(info.updateIntervalSecs);
        sb.append('}');
        return sb.toString();
    }

}