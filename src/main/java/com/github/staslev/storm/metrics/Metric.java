package com.github.staslev.storm.metrics;

import com.google.common.base.Joiner;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Metric {

    private static final String NAME_FRAGMENT_SEPARATOR = ".";

    private final String component;
    private final String operation;
    private final double value;

    public Metric(final String component, final String operation, final double value) {
        this.component = component;
        this.operation = operation;
        this.value = value;
    }

    /**
     * Returns the operation portion of the metric after a given string literal.
     *
     * @param after the string literal to skip.
     * @return the operation portion of the metric after a given string literal. If the provided string literal is not
     * present in the operation string, the operation string is returned as is.
     */
    public String getOperationAfterString(String after) {
        return !getOperation().contains(after) ?
                getOperation() :
                getOperation().substring(getOperation().indexOf(after) + after.length() + 1);
    }

    /**
     * Joins multiple metric name strings using a Graphite style dot separator.
     *
     * @param metricNameFragments The metric metricNameFragments.
     * @return A joined metric name string.
     */
    public static String joinNameFragments(String... metricNameFragments) {

        final List<String> nonBlankNameFragments =
                Arrays.stream(metricNameFragments)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList());

        return Joiner.on(NAME_FRAGMENT_SEPARATOR).join(nonBlankNameFragments);
    }

    /**
     * Removes restricted characters from a metric name fragment.
     *
     * @param metricNameFragment A metric name fragment string.
     * @return A "clean" metric name fragment string, with restricted characters replaced.
     */
    static String cleanNameFragment(final String metricNameFragment) {
        return metricNameFragment
                .replace("__", "")
                .replace('/', '.')
                .replace(':', '_');
    }

    String getMetricName() {
        return joinNameFragments(getComponent(), getOperation());
    }

    public double getValue() {
        return value;
    }

    public String getComponent() {
        return component;
    }

    public String getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "component='" + component + '\'' +
                ", operation='" + operation + '\'' +
                ", value=" + value +
                '}';
    }
}
