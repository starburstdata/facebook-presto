/*
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

package com.facebook.presto.tests.statistics;

import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.tests.statistics.MetricComparator.getMetricComparisons;
import static com.facebook.presto.tests.statistics.MetricAssertStrategies.noError;
import static com.facebook.presto.tests.statistics.Metrics.distinctValuesCount;
import static com.facebook.presto.tests.statistics.Metrics.highValue;
import static com.facebook.presto.tests.statistics.Metrics.lowValue;
import static com.facebook.presto.tests.statistics.Metrics.nullsFraction;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StatisticsAssertion
        implements AutoCloseable
{
    private final QueryRunner runner;

    public StatisticsAssertion(QueryRunner runner)
    {
        this.runner = requireNonNull(runner, "runner is null");
    }

    @Override
    public void close()
    {
        runner.close();
    }

    public void check(@Language("SQL") String query, Consumer<Checks> checksBuilderConsumer)
    {
        Checks checks = new Checks();
        checksBuilderConsumer.accept(checks);
        checks.run(query, runner);
    }

    private static class MetricsCheck
    {
        public final Metric metric;
        public final MetricAssertStrategy strategy;

        MetricsCheck(Metric metric, MetricAssertStrategy strategy)
        {
            this.metric = metric;
            this.strategy = strategy;
        }
    }

    public static class Checks
    {
        private final List<MetricsCheck> checks = new ArrayList<>();

        public Checks verifyExactColumnStatistics(String columnName)
        {
            verifyColumnStatistics(columnName, noError());
            return this;
        }

        public Checks verifyColumnStatistics(String columnName, MetricAssertStrategy strategy)
        {
            estimate(nullsFraction(columnName), strategy);
            estimate(distinctValuesCount(columnName), strategy);
            estimate(lowValue(columnName), strategy);
            estimate(highValue(columnName), strategy);
            return this;
        }

        public Checks verifyCharacterColumnStatistics(String columnName, MetricAssertStrategy strategy)
        {
            estimate(nullsFraction(columnName), strategy);
            estimate(distinctValuesCount(columnName), strategy);
            // currently we do not support low/high values for char/varchar in stats calculations
            return this;
        }

        public Checks verifyNoColumnStatistics(String columnName)
        {
            noEstimate(nullsFraction(columnName));
            noEstimate(distinctValuesCount(columnName));
            noEstimate(lowValue(columnName));
            noEstimate(highValue(columnName));
            return this;
        }

        public Checks estimate(Metric metric, MetricAssertStrategy strategy)
        {
            checks.add(new MetricsCheck(metric, strategy));
            return this;
        }

        public Checks noEstimate(Metric metric)
        {
            checks.add(new MetricsCheck(metric, MetricAssertStrategies.noEstimate()));
            return this;
        }

        public Checks noBaseline(Metric metric)
        {
            checks.add(new MetricsCheck(metric, MetricAssertStrategies.noBaseline()));
            return this;
        }

        public Checks noBaselineAndNoEstimate(Metric metric)
        {
            checks.add(new MetricsCheck(metric, MetricAssertStrategies.noBaselineAndNoEstimate()));
            return this;
        }

        void run(@Language("SQL") String query, QueryRunner runner)
        {
            List<Metric> metrics = checks.stream()
                    .map(check -> check.metric)
                    .collect(toImmutableList());
            List<MetricComparison> metricComparisons = getMetricComparisons(query, runner, metrics);
            verify(checks.size() == metricComparisons.size());
            for (int i = 0; i < checks.size(); i++) {
                MetricsCheck check = checks.get(i);
                MetricComparison metricComparison = metricComparisons.get(i);
                metricComparison.matches(check.strategy);
            }
        }
    }
}
