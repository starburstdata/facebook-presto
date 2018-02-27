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

import java.util.OptionalDouble;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MetricComparison
{
    private final Metric metric;
    private final OptionalDouble estimatedValue;
    private final OptionalDouble actualValue;

    public MetricComparison(Metric metric, OptionalDouble estimatedValue, OptionalDouble actualValue)
    {
        this.metric = metric;
        this.estimatedValue = estimatedValue;
        this.actualValue = actualValue;
    }

    @Override
    public String toString()
    {
        return format("Metric [%s] - estimated: [%s], real: [%s]",
                metric, print(estimatedValue), print(actualValue));
    }

    public void matches(MetricAssertStrategy metricAssertStrategy)
    {
        requireNonNull(metricAssertStrategy, "metricComparisonStrategy is null");
        metricAssertStrategy.matches(actualValue, estimatedValue);
    }

    private String print(OptionalDouble value)
    {
        if (!value.isPresent()) {
            return "UNKNOWN";
        }
        return String.valueOf(value.getAsDouble());
    }
}
