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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public final class MetricAssertStrategies
{
    private MetricAssertStrategies() {}

    public static MetricAssertStrategy noError()
    {
        return absoluteError(0);
    }

    public static MetricAssertStrategy absoluteError(double error)
    {
        return absoluteError(-error, error);
    }

    public static MetricAssertStrategy absoluteError(double minError, double maxError)
    {
        checkArgument(minError <= maxError, "minError '%s' has to be lower or equal than maxError '%s'", minError, maxError);
        return (actual, estimate) -> {
            checkArgument(actual.isPresent() && estimate.isPresent(), "Expected actual and estimate to be provided");
            double minEstimateValue = actual.getAsDouble() + minError;
            double maxEstimateValue = actual.getAsDouble() + maxError;
            assertTrue(
                    estimate.getAsDouble() >= minEstimateValue && estimate.getAsDouble() <= maxEstimateValue,
                    format("Expected estimate '%f' to be between '%f' and '%f, actual is '%f'", estimate.getAsDouble(), minEstimateValue, maxEstimateValue, actual.getAsDouble()));
        };
    }

    public static MetricAssertStrategy defaultTolerance()
    {
        return relativeError(.1);
    }

    public static MetricAssertStrategy relativeError(double error)
    {
        return relativeError(-error, error);
    }

    public static MetricAssertStrategy relativeError(double minError, double maxError)
    {
        checkArgument(minError <= maxError, "minError '%s' has to be lower or equal than maxError '%s'", minError, maxError);
        return (actual, estimate) -> {
            checkArgument(actual.isPresent() && estimate.isPresent(), "Expected actual and estimate to be provided");
            double minEstimateValue = actual.getAsDouble() * (minError + 1);
            double maxEstimateValue = actual.getAsDouble() * (maxError + 1);
            assertTrue(
                    estimate.getAsDouble() >= minEstimateValue && estimate.getAsDouble() <= maxEstimateValue,
                    format("Expected estimate '%d' to be between '%d' and '%d, actual is '%d'", estimate.getAsDouble(), minEstimateValue, maxEstimateValue, actual.getAsDouble()));
        };
    }

    public static MetricAssertStrategy noEstimate()
    {
        return (actual, estimate) -> {
            assertTrue(!estimate.isPresent(), format("Expected no estimate but got '%d'", estimate.getAsDouble()));
            assertTrue(actual.isPresent(), "Expected estimate but it was not provided");
        };
    }

    public static MetricAssertStrategy noBaseline()
    {
        return (actual, estimate) -> {
            assertTrue(!actual.isPresent(), format("Expected no baseline but got '%d'", actual.getAsDouble()));
            assertTrue(estimate.isPresent(), "Expected estimate but it was not provided");
        };
    }

    public static MetricAssertStrategy noBaselineAndNoEstimate()
    {
        return (actual, estimate) -> {
            assertTrue(estimate.isPresent(), "Expected estimate but it was not provided");
            assertTrue(actual.isPresent(), "Expected estimate but it was not provided");
        };
    }
}
