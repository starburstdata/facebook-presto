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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Suppliers;

import java.util.Map;
import java.util.function.Supplier;

import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static java.util.Objects.requireNonNull;

public final class CalculatingStatsProvider
        extends StatsProvider
{
    private final StatsCalculator statsCalculator;
    private final Lookup lookup;
    private final Session session;
    private final Supplier<Map<Symbol, Type>> types;

    public CalculatingStatsProvider(StatsCalculator statsCalculator, Session session, Map<Symbol, Type> types)
    {
        this(statsCalculator, noLookup(), session, Suppliers.ofInstance(requireNonNull(types, "types is null")));
    }

    public CalculatingStatsProvider(StatsCalculator statsCalculator, Lookup lookup, Session session, Supplier<Map<Symbol, Type>> types)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.session = requireNonNull(session, "session is null");
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode node, StatsProvider wrapper)
    {
        requireNonNull(node, "node is null");

        return statsCalculator.calculateStats(node, wrapper, lookup, session, types.get());
    }
}
