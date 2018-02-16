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

import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static java.util.Objects.requireNonNull;

final class SettableStatsProvider
        extends StatsProvider
{
    private Map<PlanNode, PlanNodeStatsEstimate> stats = new HashMap<>();

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode node, StatsProvider wrapper)
    {
        return stats.getOrDefault(node, UNKNOWN_STATS);
    }

    public void put(PlanNode planNode, PlanNodeStatsEstimate planNodeStats)
    {
        requireNonNull(planNode, "planNode is null");
        requireNonNull(planNodeStats, "planNodeStats is null");
        stats.put(planNode, planNodeStats);
    }
}
