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

import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public final class CachingStatsProvider
        extends StatsProvider
{
    private final StatsProvider delegate;
    private final Optional<Memo> memo;

    private final Map<PlanNode, PlanNodeStatsEstimate> cache = new IdentityHashMap<>();

    public CachingStatsProvider(StatsProvider delegate)
    {
        this(delegate, Optional.empty());
    }

    public CachingStatsProvider(StatsProvider delegate, Optional<Memo> memo)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.memo = requireNonNull(memo, "memo is null");
    }

    @Override
    public PlanNodeStatsEstimate getStats(PlanNode node, StatsProvider wrapper)
    {
        requireNonNull(node, "node is null");

        if (node instanceof GroupReference) {
            return getGroupStats((GroupReference) node, wrapper);
        }

        PlanNodeStatsEstimate stats = cache.get(node);
        if (stats != null) {
            return stats;
        }

        stats = delegate.getStats(node, wrapper);
        verify(cache.put(node, stats) == null, "Stats already set");
        return stats;
    }

    private PlanNodeStatsEstimate getGroupStats(GroupReference groupReference, StatsProvider wrapper)
    {
        int group = groupReference.getGroupId();
        Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("CachingStatsProvider without memo cannot handle GroupReferences"));

        Optional<PlanNodeStatsEstimate> stats = memo.getStats(group);
        if (stats.isPresent()) {
            return stats.get();
        }

        PlanNodeStatsEstimate groupStats = delegate.getStats(memo.resolve(groupReference), wrapper);
        verify(!memo.getStats(group).isPresent(), "Group stats already set");
        memo.storeStats(group, groupStats);
        return groupStats;
    }
}
