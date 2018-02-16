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

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public final class MemoStatsProvider
        extends StatsProvider
{
    private final StatsProvider delegate;
    private final Memo memo;

    public MemoStatsProvider(StatsProvider delegate, Memo memo)
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

        return delegate.getStats(node, wrapper);
    }

    private PlanNodeStatsEstimate getGroupStats(GroupReference groupReference, StatsProvider wrapper)
    {
        int group = groupReference.getGroupId();

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
