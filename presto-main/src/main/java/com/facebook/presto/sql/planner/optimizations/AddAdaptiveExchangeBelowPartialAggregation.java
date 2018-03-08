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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.ADAPTIVE_HASH_PASSTHROUGH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.adaptiveParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.deriveProperties;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.groupingKeys;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.step;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AddAdaptiveExchangeBelowPartialAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(AggregationNode.Step.PARTIAL))
            .with(Pattern.nonEmpty(groupingKeys()));

    private final Metadata metadata;
    private final SqlParser parser;

    public AddAdaptiveExchangeBelowPartialAggregation(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        StreamProperties childProperties = derivePropertiesRecursively(node.getSource(), context);
        StreamPreferredProperties requiredProperties = adaptiveParallelism().withPartitioning(node.getGroupingKeys());

        if (requiredProperties.isSatisfiedBy(childProperties)) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                node.replaceChildren(ImmutableList.of(
                        partitionedExchange(
                                context.getIdAllocator().getNextId(),
                                LOCAL,
                                node.getSource(),
                                new PartitioningScheme(
                                        Partitioning.create(ADAPTIVE_HASH_PASSTHROUGH_DISTRIBUTION, node.getGroupingKeys()),
                                        node.getSource().getOutputSymbols(),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())))));
    }

    private StreamProperties derivePropertiesRecursively(PlanNode node, Context context)
    {
        PlanNode resolvedPlanNode = context.getLookup().resolve(node);
        List<StreamProperties> inputProperties = resolvedPlanNode.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, context))
                .collect(toImmutableList());
        return deriveProperties(resolvedPlanNode, inputProperties, metadata, context.getSession(), context.getSymbolAllocator().getTypes(), parser);
    }
}
