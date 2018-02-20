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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.cost.PlanNodeCostEstimate.INFINITE_COST;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.COST_BASED;
import static com.facebook.presto.sql.planner.EqualityInference.createEqualityInference;
import static com.facebook.presto.sql.planner.iterative.rule.MultiJoinNode.toMultiJoinNode;
import static com.facebook.presto.sql.planner.iterative.rule.PlanEnumeration.INFINITE_COST_RESULT;
import static com.facebook.presto.sql.planner.iterative.rule.PlanEnumeration.UNKNOWN_COST_RESULT;
import static com.facebook.presto.sql.planner.plan.Assignments.identity;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);
    private static final Pattern<JoinNode> PATTERN = Pattern.typeOf(JoinNode.class);
    private static final int JOIN_LIMIT = 10;

    private final CostComparator costComparator;

    public ReorderJoins(CostComparator costComparator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinReorderingStrategy(session) == COST_BASED;
    }

    @Override
    public Rule.Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        // We check that join distribution type is absent because we only want to do this transformation once (reordered joins will have distribution type already set).
        if (!(joinNode.getType() == INNER) || joinNode.getDistributionType().isPresent()) {
            return Rule.Result.empty();
        }

        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), JOIN_LIMIT);
        if (multiJoinNode.getSources().size() < 2) {
            // Possible when e.g. joinNode has non-deterministic filter. Reordering is generally not allowed in this case.
            return Rule.Result.empty();
        }

        JoinEnumerator joinEnumerator = new JoinEnumerator(
                context.getCostProvider(),
                costComparator,
                context.getIdAllocator(),
                context.getSession(),
                multiJoinNode.getFilter());
        PlanEnumeration.Result result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols());
        if (!result.getPlanNode().isPresent()) {
            return Rule.Result.empty();
        }
        if (result.getCost().hasUnknownComponents() || result.getCost().equals(INFINITE_COST)) {
            return Rule.Result.empty();
        }
        return Rule.Result.ofPlanNode(result.getPlanNode().get());
    }

    @VisibleForTesting
    static class JoinEnumerator
    {
        private final CostProvider costProvider;
        private final CostComparator costComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final EqualityInference allInference;
        private final Expression allFilter;

        private final Map<Set<PlanNode>, PlanEnumeration.Result> memo = new HashMap<>();

        @VisibleForTesting
        JoinEnumerator(CostProvider costProvider, CostComparator costComparator, PlanNodeIdAllocator idAllocator, Session session, Expression filter)
        {
            this.costProvider = requireNonNull(costProvider, "costProvider is null");
            this.costComparator = requireNonNull(costComparator, "costComparator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.session = requireNonNull(session, "session is null");
            this.allInference = createEqualityInference(filter);
            this.allFilter = requireNonNull(filter, "filter is null");
        }

        private PlanEnumeration.Result chooseJoinOrder(List<PlanNode> sources, List<Symbol> outputSymbols)
        {
            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            PlanEnumeration.Result bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                PlanEnumeration planEnumeration = getPlanEnumeration();
                Set<Set<Integer>> partitions = generatePartitions(sources.size()).collect(toImmutableSet());
                for (Set<Integer> partition : partitions) {
                    PlanEnumeration.Result result = createJoinAccordingToPartitioning(sources, outputSymbols, partition);
                    if (result.getCost().hasUnknownComponents()) {
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.getCost().equals(INFINITE_COST)) {
                        planEnumeration.enumerate(result);
                    }
                }

                bestResult = planEnumeration.getResult();
                if (bestResult.getCost().equals(INFINITE_COST)) {
                    return INFINITE_COST_RESULT;
                }
                memo.put(multiJoinKey, bestResult);
            }
            if (bestResult.getPlanNode().isPresent()) {
                log.debug("Least cost join was: " + bestResult.getPlanNode().get().toString());
            }
            return bestResult;
        }

        /**
         * This method generates all the ways of dividing totalNodes into two sets
         * each containing at least one node. It will generate one set for each
         * possible partitioning. The other partition is implied in the absent values.
         * In order not to generate the inverse of any set, we always include the 0th
         * node in our sets.
         *
         * @param totalNodes
         * @return A set of sets each of which defines a partitioning of totalNodes
         */
        @VisibleForTesting
        static Stream<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes >= 2, "totalNodes must be greater than or equal to 2");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return Sets.powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size());
        }

        PlanEnumeration.Result createJoinAccordingToPartitioning(List<PlanNode> sources, List<Symbol> outputSymbols, Set<Integer> partitioning)
        {
            Set<PlanNode> leftSources = partitioning.stream()
                    .map(sources::get)
                    .collect(toImmutableSet());
            Set<PlanNode> rightSources = Sets.difference(ImmutableSet.copyOf(sources), ImmutableSet.copyOf(leftSources));
            return createJoin(leftSources, rightSources, outputSymbols);
        }

        private PlanEnumeration.Result createJoin(Set<PlanNode> leftSources, Set<PlanNode> rightSources, List<Symbol> outputSymbols)
        {
            Set<Symbol> leftSymbols = leftSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            Set<Symbol> rightSymbols = rightSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            ImmutableList.Builder<Expression> joinPredicatesBuilder = ImmutableList.builder();

            // add join conjuncts that were not used for inference
            stream(EqualityInference.nonInferrableConjuncts(allFilter))
                    .map(conjunct -> allInference.rewriteExpression(conjunct, symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right symbols
                    .filter(conjuct -> allInference.rewriteExpression(conjuct, leftSymbols::contains) == null)
                    .filter(conjuct -> allInference.rewriteExpression(conjuct, rightSymbols::contains) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available symbols
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<Expression> joinEqualities = allInference.generateEqualitiesPartitionedBy(symbol -> leftSymbols.contains(symbol) || rightSymbols.contains(symbol)).getScopeEqualities();
            EqualityInference joinInference = createEqualityInference(joinEqualities.toArray(new Expression[joinEqualities.size()]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftSymbols)).getScopeStraddlingEqualities());

            List<Expression> joinPredicates = joinPredicatesBuilder.build();
            List<JoinNode.EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(JoinEnumerator::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause((ComparisonExpression) predicate, leftSymbols))
                    .collect(toImmutableList());
            if (joinConditions.isEmpty()) {
                return INFINITE_COST_RESULT;
            }
            List<Expression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(outputSymbols)
                    .addAll(SymbolsExtractor.extractUnique(joinPredicates))
                    .build();

            PlanEnumeration.Result leftResult = getJoinSource(
                    ImmutableList.copyOf(leftSources),
                    requiredJoinSymbols.stream().filter(leftSymbols::contains).collect(toImmutableList()));
            if (leftResult.getCost().hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.getCost().equals(INFINITE_COST)) {
                return INFINITE_COST_RESULT;
            }
            PlanNode left = leftResult.getPlanNode().orElseThrow(() -> new IllegalStateException("no planNode present"));
            PlanEnumeration.Result rightResult = getJoinSource(
                    ImmutableList.copyOf(rightSources),
                    requiredJoinSymbols.stream()
                            .filter(rightSymbols::contains)
                            .collect(toImmutableList()));
            if (rightResult.getCost().hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.getCost().equals(INFINITE_COST)) {
                return INFINITE_COST_RESULT;
            }
            PlanNode right = rightResult.getPlanNode().orElseThrow(() -> new IllegalStateException("no planNode present"));

            // sort output symbols so that the left input symbols are first
            List<Symbol> sortedOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            // Cross joins can't filter symbols as part of the join
            // If we're doing a cross join, use all output symbols from the inputs and add a project node
            // on top
            List<Symbol> joinOutputSymbols = sortedOutputSymbols;
            if (joinConditions.isEmpty() && joinFilters.isEmpty()) {
                joinOutputSymbols = Stream.concat(left.getOutputSymbols().stream(), right.getOutputSymbols().stream())
                        .collect(toImmutableList());
            }

            PlanEnumeration.Result result = setJoinNodeProperties(new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinConditions,
                    joinOutputSymbols,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));

            if (!joinOutputSymbols.equals(sortedOutputSymbols)) {
                PlanNode resultNode = new ProjectNode(idAllocator.getNextId(), result.getPlanNode().get(), identity(sortedOutputSymbols));
                result = getPlanEnumeration().enumerate(resultNode).getResult();
            }

            return result;
        }

        private PlanEnumeration.Result getJoinSource(List<PlanNode> nodes, List<Symbol> outputSymbols)
        {
            PlanNode planNode;
            if (nodes.size() == 1) {
                planNode = getOnlyElement(nodes);
                ImmutableList.Builder<Expression> predicates = ImmutableList.builder();
                predicates.addAll(allInference.generateEqualitiesPartitionedBy(outputSymbols::contains).getScopeEqualities());
                stream(EqualityInference.nonInferrableConjuncts(allFilter))
                        .map(conjuct -> allInference.rewriteExpression(conjuct, outputSymbols::contains))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                Expression filter = combineConjuncts(predicates.build());
                if (!(TRUE_LITERAL).equals(filter)) {
                    planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return getPlanEnumeration().enumerate(planNode).getResult();
            }
            return chooseJoinOrder(nodes, outputSymbols);
        }

        private static boolean isJoinEqualityCondition(Expression expression)
        {
            return expression instanceof ComparisonExpression
                    && ((ComparisonExpression) expression).getType() == EQUAL
                    && ((ComparisonExpression) expression).getLeft() instanceof SymbolReference
                    && ((ComparisonExpression) expression).getRight() instanceof SymbolReference;
        }

        private static JoinNode.EquiJoinClause toEquiJoinClause(ComparisonExpression equality, Set<Symbol> leftSymbols)
        {
            Symbol leftSymbol = Symbol.from(equality.getLeft());
            Symbol rightSymbol = Symbol.from(equality.getRight());
            JoinNode.EquiJoinClause equiJoinClause = new JoinNode.EquiJoinClause(leftSymbol, rightSymbol);
            return leftSymbols.contains(leftSymbol) ? equiJoinClause : equiJoinClause.flip();
        }

        private PlanEnumeration.Result setJoinNodeProperties(JoinNode joinNode)
        {
            // TODO avoid stat (but not cost) recalculation for all considered (distribution,flip) pairs, since resulting relation is the same in all case

            PlanEnumeration planEnumeration = getPlanEnumeration();
            FeaturesConfig.JoinDistributionType joinDistributionType = getJoinDistributionType(session);
            if (joinDistributionType.canRepartition() && !joinNode.isCrossJoin()) {
                JoinNode node = joinNode.withDistributionType(PARTITIONED);
                planEnumeration.enumerate(node);
                planEnumeration.enumerate(node.flipChildren());
            }
            if (joinDistributionType.canReplicate()) {
                JoinNode node = joinNode.withDistributionType(REPLICATED);
                planEnumeration.enumerate(node);
                planEnumeration.enumerate(node.flipChildren());
            }
            return planEnumeration.getResult();
        }

        private PlanEnumeration getPlanEnumeration()
        {
            return new PlanEnumeration(costProvider, costComparator, session);
        }
    }
}
