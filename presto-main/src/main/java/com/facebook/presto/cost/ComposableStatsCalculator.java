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
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.pattern.TypeOfPattern;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Multimaps.toMultimap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ComposableStatsCalculator
        implements StatsCalculator
{
    private final ListMultimap<Class<?>, Rule> rulesByRootType;
    private final StatsNormalizer normalizer;

    public ComposableStatsCalculator(List<Rule> rules, StatsNormalizer normalizer)
    {
        this.rulesByRootType = rules.stream()
                .peek(rule -> {
                    checkArgument(rule.getPattern() instanceof TypeOfPattern, "Rule pattern must be TypeOfPattern");
                    Class<?> expectedClass = ((TypeOfPattern<?>) rule.getPattern()).expectedClass();
                    checkArgument(!expectedClass.isInterface() && !Modifier.isAbstract(expectedClass.getModifiers()), "Rule must be registered on a concrete class");
                })
                .collect(toMultimap(
                        rule -> ((TypeOfPattern<?>) rule.getPattern()).expectedClass(),
                        rule -> rule,
                        ArrayListMultimap::create));
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
    }

    private Stream<Rule> getCandidates(PlanNode node)
    {
        for (Class<?> superclass = node.getClass().getSuperclass(); superclass != null; superclass = superclass.getSuperclass()) {
            // This is important because rule ordering, given in the constructor, is significant.
            // We can't check this fully in the constructor, since abstract class may lack `abstract` modifier.
            checkState(rulesByRootType.get(superclass).isEmpty(), "Cannot maintain rule order because there is rule registered for %s", superclass);
        }
        return rulesByRootType.get(node.getClass()).stream();
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(sourceStats, lookup, session, types);
        return node.accept(visitor, null);
    }

    @VisibleForTesting
    static void checkConsistent(StatsNormalizer normalizer, String source, PlanNodeStatsEstimate stats, Collection<Symbol> outputSymbols, Map<Symbol, Type> types)
    {
        PlanNodeStatsEstimate normalized = normalizer.normalize(stats, outputSymbols, types);
        if (Objects.equals(stats, normalized)) {
            return;
        }

        List<String> problems = new ArrayList<>();

        if (Double.compare(stats.getOutputRowCount(), normalized.getOutputRowCount()) != 0) {
            problems.add(format(
                    "Output row count is %s, should be normalized to %s",
                    stats.getOutputRowCount(),
                    normalized.getOutputRowCount()));
        }

        for (Symbol symbol : stats.getSymbolsWithKnownStatistics()) {
            if (!Objects.equals(stats.getSymbolStatistics(symbol), normalized.getSymbolStatistics(symbol))) {
                problems.add(format(
                        "Symbol stats for '%s' are \n\t\t\t\t\t%s, should be normalized to \n\t\t\t\t\t%s",
                        symbol,
                        stats.getSymbolStatistics(symbol),
                        normalized.getSymbolStatistics(symbol)));
            }
        }

        if (problems.isEmpty()) {
            problems.add(stats.toString());
        }
        throw new IllegalStateException(format(
                "Rule %s returned inconsistent stats: %s",
                source,
                problems.stream().collect(joining("\n\t\t\t", "\n\t\t\t", ""))));
    }

    public interface Rule
    {
        Pattern<? extends PlanNode> getPattern();

        Optional<PlanNodeStatsEstimate> calculate(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, Map<Symbol, Type> types);
    }

    private class Visitor
            extends PlanVisitor<PlanNodeStatsEstimate, Void>
    {
        private final StatsProvider sourceStats;
        private final Lookup lookup;
        private final Session session;
        private final Map<Symbol, Type> types;

        public Visitor(StatsProvider sourceStats, Lookup lookup, Session session, Map<Symbol, Type> types)
        {
            this.sourceStats = requireNonNull(sourceStats, "sourceStats is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.session = requireNonNull(session, "session is null");
            this.types = ImmutableMap.copyOf(types);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, Void context)
        {
            Iterator<Rule> ruleIterator = getCandidates(node).iterator();
            while (ruleIterator.hasNext()) {
                Rule rule = ruleIterator.next();
                Optional<PlanNodeStatsEstimate> calculatedStats = rule.calculate(node, sourceStats, lookup, session, types);
                if (calculatedStats.isPresent()) {
                    PlanNodeStatsEstimate stats = calculatedStats.get();
                    checkConsistent(normalizer, rule.toString(), stats, node.getOutputSymbols(), types);
                    return stats;
                }
            }
            return PlanNodeStatsEstimate.UNKNOWN_STATS;
        }
    }
}
