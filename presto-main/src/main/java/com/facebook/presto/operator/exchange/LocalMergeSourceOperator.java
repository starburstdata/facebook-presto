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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.operator.ContinuousWork;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PageComparatorFactory;
import com.facebook.presto.operator.PageWithPositionComparator;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.ContinuousWorkUtils.WorkState;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.util.MergeSortedPages.mergeSortedPages;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LocalMergeSourceOperator
        implements Operator
{
    public static class LocalMergeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageComparatorFactory comparatorFactory;
        private final LocalExchangeFactory localExchangeFactory;
        private final List<Type> types;
        private final List<Integer> sortChannels;
        private final List<SortOrder> orderings;
        private boolean closed;

        public LocalMergeSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageComparatorFactory comparatorFactory,
                LocalExchangeFactory localExchangeFactory,
                List<Type> types,
                List<Integer> sortChannels,
                List<SortOrder> orderings)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.comparatorFactory = requireNonNull(comparatorFactory, "comparator is null");
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "exchange is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            LocalExchange inMemoryExchange = localExchangeFactory.getLocalExchange(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSourceOperator.class.getSimpleName());
            PageWithPositionComparator comparator = comparatorFactory.create(types, sortChannels, orderings);

            List<LocalExchangeSource> sources = IntStream.range(0, inMemoryExchange.getBufferCount())
                    .boxed()
                    .map(index -> inMemoryExchange.getNextSource())
                    .collect(toImmutableList());
            return new LocalMergeSourceOperator(operatorContext, sources, types, comparator);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final List<LocalExchangeSource> sources;
    private final List<Type> types;
    private final ContinuousWork<Page> mergedPages;

    public LocalMergeSourceOperator(OperatorContext operatorContext, List<LocalExchangeSource> sources, List<Type> types, PageWithPositionComparator comparator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sources = requireNonNull(sources, "sources is null");
        this.types = requireNonNull(types, "types is null");
        List<ContinuousWork<Page>> pageProducers = sources.stream()
                .map(LocalMergeSourceOperator::createPageProducerFor)
                .collect(toImmutableList());
        mergedPages = mergeSortedPages(
                pageProducers,
                requireNonNull(comparator, "comparator is null"),
                IntStream.range(0, getTypes().size()).boxed().collect(toImmutableList()),
                getTypes(),
                operatorContext.localUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        sources.forEach(LocalExchangeSource::finish);
    }

    @Override
    public boolean isFinished()
    {
        return mergedPages.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (mergedPages.isBlocked()) {
            return mergedPages.getBlockedFuture();
        }

        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!mergedPages.process() || mergedPages.isFinished()) {
            return null;
        }

        Page page = mergedPages.getResult();
        operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
        return page;
    }

    @Override
    public void close()
            throws IOException
    {
        sources.forEach(LocalExchangeSource::close);
    }

    private static ContinuousWork<Page> createPageProducerFor(LocalExchangeSource source)
    {
        return ContinuousWork.create(() -> {
            ListenableFuture<?> blocked = source.waitForReading();
            if (!blocked.isDone()) {
                return WorkState.blocked(blocked);
            }

            if (source.isFinished()) {
                return WorkState.finished();
            }

            return WorkState.ofResult(source.removePage());
        });
    }
}
