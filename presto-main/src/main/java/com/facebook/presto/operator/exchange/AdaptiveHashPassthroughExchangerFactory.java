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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Murmur3Hash128;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class AdaptiveHashPassthroughExchangerFactory
{
    private static final double HLL_RSD = 0.15;
    private static final DataSize VALIDATION_SIZE_THRESHOLD = new DataSize(512, MEGABYTE);
    private static final DataSize UPDATE_SIZE_THRESHOLD = new DataSize(4, MEGABYTE);
    private static final double FALLBACK_TO_PASSTHROUGH_MIN_REDUCTION = 5.;
    private static final double FALLBACK_TO_PASSTHROUGH_MAX_REDUCTION = 100.;

    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final LocalPartitionGenerator partitionGenerator;

    private final AtomicBoolean passthrough = new AtomicBoolean();
    private final AtomicBoolean partitionPages = new AtomicBoolean();
    private final List<BufferOccupation> buffersOccupation = new ArrayList<>();

    private final List<HyperLogLog> globalDistinctHashesEstimates = new ArrayList<>();
    private long globalEstimatePositions;
    private long globalEstimateDataSize;

    public AdaptiveHashPassthroughExchangerFactory(
            List<Consumer<PageReference>> buffers,
            LocalExchangeMemoryManager memoryManager,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(buffers, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        partitionGenerator = new LocalPartitionGenerator(getHashGenerator(types, partitionChannels, hashChannel), buffers.size());
        IntStream.range(0, buffers.size()).forEach(index -> buffersOccupation.add(new BufferOccupation(index)));
    }

    public LocalExchanger createExchanger()
    {
        // reuse buffers to reduce pages fanout in passthrough mode
        int bufferIndex = incrementLeastOccupiedBuffer();
        return new AdaptiveHashPassthroughExchanger(bufferIndex);
    }

    private static HashGenerator getHashGenerator(List<? extends Type> types, List<Integer> partitionChannels, Optional<Integer> hashChannel)
    {
        if (hashChannel.isPresent()) {
            return new PrecomputedHashGenerator(hashChannel.get());
        }

        List<Type> partitionChannelTypes = partitionChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        return new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels));
    }

    private synchronized int incrementLeastOccupiedBuffer()
    {
        BufferOccupation leastOccupiedBuffer = getLeastOccupiedBuffer();
        leastOccupiedBuffer.incrementOccupation();
        return leastOccupiedBuffer.getBufferIndex();
    }

    private synchronized void decrementBufferOccupation(int bufferIndex)
    {
        BufferOccupation bufferOccupation = null;
        for (Iterator<BufferOccupation> iterator = buffersOccupation.iterator(); iterator.hasNext(); ) {
            bufferOccupation = iterator.next();
            if (bufferOccupation.getBufferIndex() == bufferIndex) {
                iterator.remove();
                break;
            }
        }

        checkState(bufferOccupation != null);
        bufferOccupation.decrementOccupation();
        buffersOccupation.add(0, bufferOccupation);
    }

    private BufferOccupation getLeastOccupiedBuffer()
    {
        BufferOccupation minBufferOccupation = buffersOccupation.get(0);
        for (int i = 1; i < buffersOccupation.size(); ++i) {
            BufferOccupation bufferOccupation = buffersOccupation.get(i);
            if (bufferOccupation.getOccupation() < minBufferOccupation.getOccupation()) {
                minBufferOccupation = bufferOccupation;
                if (minBufferOccupation.getOccupation() == 0) {
                    return minBufferOccupation;
                }
            }
        }
        return minBufferOccupation;
    }

    private void updateHashEstimates(HyperLogLog localDistinctHashesEstimates, long localEstimatePositions, long localEstimateDataSize)
    {
        List<HyperLogLog> distinctHashesEstimates;
        long estimatePositions;
        synchronized (globalDistinctHashesEstimates) {
            globalDistinctHashesEstimates.add(localDistinctHashesEstimates);
            globalEstimateDataSize += localEstimateDataSize;
            globalEstimatePositions += localEstimatePositions;

            if (globalEstimateDataSize < VALIDATION_SIZE_THRESHOLD.toBytes()) {
                return;
            }

            distinctHashesEstimates = ImmutableList.copyOf(globalDistinctHashesEstimates);
            estimatePositions = globalEstimatePositions;

            globalDistinctHashesEstimates.clear();
            globalEstimateDataSize = 0;
            globalEstimatePositions = 0;
        }

        HyperLogLog aggregatedDistinctHashesEstimate = distinctHashesEstimates.stream().reduce((accumulator, element) -> {
            try {
                accumulator.addAll(element);
                return accumulator;
            }
            catch (CardinalityMergeException ex) {
                throw new RuntimeException(ex);
            }
        }).get();

        if (shouldPassthroughPages(estimatePositions, aggregatedDistinctHashesEstimate)) {
            // reduce factor is unsatisfactory
            System.err.println("PASSTHROUGH " + ((double) estimatePositions / aggregatedDistinctHashesEstimate.cardinality()));
            passthrough.set(true);
        }
        else {
            System.err.println("PARTITION " + ((double) estimatePositions / aggregatedDistinctHashesEstimate.cardinality()));
            partitionPages.set(true);
        }
    }

    private static boolean shouldPassthroughPages(long positions, HyperLogLog hll)
    {
        double reductionFactor = (double) positions / hll.cardinality();
        return reductionFactor < FALLBACK_TO_PASSTHROUGH_MIN_REDUCTION || reductionFactor > FALLBACK_TO_PASSTHROUGH_MAX_REDUCTION;
    }

    private class AdaptiveHashPassthroughExchanger
            implements LocalExchanger
    {
        private final IntArrayList[] partitionAssignments;
        private int passthroughBufferIndex;

        private HyperLogLog distinctHashesEstimate;
        private long estimateDataSize;
        private long estimatePositions;

        private AdaptiveHashPassthroughExchanger(int passthroughBufferIndex)
        {
            this.passthroughBufferIndex = passthroughBufferIndex;

            partitionAssignments = new IntArrayList[buffers.size()];
            Arrays.setAll(partitionAssignments, partition -> new IntArrayList());

            distinctHashesEstimate = new HyperLogLog(HLL_RSD);
        }

        @Override
        public void accept(Page page)
        {
            if (passthrough.get()) {
                passthroughPage(page);
                return;
            }

            if (partitionPages.get()) {
                // reset the assignment lists
                for (IntList partitionAssignment : partitionAssignments) {
                    partitionAssignment.clear();
                }

                // assign each row to a partition
                for (int position = 0; position < page.getPositionCount(); position++) {
                    long rawHash = partitionGenerator.getRawHash(page, position);
                    int partition = partitionGenerator.getPartition(rawHash);
                    partitionAssignments[partition].add(position);
                }

                // since pages partitioning is costly, do it only when reduce factor is satisfactory
                // build a page for each partition
                Block[] sourceBlocks = page.getBlocks();
                Block[] outputBlocks = new Block[sourceBlocks.length];
                for (int partition = 0; partition < buffers.size(); partition++) {
                    IntArrayList positions = partitionAssignments[partition];
                    if (!positions.isEmpty()) {
                        for (int i = 0; i < sourceBlocks.length; i++) {
                            outputBlocks[i] = sourceBlocks[i].copyPositions(positions.elements(), 0, positions.size());
                        }

                        Page pageSplit = new Page(positions.size(), outputBlocks);
                        memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
                        buffers.get(partition).accept(new PageReference(pageSplit, 1, () -> memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes())));
                    }
                }

                return;
            }

            estimatePositions += page.getPositionCount();
            estimateDataSize += page.getRetainedSizeInBytes();

            // assign each row to a partition
            for (int position = 0; position < page.getPositionCount(); position++) {
                long rawHash = partitionGenerator.getRawHash(page, position);
                distinctHashesEstimate.offerHashed(Murmur3Hash128.hash64(rawHash));
            }

            passthroughPage(page);

            if (estimateDataSize >= UPDATE_SIZE_THRESHOLD.toBytes()) {
                updateHashEstimates(distinctHashesEstimate, estimatePositions, estimateDataSize);
                distinctHashesEstimate = new HyperLogLog(0.15);
                estimatePositions = 0;
                estimateDataSize = 0;
            }
        }

        private void passthroughPage(Page page)
        {
            memoryManager.updateMemoryUsage(page.getRetainedSizeInBytes());
            PageReference pageReference = new PageReference(page, 1, () -> memoryManager.updateMemoryUsage(-page.getRetainedSizeInBytes()));
            buffers.get(passthroughBufferIndex).accept(pageReference);
        }

        @Override
        public void close()
        {
            updateHashEstimates(distinctHashesEstimate, estimatePositions, estimateDataSize);
            decrementBufferOccupation(passthroughBufferIndex);
        }

        @Override
        public ListenableFuture<?> waitForWriting()
        {
            return memoryManager.getNotFullFuture();
        }
    }

    private static class BufferOccupation
    {
        final int bufferIndex;
        int occupation;

        BufferOccupation(int bufferIndex)
        {
            this.bufferIndex = bufferIndex;
        }

        int getBufferIndex()
        {
            return bufferIndex;
        }

        int getOccupation()
        {
            return occupation;
        }

        void incrementOccupation()
        {
            occupation++;
        }

        void decrementOccupation()
        {
            occupation--;
        }
    }
}
