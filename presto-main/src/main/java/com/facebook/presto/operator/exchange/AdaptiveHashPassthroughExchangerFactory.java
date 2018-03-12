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

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.cardinality.HyperLogLog;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class AdaptiveHashPassthroughExchangerFactory
{
    private static final int HLL_BUCKETS = 512;
    private static final DataSize VALIDATION_SIZE_THRESHOLD = new DataSize(32, MEGABYTE);
    private static final DataSize UPDATE_SIZE_THRESHOLD = new DataSize(2, MEGABYTE);
    private static final double FALLBACK_TO_PASSTHROUGH_MIN_REDUCTION = 5.;
    private static final double FALLBACK_TO_PASSTHROUGH_MAX_REDUCTION = 100.;

    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final LocalPartitionGenerator partitionGenerator;

    private final AtomicBoolean passthrough = new AtomicBoolean();
    private final int[] buffersOccupation;

    private final List<HyperLogLog> globalDistinctHashesEstimates = new ArrayList<>();
    private long globalEstimatesPositions;
    private long globalEstimatesDataSize;

    public AdaptiveHashPassthroughExchangerFactory(
            List<Consumer<PageReference>> partitions,
            LocalExchangeMemoryManager memoryManager,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

        partitionGenerator = new LocalPartitionGenerator(getHashGenerator(types, partitionChannels, hashChannel), buffers.size());
        buffersOccupation = new int[buffers.size()];
    }

    public LocalExchanger createExchanger()
    {
        return new AdaptiveHashPassthroughExchanger(incrementLeastOccupiedBuffer());
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

    private int incrementLeastOccupiedBuffer()
    {
        synchronized (buffersOccupation) {
            int leastOccupiedBufferIndex = getLeastOccupiedBuffer();
            buffersOccupation[leastOccupiedBufferIndex]++;
            return leastOccupiedBufferIndex;
        }
    }

    private int updateOccupiedBuffer(int currentBufferIndex)
    {
        synchronized (buffersOccupation) {
            int leastOccupiedBufferIndex = getLeastOccupiedBuffer();
            if (buffersOccupation[leastOccupiedBufferIndex] + 1 < buffersOccupation[currentBufferIndex]) {
                // switch to less occupied buffer
                buffersOccupation[leastOccupiedBufferIndex]++;
                buffersOccupation[currentBufferIndex]--;
                return leastOccupiedBufferIndex;
            }
            return currentBufferIndex;
        }
    }

    private int getLeastOccupiedBuffer()
    {
        synchronized (buffersOccupation) {
            int minIndex = 0;
            int minOccupation = buffersOccupation[0];
            for (int i = 1; i < buffersOccupation.length; ++i) {
                int occupation = buffersOccupation[i];
                if (occupation < minOccupation) {
                    minIndex = i;
                    minOccupation = occupation;
                }
            }
            return minIndex;
        }
    }

    private void decrementBufferUsage(int bufferIndex)
    {
        synchronized (buffersOccupation) {
            buffersOccupation[bufferIndex]--;
        }
    }

    private void updateHashEstimates(HyperLogLog localDistinctHashesEstimates, long localEstimatesPositions, long localEstimatesDataSize)
    {
        List<HyperLogLog> distinctHashesEstimates;
        long estimatesPositions;
        synchronized (globalDistinctHashesEstimates) {
            globalDistinctHashesEstimates.add(localDistinctHashesEstimates);
            globalEstimatesDataSize += localEstimatesDataSize;
            globalEstimatesPositions += localEstimatesPositions;

            if (globalEstimatesDataSize < VALIDATION_SIZE_THRESHOLD.toBytes() * buffers.size()) {
                return;
            }

            distinctHashesEstimates = ImmutableList.copyOf(globalDistinctHashesEstimates);
            estimatesPositions = globalEstimatesPositions;

            globalDistinctHashesEstimates.clear();
            globalEstimatesDataSize = 0;
            globalEstimatesPositions = 0;
        }

        HyperLogLog distinctHashesEstimate = distinctHashesEstimates.stream().reduce((accumulator, element) -> {
            accumulator.mergeWith(element);
            return accumulator;
        }).get();

        double reductionFactor = (double) estimatesPositions / distinctHashesEstimate.cardinality();
        if (reductionFactor < FALLBACK_TO_PASSTHROUGH_MIN_REDUCTION || reductionFactor > FALLBACK_TO_PASSTHROUGH_MAX_REDUCTION) {
            System.err.println("PASSTHROUGH " + reductionFactor);
            passthrough.set(true);
        }
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

            distinctHashesEstimate = HyperLogLog.newInstance(HLL_BUCKETS);
        }

        @Override
        public void accept(Page page)
        {
            estimatePositions += page.getPositionCount();
            estimateDataSize += page.getRetainedSizeInBytes();

            if (passthrough.get()) {
                memoryManager.updateMemoryUsage(page.getRetainedSizeInBytes());
                PageReference pageReference = new PageReference(page, 1, () -> memoryManager.updateMemoryUsage(-page.getRetainedSizeInBytes()));
                buffers.get(passthroughBufferIndex).accept(pageReference);

                if (estimateDataSize >= UPDATE_SIZE_THRESHOLD.toBytes()) {
                    passthroughBufferIndex = updateOccupiedBuffer(passthroughBufferIndex);
                    estimatePositions = 0;
                    estimateDataSize = 0;
                }

                return;
            }

            // reset the assignment lists
            for (IntList partitionAssignment : partitionAssignments) {
                partitionAssignment.clear();
            }

            // assign each row to a partition
            for (int position = 0; position < page.getPositionCount(); position++) {
                long rawHash = partitionGenerator.getRawHash(page, position);
                int partition = partitionGenerator.getPartition(rawHash);
                partitionAssignments[partition].add(position);
                distinctHashesEstimate.add(rawHash);
            }

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

            if (estimateDataSize >= UPDATE_SIZE_THRESHOLD.toBytes()) {
                updateHashEstimates(distinctHashesEstimate, estimatePositions, estimateDataSize);
                distinctHashesEstimate = HyperLogLog.newInstance(HLL_BUCKETS);
                estimatePositions = 0;
                estimateDataSize = 0;
            }
        }

        @Override
        public void close()
        {
            decrementBufferUsage(passthroughBufferIndex);
            updateHashEstimates(distinctHashesEstimate, estimatePositions, estimateDataSize);
        }

        @Override
        public ListenableFuture<?> waitForWriting()
        {
            return memoryManager.getNotFullFuture();
        }
    }
}
