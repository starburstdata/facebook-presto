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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final LocalPartitionGenerator partitionGenerator;
    private final IntArrayList[] partitionAssignments;
    private final boolean preferPageCopying;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LocalExchangeMemoryManager memoryManager,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> hashChannel,
            boolean preferPageCopying)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.preferPageCopying = preferPageCopying;

        HashGenerator hashGenerator;
        if (hashChannel.isPresent()) {
            hashGenerator = new PrecomputedHashGenerator(hashChannel.get());
        }
        else {
            List<Type> partitionChannelTypes = partitionChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());
            hashGenerator = new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels));
        }
        partitionGenerator = new LocalPartitionGenerator(hashGenerator, buffers.size());

        partitionAssignments = new IntArrayList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
    }

    @Override
    public synchronized void accept(Page page)
    {
        // reset the assignment lists
        for (IntList partitionAssignment : partitionAssignments) {
            partitionAssignment.clear();
        }

        // assign each row to a partition
        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(page, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        Block[] sourceBlocks = page.getBlocks();
        Block[] outputBlocks = new Block[sourceBlocks.length];
        ConcurrentHashMap<Object, AtomicInteger> referenceCountMap = new ConcurrentHashMap<>(
                page.getChannelCount() * 3,
                0.75f,
                buffers.size());
        for (int partition = 0; partition < buffers.size(); partition++) {
            IntArrayList positions = partitionAssignments[partition];

            if (!positions.isEmpty()) {
                int[] positionsArray = positions.elements();
                Map<Object, Long> parts = new HashMap<>();

                if (!preferPageCopying) {
                    positionsArray = Arrays.copyOf(positionsArray, positions.size());
                }

                for (int i = 0; i < sourceBlocks.length; i++) {
                    if (preferPageCopying) {
                        outputBlocks[i] = sourceBlocks[i].copyPositions(positionsArray, 0, positions.size());
                    }
                    else {
                        outputBlocks[i] = sourceBlocks[i].getPositions(positionsArray, 0, positions.size());
                        outputBlocks[i].retainedBytesForEachPart(parts::put);
                    }
                }

                Page pageSplit = new Page(positions.size(), outputBlocks);
                incrementPageMemoryUsage(referenceCountMap, parts, page);
                buffers.get(partition).accept(new PageReference(pageSplit, 1, () -> decrementPageMemoryUsage(referenceCountMap, parts, page)));
            }
        }
    }

    private void incrementPageMemoryUsage(ConcurrentHashMap<Object, AtomicInteger> referenceCountMap, Map<Object, Long> parts, Page page)
    {
        if (preferPageCopying) {
            memoryManager.updateMemoryUsage(page.getRetainedSizeInBytes());
            return;
        }

        long size = parts.entrySet().stream()
                .filter(entry -> referenceCountMap.computeIfAbsent(entry.getKey(), key -> new AtomicInteger()).incrementAndGet() == 1)
                .mapToLong(Map.Entry::getValue)
                .sum();
        memoryManager.updateMemoryUsage(size);
    }

    private void decrementPageMemoryUsage(ConcurrentHashMap<Object, AtomicInteger> referenceCountMap, Map<Object, Long> parts, Page page)
    {
        if (preferPageCopying) {
            memoryManager.updateMemoryUsage(-page.getRetainedSizeInBytes());
            return;
        }

        long size = parts.entrySet().stream()
                .filter(entry -> referenceCountMap.get(entry.getKey()).decrementAndGet() == 0)
                .mapToLong(Map.Entry::getValue)
                .sum();
        memoryManager.updateMemoryUsage(-size);
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
