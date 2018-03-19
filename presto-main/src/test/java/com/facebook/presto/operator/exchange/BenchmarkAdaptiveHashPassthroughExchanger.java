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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.BigintOperators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 2, timeUnit = SECONDS)
public class BenchmarkAdaptiveHashPassthroughExchanger
{
    private static class Buffer
            implements Consumer<PageReference>
    {
        private final List<Page> pages = new ArrayList<>();

        @Override
        public void accept(PageReference pageReference)
        {
            pages.add(pageReference.removePage());
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @Benchmark
    public Object benchmarkExchanger(BenchmarkData data)
    {
        data.getBuffer().getPages().clear();
        AdaptiveHashPassthroughExchangerFactory factory = new AdaptiveHashPassthroughExchangerFactory(
                ImmutableList.of(data.getBuffer()),
                new LocalExchangeMemoryManager(new DataSize(512, MEGABYTE).toBytes()),
                data.getTypes(),
                Ints.asList(data.getChannels()),
                data.getHashChannel());
        LocalExchanger exchanger = factory.createExchanger();
        for (Page page : data.getPages()) {
            exchanger.accept(page);
        }

        return data.getBuffer().getPages();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"20000000"})
        private int positions = 20000000;

        @Param({"1"})
        private int channelCount = 1;

        @Param("10000000")
        private int groupCount = 10000000;

        @Param({"true", "false"})
        private boolean hashEnabled = true;

        private List<Page> pages;
        private Optional<Integer> hashChannel;
        private List<Type> types;
        private int[] channels;
        private Buffer buffer;

        @Setup
        public void setup()
        {
            pages = createPages(positions, groupCount, Collections.nCopies(channelCount, BIGINT), hashEnabled);
            hashChannel = hashEnabled ? Optional.of(channelCount) : Optional.empty();
            types = Collections.nCopies(channelCount, BIGINT);
            channels = new int[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = i;
            }
            buffer = new Buffer();
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Optional<Integer> getHashChannel()
        {
            return hashChannel;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public int[] getChannels()
        {
            return channels;
        }

        public Buffer getBuffer()
        {
            return buffer;
        }
    }

    private static List<Page> createPages(int positionCount, int groupCount, List<Type> types, boolean hashEnabled)
    {
        int channelCount = types.size();

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            int rand = position % groupCount;
            pageBuilder.declarePosition();
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(numChannel), rand);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), BigintOperators.hashCode(rand));
            }
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    @Test
    public void test()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        benchmarkExchanger(data);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkAdaptiveHashPassthroughExchanger.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
