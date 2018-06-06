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
package com.facebook.presto.operator.project;

import com.facebook.presto.operator.WorkProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.WorkProcessorAssertion.assertFinishes;
import static com.facebook.presto.operator.WorkProcessorAssertion.assertResult;
import static com.facebook.presto.operator.project.MergePages.mergePages;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static java.lang.Math.toIntExact;

public class TestMergePages
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, REAL, DOUBLE);

    @Test
    public void testMinPageSizeThreshold()
    {
        Page page = createSequencePage(TYPES, 10);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                page.getSizeInBytes(),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                pagesSource(page),
                newSimpleAggregatedMemoryContext());

        assertResult(mergePages, (Consumer<Page>) actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testMinRowCountThreshold()
    {
        Page page = createSequencePage(TYPES, 10);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                1024 * 1024,
                page.getPositionCount(),
                Integer.MAX_VALUE,
                pagesSource(page),
                newSimpleAggregatedMemoryContext());

        assertResult(mergePages, (Consumer<Page>) actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testBufferSmallPages()
    {
        int singlePageRowCount = 10;
        Page page = createSequencePage(TYPES, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                page.getSizeInBytes() + 1,
                page.getPositionCount() + 1,
                Integer.MAX_VALUE,
                pagesSource(splits.get(0), splits.get(1)),
                newSimpleAggregatedMemoryContext());

        assertResult(mergePages, (Consumer<Page>) actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testFlushOnBigPage()
    {
        Page smallPage = createSequencePage(TYPES, 10);
        Page bigPage = createSequencePage(TYPES, 100);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                bigPage.getSizeInBytes(),
                bigPage.getPositionCount(),
                Integer.MAX_VALUE,
                pagesSource(smallPage, bigPage),
                newSimpleAggregatedMemoryContext());

        assertResult(mergePages, (Consumer<Page>) actualPage -> assertPageEquals(TYPES, actualPage, smallPage));
        assertResult(mergePages, (Consumer<Page>) actualPage -> assertPageEquals(TYPES, actualPage, bigPage));
        assertFinishes(mergePages);
    }

    @Test
    public void testFlushOnFullPage()
    {
        int singlePageRowCount = 10;
        List<Type> types = ImmutableList.of(BIGINT);
        Page page = createSequencePage(types, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        WorkProcessor<Page> mergePages = mergePages(
                types,
                page.getSizeInBytes() / 2 + 1,
                page.getPositionCount() / 2 + 1,
                toIntExact(page.getSizeInBytes()),
                pagesSource(splits.get(0), splits.get(1), splits.get(0), splits.get(1)),
                newSimpleAggregatedMemoryContext());

        assertResult(mergePages, (Consumer<Page>) actualPage -> assertPageEquals(types, actualPage, page));
        assertResult(mergePages, (Consumer<Page>) actualPage -> assertPageEquals(types, actualPage, page));
        assertFinishes(mergePages);
    }

    private static WorkProcessor<Page> pagesSource(Page... pages)
    {
        return WorkProcessor.fromIterable(ImmutableList.copyOf(pages));
    }
}
