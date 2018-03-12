/*
 * Copyright 2016-2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kotlinx.coroutines.experimental.channels

import kotlinx.coroutines.experimental.*
import kotlin.coroutines.experimental.*
import kotlin.test.*

/**
 * Tests that various operators on channels properly consume (close) their source channels.
 */
class ChannelsConsumeTest {
    private val sourceList = (1..10).toList()

    // test source with numbers 1..10
    private fun testSource() = produce {
        for (i in sourceList) {
            send(i)
        }
    }

    @Test
    fun testConsume() {
        checkTerminal {
            consume {
                assertEquals(1, receive())
            }
        }
    }

    @Test
    fun testConsumeEach() {
        checkTerminal {
            var sum = 0
            consumeEach { sum += it }
            assertEquals(55, sum)
        }
    }

    @Test
    fun testConsumeEachIndexed() {
        checkTerminal {
            var sum = 0
            consumeEachIndexed { (index, i) -> sum += index * i }
            assertEquals(330, sum)
        }
    }

    @Test
    fun testElementAt() {
        checkTerminal {
            assertEquals(2, elementAt(1))
        }
        checkTerminal(expected = { it is IndexOutOfBoundsException }) {
            elementAt(10)
        }
    }

    @Test
    fun testElementAtOrElse() {
        checkTerminal {
            assertEquals(3, elementAtOrElse(2) { error("Cannot happen") })
        }
        checkTerminal {
            assertEquals(-23, elementAtOrElse(10) { -23 })
        }
    }

    @Test
    fun testElementOrNull() {
        checkTerminal {
            assertEquals(4, elementAtOrNull(3))
        }
        checkTerminal {
            assertEquals(null, elementAtOrNull(10))
        }
    }

    @Test
    fun testFind() {
        checkTerminal {
            assertEquals(3, find { it % 3 == 0 })
        }
    }

    @Test
    fun testFindLast() {
        checkTerminal {
            assertEquals(9, findLast { it % 3 == 0 })
        }
    }

    @Test
    fun testFirst() {
        checkTerminal {
            assertEquals(1, first())
        }
    }

    @Test
    fun testFirstPredicate() {
        checkTerminal {
            assertEquals(3, first { it % 3 == 0 })
        }
        checkTerminal(expected = { it is NoSuchElementException }) {
            first { it > 10 }
        }
    }

    @Test
    fun testFirstOrNull() {
        checkTerminal {
            assertEquals(1, firstOrNull())
        }
    }

    @Test
    fun testFirstOrNullPredicate() {
        checkTerminal {
            assertEquals(3, firstOrNull { it % 3 == 0 })
        }
        checkTerminal {
            assertEquals(null, firstOrNull { it > 10 })
        }
    }

    @Test
    fun testIndexOf() {
        checkTerminal {
            assertEquals(2, indexOf(3))
        }
        checkTerminal {
            assertEquals(-1, indexOf(11))
        }
    }

    @Test
    fun testIndexOfFirst() {
        checkTerminal {
            assertEquals(2, indexOfFirst { it % 3 == 0 })
        }
        checkTerminal {
            assertEquals(-1, indexOfFirst { it > 10 })
        }
    }

    @Test
    fun testIndexOfLast() {
        checkTerminal {
            assertEquals(8, indexOfLast { it % 3 == 0 })
        }
        checkTerminal {
            assertEquals(-1, indexOfLast { it > 10 })
        }
    }

    @Test
    fun testLast() {
        checkTerminal {
            assertEquals(10, last())
        }
    }

    @Test
    fun testLastPredicate() {
        checkTerminal {
            assertEquals(9, last { it % 3 == 0 })
        }
        checkTerminal(expected = { it is NoSuchElementException }) {
            last { it > 10 }
        }
    }

    @Test
    fun testLastIndexOf() {
        checkTerminal {
            assertEquals(8, lastIndexOf(9))
        }
    }

    @Test
    fun testLastOrNull() {
        checkTerminal {
            assertEquals(10, lastOrNull())
        }
    }

    @Test
    fun testLastOrNullPredicate() {
        checkTerminal {
            assertEquals(9, lastOrNull { it % 3 == 0 })
        }
        checkTerminal {
            assertEquals(null, lastOrNull { it > 10 })
        }
    }

    @Test
    fun testSingle() {
        checkTerminal(expected = { it is IllegalArgumentException }) {
            single()
        }
    }

    @Test
    fun testSinglePredicate() {
        checkTerminal {
            assertEquals(7, single { it % 7 == 0 })
        }
        checkTerminal(expected = { it is IllegalArgumentException }) {
            single { it % 3 == 0 }
        }
        checkTerminal(expected = { it is NoSuchElementException }) {
            single { it > 10 }
        }
    }

    @Test
    fun testSingleOrNull() {
        checkTerminal {
            assertEquals(null, singleOrNull())
        }
    }

    @Test
    fun testSingleOrNullPredicate() {
        checkTerminal {
            assertEquals(7, singleOrNull { it % 7 == 0 })
        }
        checkTerminal {
            assertEquals(null, singleOrNull { it % 3 == 0 })
        }
        checkTerminal {
            assertEquals(null, singleOrNull { it > 10 })
        }
    }

    @Test
    fun testDrop() {
        checkTransform(sourceList.drop(3)) { ctx ->
            drop(3, ctx)
        }
    }

    @Test
    fun testDropWhile() {
        checkTransform(sourceList.dropWhile { it < 4}) { ctx ->
            dropWhile(ctx) { it < 4 }
        }
    }

    @Test
    fun testFilter() {
        checkTransform(sourceList.filter { it % 2 == 0 }) { ctx ->
            filter(ctx) { it % 2 == 0 }
        }
    }

    @Test
    fun testFilterIndexed() {
        checkTransform(sourceList.filterIndexed { index, _ -> index % 2 == 0 }) { ctx ->
            filterIndexed(ctx) { index, _ -> index % 2 == 0 }
        }
    }

    @Test
    fun testFilterIndexedToCollection() {
        checkTerminal {
            val list = mutableListOf<Int>()
            filterIndexedTo(list) { index, _ -> index % 2 == 0 }
            assertEquals(listOf(1, 3, 5, 7, 9), list)
        }
    }

    @Test
    fun testFilterIndexedToChannel() {
        checkTerminal {
            val channel = Channel<Int>()
            val result = async { channel.toList() }
            filterIndexedTo(channel) { index, _ -> index % 2 == 0 }
            channel.close()
            assertEquals(listOf(1, 3, 5, 7, 9), result.await())
        }
    }

    @Test
    fun testFilterNot() {
        checkTransform(sourceList.filterNot { it % 2 == 0 }) { ctx ->
            filterNot(ctx) { it % 2 == 0 }
        }
    }

    @Test
    fun testFilterNotNullToCollection() {
        checkTerminal {
            val list = mutableListOf<Int>()
            filterNotNullTo(list)
            assertEquals((1..10).toList(), list)
        }
    }

    @Test
    fun testFilterNotNullToChannel() {
        checkTerminal {
            val channel = Channel<Int>()
            val result = async { channel.toList() }
            filterNotNullTo(channel)
            channel.close()
            assertEquals((1..10).toList(), result.await())
        }
    }

    @Test
    fun testFilterNotToCollection() {
        checkTerminal {
            val list = mutableListOf<Int>()
            filterNotTo(list) { it % 2 == 0 }
            assertEquals(listOf(1, 3, 5, 7, 9), list)
        }
    }

    @Test
    fun testFilterNotToChannel() {
        checkTerminal {
            val channel = Channel<Int>()
            val result = async { channel.toList() }
            filterNotTo(channel) { it % 2 == 0 }
            channel.close()
            assertEquals(listOf(1, 3, 5, 7, 9), result.await())
        }
    }

    @Test
    fun testFilterToCollection() {
        checkTerminal {
            val list = mutableListOf<Int>()
            filterTo(list) { it % 2 == 0 }
            assertEquals(listOf(2, 4, 6, 8, 10), list)
        }
    }

    @Test
    fun testFilterToChannel() {
        checkTerminal {
            val channel = Channel<Int>()
            val result = async { channel.toList() }
            filterTo(channel) { it % 2 == 0 }
            channel.close()
            assertEquals(listOf(2, 4, 6, 8, 10), result.await())
        }
    }

    @Test
    fun testTake() {
        checkTransform(sourceList.take(3)) { ctx ->
            take(3, ctx)
        }
    }

    @Test
    fun testTakeWhile() {
        checkTransform(sourceList.takeWhile { it < 4 }) { ctx ->
            takeWhile(ctx) { it < 4 }
        }
    }

    @Test
    fun testAssociate() {
        checkTerminal {
            assertEquals(sourceList.associate { it to it.toString() }, associate { it to it.toString() })
        }
    }

    @Test
    fun testAssociateBy() {
        checkTerminal {
            assertEquals(sourceList.associateBy { it.toString() }, associateBy { it.toString() })
        }
    }

    @Test
    fun testAssociateByTwo() {
        checkTerminal {
            assertEquals(sourceList.associateBy({ it.toString() }, { it + 1}), associateBy({ it.toString() }, { it + 1}))
        }
    }

    @Test
    fun testAssociateByToMap() {
        checkTerminal {
            val map = mutableMapOf<String, Int>()
            associateByTo(map) { it.toString() }
            assertEquals(sourceList.associateBy { it.toString() }, map)
        }
    }

    @Test
    fun testAssociateByTwoToMap() {
        checkTerminal {
            val map = mutableMapOf<String, Int>()
            associateByTo(map, { it.toString() }, { it + 1})
            assertEquals(sourceList.associateBy({ it.toString() }, { it + 1}), map)
        }
    }

    @Test
    fun testAssociateToMap() {
        checkTerminal {
            val map = mutableMapOf<Int, String>()
            associateTo(map) { it to it.toString() }
            assertEquals(sourceList.associate { it to it.toString() }, map)
        }
    }

    @Test
    fun testMap() {
        checkTransform(sourceList.map { it.toString() }) { ctx ->
            map(ctx) { it.toString() }
        }
    }

    // ------------------
    
    private fun checkTerminal(
        expected: ((Throwable?) -> Unit)? = null,
        terminal: suspend ReceiveChannel<Int>.() -> Unit)
    {
        val src = testSource()
        runBlocking {
            try {
                // terminal operation
                terminal(src)
                // source must be cancelled at the end of terminal op
                assertTrue(src.isClosedForReceive, "Source must be closed")
                if (expected != null) error("Exception was expected")
            } catch (e: Throwable) {
                if (expected == null) throw e
                expected(e)
            }
        }
    }

    private fun <R> checkTransform(
        expect: List<R>,
        transform: ReceiveChannel<Int>.(CoroutineContext) -> ReceiveChannel<R>
    ) {
        // check for varying number of received elements from the channel
        for (nReceive in 0..expect.size) {
            checkTransform(nReceive, expect, transform)
        }
    }

    private fun <R> checkTransform(
        nReceive: Int,
        expect: List<R>,
        transform: ReceiveChannel<Int>.(CoroutineContext) -> ReceiveChannel<R>
    ) {
        val src = testSource()
        runBlocking {
            // transform
            val res = transform(src, coroutineContext)
            // receive nReceive elements from the result
            repeat(nReceive) { i ->
                assertEquals(expect[i], res.receive())
            }
            if (nReceive < expect.size) {
                // then cancel
                res.cancel()
            } else {
                // then check that result is closed
                assertEquals(null, res.receiveOrNull(), "Result has unexpected values")
            }
        }
        // source must be cancelled when runBlocking processes all the scheduled stuff
        assertTrue(src.isClosedForReceive, "Source must be closed")
    }
}