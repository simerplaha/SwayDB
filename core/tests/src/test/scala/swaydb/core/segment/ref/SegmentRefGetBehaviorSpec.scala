/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.segment.ref

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset}
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data.{SegmentKeyOrders, Persistent, PersistentOption, Time}
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.search.{SegmentSearcher, ThreadReadState}
import swaydb.core.{ACoreSpec, TestExecutionContext, TestSweeper}
import swaydb.core.segment.ASegmentSpec
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.{MaxKey, Slice}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._

import java.nio.file.Paths
import swaydb.testkit.TestKit._

class SegmentRefGetBehaviorSpec extends AnyWordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val keyOrders: SegmentKeyOrders = SegmentKeyOrders(keyOrder)
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = SegmentKeyOrders(keyOrder).partialKeyOrder
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))

  implicit val cacheMemorySweeper: Option[MemorySweeper.Cache] = None
  implicit val blockMemorySweeper: Option[MemorySweeper.Block] = None

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO: SegmentReadIO = SegmentReadIO.random

  "get" when {
    implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = None

    "key is > MaxKey.Fixed" should {
      "return none" in {
        implicit val segmentSearcher: SegmentSearcher = mock[SegmentSearcher]

        implicit val segmentRef: SegmentRef =
          new SegmentRef(
            path = Paths.get("1"),
            maxKey = MaxKey.Fixed[Slice[Byte]](100),
            minKey = 0,
            updateCount = 0,
            rangeCount = 0,
            putCount = 0,
            putDeadlineCount = 0,
            keyValueCount = 0,
            createdInLevel = 0,
            nearestPutDeadline = None,
            minMaxFunctionId = None,
            skipList = None,
            segmentBlockCache = null
          )

        SegmentRefReader.get(key = 101, threadState = null) shouldBe Persistent.Null

      }
    }

    "key is > MaxKey.Range" should {
      "return none" in {
        implicit val segmentSearcher: SegmentSearcher = mock[SegmentSearcher]

        implicit val segmentRef: SegmentRef =
          new SegmentRef(
            path = Paths.get("1"),
            maxKey = MaxKey.Range[Slice[Byte]](90, 100),
            minKey = 0,
            updateCount = 0,
            rangeCount = 0,
            putCount = 0,
            putDeadlineCount = 0,
            keyValueCount = 0,
            createdInLevel = 0,
            nearestPutDeadline = None,
            minMaxFunctionId = None,
            skipList = None,
            segmentBlockCache = null
          )

        SegmentRefReader.get(key = 100, threadState = null) shouldBe Persistent.Null
        SegmentRefReader.get(key = 101, threadState = null) shouldBe Persistent.Null

      }
    }
  }

  "GET - Behaviour" in {
    runThis(10.times, log = true) {
      TestSweeper {
        implicit sweeper =>
          implicit val ec = TestExecutionContext.executionContext
          implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = None

          //test data
          val path = Paths.get("1")

          val keyValues = randomKeyValues(1)
          val segmentBlockCache = getSegmentBlockCache(keyValues, bloomFilterConfig = BloomFilterBlockConfig.disabled())

          implicit val segmentSearcher: SegmentSearcher = mock[SegmentSearcher]

          implicit val segmentRef: SegmentRef =
            new SegmentRef(
              path = path,
              maxKey = MaxKey.Fixed[Slice[Byte]](Int.MaxValue),
              minKey = 0,
              updateCount = 0,
              rangeCount = 0,
              putCount = 0,
              putDeadlineCount = 0,
              keyValueCount = 0,
              createdInLevel = 0,
              nearestPutDeadline = None,
              minMaxFunctionId = None,
              skipList = None,
              segmentBlockCache = segmentBlockCache.head
            )

          val threadState = ThreadReadState.random

          threadState.getSegmentState(path).isNoneS shouldBe true

          def testKeyValue(key: Slice[Byte], indexOffset: Int, nextIndexOffset: Int) =
            Persistent.Remove(
              _key =
                eitherOne(
                  //tests that sorted keys are cutd.
                  (key ++ Slice.fill[Byte](10)(randomByte())).dropRight(10),
                  (Slice.fill[Byte](10)(randomByte()) ++ key).drop(10)
                ),
              deadline = None,
              _time = Time.empty,
              indexOffset = indexOffset,
              nextIndexOffset = nextIndexOffset,
              nextKeySize = 4,
              sortedIndexAccessPosition = 0,
              previousIndexOffset = 0
            )

          val keyValue1 = testKeyValue(key = 1, indexOffset = 0, nextIndexOffset = 1)
          val keyValue2 = testKeyValue(key = 2, indexOffset = 1, nextIndexOffset = 2)
          val keyValue3 = testKeyValue(key = 3, indexOffset = 2, nextIndexOffset = 3)

          val keyValue100 = testKeyValue(key = 100, indexOffset = 99, nextIndexOffset = 100)
          val keyValue101 = testKeyValue(key = 101, indexOffset = 100, nextIndexOffset = 101)

          /**
           * First perform sequential read - keyValue1
           */
          {
            (segmentSearcher
              .searchSequential(
                _: Slice[Byte],
                _: PersistentOption,
                _: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                _: UnblockedReader[ValuesBlockOffset, ValuesBlock])(_: KeyOrder[Slice[Byte]], _: KeyOrder[Persistent.Partial])
              )
              .expects(*, *, *, *, *, *)
              .onCall {
                (key: Slice[Byte], startFrom: PersistentOption, _, _, _, _) =>
                  key shouldBe keyValue1.key
                  startFrom.isNoneS shouldBe true
                  keyValue1
              }

            SegmentRefReader.get(key = keyValue1.key, threadState = threadState) shouldBe keyValue1

            val segmentState = threadState.getSegmentState(path).getS
            segmentState.keyValue._2 shouldBe keyValue1
            keyValue1.key.underlyingArraySize shouldBe 4
            segmentState.isSequential shouldBe true
          }

          /**
           * and then perform sequential read again - keyValue2
           */
          {
            (segmentSearcher
              .searchSequential(
                _: Slice[Byte],
                _: PersistentOption,
                _: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                _: UnblockedReader[ValuesBlockOffset, ValuesBlock])(_: KeyOrder[Slice[Byte]], _: KeyOrder[Persistent.Partial])
              )
              .expects(*, *, *, *, *, *)
              .onCall {
                (key: Slice[Byte], startFrom: PersistentOption, _, _, _, _) =>
                  key shouldBe keyValue2.key
                  startFrom shouldBe keyValue1
                  keyValue2
              }

            SegmentRefReader.get(key = keyValue2.key, threadState = threadState) shouldBe keyValue2

            val segmentState = threadState.getSegmentState(path).getS
            segmentState.keyValue._2 shouldBe keyValue2
            keyValue2.key.underlyingArraySize shouldBe 4
            segmentState.isSequential shouldBe true
          }

          /**
           * and then perform sequential read again but fails and reverts to random seek because the next key is random - keyValue100
           */
          {
            (segmentSearcher
              .searchSequential(
                _: Slice[Byte],
                _: PersistentOption,
                _: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                _: UnblockedReader[ValuesBlockOffset, ValuesBlock])(_: KeyOrder[Slice[Byte]], _: KeyOrder[Persistent.Partial])
              )
              .expects(*, *, *, *, *, *)
              .onCall {
                (key: Slice[Byte], startFrom: PersistentOption, _, _, _, _) =>
                  key shouldBe keyValue100.key
                  startFrom shouldBe keyValue2
                  Persistent.Null
              }

            (segmentSearcher
              .searchRandom(
                _: Slice[Byte],
                _: PersistentOption,
                _: PersistentOption,
                _: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
                _: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                _: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                _: UnblockedReader[ValuesBlockOffset, ValuesBlock],
                _: Boolean,
                _: Int)(_: KeyOrder[Slice[Byte]], _: KeyOrder[Persistent.Partial]))
              .expects(*, *, *, *, *, *, *, *, *, *, *)
              .onCall {
                result =>
                  result match {
                    case (key: Slice[Byte], startFrom: PersistentOption, _, _, _, _, _, hasRange: Boolean, keyValueCount: Function0[Int], _, _) =>
                      key shouldBe keyValue100.key
                      startFrom shouldBe keyValue2
                      hasRange shouldBe keyValues.exists(_.isRange)
                      keyValueCount() shouldBe keyValues.size
                      keyValue100
                  }
              }

            SegmentRefReader.get(key = keyValue100.key, threadState = threadState) shouldBe keyValue100

            val segmentState = threadState.getSegmentState(path).getS
            segmentState.keyValue._2 shouldBe keyValue100
            segmentState.isSequential shouldBe false
          }

          /**
           * and then perform random read to start with and set sequential to true - keyValue101
           */
          {
            //101 is sequential to 100 - isSequential will be true.
            (segmentSearcher
              .searchRandom(
                _: Slice[Byte],
                _: PersistentOption,
                _: PersistentOption,
                _: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
                _: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                _: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                _: UnblockedReader[ValuesBlockOffset, ValuesBlock],
                _: Boolean,
                _: Int)(_: KeyOrder[Slice[Byte]], _: KeyOrder[Persistent.Partial]))
              .expects(*, *, *, *, *, *, *, *, *, *, *)
              .onCall {
                result =>
                  result match {
                    case (key: Slice[Byte], startFrom: PersistentOption, _, _, _, _, _, hasRange: Boolean, keyValueCount: Function0[Int], _, _) =>
                      key shouldBe keyValue101.key
                      startFrom shouldBe keyValue100
                      hasRange shouldBe keyValues.exists(_.isRange)
                      keyValueCount() shouldBe keyValues.size
                      keyValue101
                  }
              }

            SegmentRefReader.get(key = keyValue101.key, threadState = threadState) shouldBe keyValue101

            val segmentState = threadState.getSegmentState(path).getS
            segmentState.keyValue._2 shouldBe keyValue101
            segmentState.isSequential shouldBe true
          }

          /**
           * and then perform sequential read to start with but fail because keyValue3 is not sequential to keyValue101 - keyValue3
           */
          {
            (segmentSearcher
              .searchSequential(
                _: Slice[Byte],
                _: PersistentOption,
                _: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                _: UnblockedReader[ValuesBlockOffset, ValuesBlock])(_: KeyOrder[Slice[Byte]], _: KeyOrder[Persistent.Partial])
              )
              .expects(*, *, *, *, *, *)
              .onCall {
                (key: Slice[Byte], startFrom: PersistentOption, _, _, _, _) =>
                  key shouldBe keyValue3.key
                  //start from is None because cached keyValue10's key > keyValue3's key
                  startFrom.isNoneS shouldBe true
                  Persistent.Null
              }

            (segmentSearcher
              .searchRandom(
                _: Slice[Byte],
                _: PersistentOption,
                _: PersistentOption,
                _: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
                _: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                _: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                _: UnblockedReader[ValuesBlockOffset, ValuesBlock],
                _: Boolean,
                _: Int)(_: KeyOrder[Slice[Byte]], _: KeyOrder[Persistent.Partial]))
              .expects(*, *, *, *, *, *, *, *, *, *, *)
              .onCall {
                result =>
                  result match {
                    case (key: Slice[Byte], startFrom: PersistentOption, _, _, _, _, _, hasRange: Boolean, keyValueCount: Function0[Int], _, _) =>
                      key shouldBe keyValue3.key
                      //start from is None because cached keyValue10's key > keyValue3's key
                      startFrom.isNoneS shouldBe true
                      hasRange shouldBe keyValues.exists(_.isRange)
                      keyValueCount() shouldBe keyValues.size
                      keyValue3
                  }
              }

            SegmentRefReader.get(key = keyValue3.key, threadState = threadState) shouldBe keyValue3

            val segmentState = threadState.getSegmentState(path).getS
            segmentState.keyValue._2 shouldBe keyValue3
            segmentState.isSequential shouldBe false
          }
      }
    }
  }
}
