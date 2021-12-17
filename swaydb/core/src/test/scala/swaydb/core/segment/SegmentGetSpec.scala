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

package swaydb.core.segment

import org.scalatest.OptionValues._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.CoreTestSweeper._
import swaydb.core.segment.data._
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.segment.SegmentTestKit._
import swaydb.core.segment.cache.sweeper.MemorySweeperTestKit
import swaydb.core.segment.ref.search.SegmentSearchTestKit._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._

import scala.util.Random

class SegmentGetSpec extends AnyWordSpec {

  implicit val keyOrder = KeyOrder.default

  val keyValuesCount: Int = 100

  "Segment.get" should {
    "fixed key-value" in {
      CoreTestSweeper.foreachRepeat(100.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues = Slice(randomFixedKeyValue(1)),

            assert =
              (keyValues, segment) =>
                Random.shuffle(
                  Seq(
                    () => segment.get(0, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(2, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(keyValues.head.key, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                  )
                ).foreach(_ ())
          )

          assertSegment(
            keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)),

            assert =
              (keyValues, segment) =>
                Random.shuffle(
                  Seq(
                    () => segment.get(0, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(3, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(keyValues.head.key, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                  )
                ).foreach(_ ())
          )
      }
    }

    "range-value" in {
      CoreTestSweeper.foreachRepeat(100.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          assertSegment(
            keyValues = Slice(randomRangeKeyValue(1, 10)),

            assert =
              (keyValues, segment) =>
                Random.shuffle(
                  Seq(
                    () => segment.get(0, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(10, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(11, ThreadReadState.random).toOption shouldBe empty,
                    () =>
                      (1 to 9) foreach {
                        i =>
                          segment.get(i, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                      }
                  )
                ).foreach(_ ())
          )

          assertSegment(
            keyValues =
              Slice(randomRangeKeyValue(1, 10), randomRangeKeyValue(10, 20)),

            assert =
              (keyValues, segment) =>
                Random.shuffle(
                  Seq(
                    () => segment.get(0, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(20, ThreadReadState.random).toOption shouldBe empty,
                    () => segment.get(21, ThreadReadState.random).toOption shouldBe empty,
                    () =>
                      (1 to 9) foreach {
                        i =>
                          segment.get(i, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                      },
                    () => {
                      val readState = ThreadReadState.random
                      (10 to 19) foreach {
                        i =>
                          segment.get(i, readState).getUnsafe shouldBe keyValues.last
                      }
                    }
                  )
                ).foreach(_ ())
          )
      }
    }

    "value random key-values" in {
      CoreTestSweeper.foreachRepeat(100.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          val keyValues = randomizedKeyValues(keyValuesCount)
          val segment = TestSegment(keyValues)
          assertGet(keyValues, segment)
      }
    }

    "add cut key-values to Segment's caches" in {
      CoreTestSweeper.foreachRepeat(100.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          MemorySweeperTestKit.createMemorySweeperMax().value.sweep()

          assertSegment(
            keyValues = randomizedKeyValues(keyValuesCount),

            testAgainAfterAssert = false,

            assert =
              (keyValues, segment) =>
                (0 until keyValues.size) foreach {
                  index =>
                    val keyValue = keyValues(index)
                    if (specType.isPersistent) segment.getFromCache(keyValue.key).toOption shouldBe empty
                    segment.get(keyValue.key, ThreadReadState.random).getUnsafe shouldBe keyValue

                    val gotFromCache = eventually(segment.getFromCache(keyValue.key).getUnsafe)
                    //underlying array sizes should not be slices but copies of arrays.
                    gotFromCache.key.underlyingArraySize shouldBe keyValue.key.toArray.length

                    gotFromCache match {
                      case range: KeyValue.Range =>
                        //if it's a range, toKey should also be cut.
                        range.toKey.underlyingArraySize shouldBe keyValues.find(_.key == range.fromKey).value.key.toArray.length

                      case _ =>
                        gotFromCache.getOrFetchValue.map(_.underlyingArraySize) shouldBe keyValue.getOrFetchValue.map(_.toArray.length)
                    }
                }
          )
      }
    }

    "add read key values to cache" in {
      CoreTestSweeper.foreachRepeat(100.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          runThis(20.times, log = true) {
            assertSegment(
              keyValues =
                randomizedKeyValues(keyValuesCount),

              testAgainAfterAssert =
                false,

              assert =
                (keyValues, segment) => {
                  val readState = ThreadReadState.random

                  keyValues foreach {
                    keyValue =>
                      if (specType.isPersistent) segment isInKeyValueCache keyValue.key shouldBe false
                      segment.get(keyValue.key, readState).getUnsafe shouldBe keyValue
                      segment isInKeyValueCache keyValue.key shouldBe true
                  }
                }
            )
          }
      }
    }

    //    "read value from a closed ValueReader" in {
    //      runThis(100.times) {
    //        assertSegment(
    //          keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)),
    //          assert =
    //            (keyValues, segment) =>
    //              keyValues foreach {
    //                keyValue =>
    //                  val readKeyValue = segment.get(keyValue.key).runIO
    //                  segment.close.runIO
    //                  readKeyValue.getOrFetchValue shouldBe keyValue.getOrFetchValue
    //              }
    //        )
    //      }
    //    }
    //
    //    "lazily load values" in {
    //      runThis(100.times) {
    //        assertSegment(
    //          keyValues = randomizedKeyValues(keyValuesCount),
    //          testAgainAfterAssert = false,
    //          assert =
    //            (keyValues, segment) =>
    //              keyValues foreach {
    //                keyValue =>
    //                  val readKeyValue = segment.get(keyValue.key).runIO
    //
    //                  readKeyValue match {
    //                    case persistent: Persistent.Remove =>
    //                      //remove has no value so isValueDefined will always return true
    //                      persistent.isValueDefined shouldBe true
    //
    //                    case persistent: Persistent =>
    //                      persistent.isValueDefined shouldBe false
    //
    //                    case _: Memory =>
    //                    //memory key-values always have values defined
    //                  }
    //                  //read the value
    //                  readKeyValue match {
    //                    case range: KeyValue.Range =>
    //                      range.fetchFromAndRangeValue.runIO
    //                    case _ =>
    //                      readKeyValue.getOrFetchValue shouldBe keyValue.getOrFetchValue
    //                  }
    //
    //                  //value is now set
    //                  readKeyValue match {
    //                    case persistent: Persistent =>
    //                      persistent.isValueDefined shouldBe true
    //
    //                    case _: Memory =>
    //                    //memory key-values always have values defined
    //                  }
    //              }
    //        )
    //      }
    //    }
  }
}
