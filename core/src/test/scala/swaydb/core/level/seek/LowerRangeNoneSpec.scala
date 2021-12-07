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

package swaydb.core.level.seek

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.core.CoreTestData._
import swaydb.core.level.LevelSeek
import swaydb.core.segment.data.KeyValue
import swaydb.core.{CoreTestData, TestTimer}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._

class LowerRangeNoneSpec extends AnyWordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = CoreTestData.functionStore

  "return None" when {
    implicit val testTimer = TestTimer.Decremental()

    "1" when {
      //0   ->   20
      //0 - 10
      //x
      "range value is not removed or expired and fromValue is None" in {
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty

          (0 to 20).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(0, 10, randomFromValueOption(addPut = false), rangeValue = randomUpdateRangeValue()))
                next.lower            _ expects (key: Slice[Byte], *)  returning KeyValue.Put.Null
                current.lower         _ expects (0: Slice[Byte], *)    returning LevelSeek.None
                //@formatter:on
              }
              Lower(key: Slice[Byte]).runRandomIO.right.value shouldBe empty
          }
        }
      }

      //0 ->10
      //0 - 10
      // 1-9
      "range value is removed or expired" in {
        //in this test lower level is read for upper Level's lower toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty

          (1 to 10).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(0, 10, randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false), randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false)))
                next.lower            _ expects (key: Slice[Byte], *)  returning KeyValue.Put.Null
                current.lower         _ expects (0: Slice[Byte], *)    returning LevelSeek.None
                //@formatter:on
              }
              Lower(key: Slice[Byte]).runRandomIO.right.value shouldBe empty
          }
        }
      }
    }

    "2" when {

      //        11
      //  1 - 10
      //  x
      "range value is not removed or expired" in {
        runThis(1.times) {

          implicit val testTimer = TestTimer.Empty

          (1 to 20).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(1, 10, randomFromValueOption(addPut = false), randomUpdateRangeValue()))
                next.lower            _ expects (key: Slice[Byte], *)  returning KeyValue.Put.Null
                current.lower         _ expects (1: Slice[Byte], *)    returning LevelSeek.None
                //@formatter:on
              }
              Lower(key: Slice[Byte]).runRandomIO.right.value shouldBe empty
          }
        }
      }

      //     10
      // 1 - 10
      // 1-9
      "range value is removed or expired" in {
        //in this test lower level is read for upper Level's lower toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty
          (2 to 10).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]
              inSequence {
                //@formatter:off
                current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(1, 10, randomRemoveOrUpdateOrFunctionRemoveValue(), randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false)))
                next.lower            _ expects (key: Slice[Byte], *)  returning KeyValue.Put.Null
                current.lower         _ expects (1: Slice[Byte], *)    returning LevelSeek.None
                //@formatter:on
              }
              Lower(key: Slice[Byte]).runRandomIO.right.value shouldBe empty
          }
        }
      }

      //        12
      // 1 - 10
      // 1-9
      "range value is removed or expired and fromKey has a gap with toKey" in {
        //in this test lower level is read for upper Level's lower toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty
          implicit val current = mock[CurrentWalker]
          implicit val next = mock[NextWalker]
          inSequence {
            //@formatter:off
            current.lower         _ expects (12: Slice[Byte], *)   returning LevelSeek.Some(1, randomRangeKeyValue(1, 10, randomRemoveOrUpdateOrFunctionRemoveValue(), randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false)))
            next.lower            _ expects (12: Slice[Byte], *)   returning KeyValue.Put.Null
            current.lower         _ expects (1: Slice[Byte], *)    returning LevelSeek.None
            //@formatter:on
          }
          Lower(12: Slice[Byte]).runRandomIO.right.value shouldBe empty
        }
      }
    }
  }
}
