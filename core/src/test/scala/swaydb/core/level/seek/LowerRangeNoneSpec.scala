/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level.seek

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.KeyValue
import swaydb.core.level.LevelSeek
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class LowerRangeNoneSpec extends WordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

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
