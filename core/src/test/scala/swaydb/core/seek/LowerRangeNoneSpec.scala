/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.seek

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import swaydb.data.io.IO
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.IOAssert._
import swaydb.core.util.IOUtil
import swaydb.core.{TestData, TestTimeGenerator}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class LowerRangeNoneSpec extends WordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return None" when {
    implicit val timeGenerator = TestTimeGenerator.Decremental()

    "1" when {
      //0   ->   20
      //0 - 10
      //x
      "range value is not removed or expired and fromValue is None" in {
        runThis(100.times) {

          implicit val timeGenerator = TestTimeGenerator.Empty

          (0 to 20).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.lower _ expects (key: Slice[Byte]) returning IO(Some(randomRangeKeyValue(0, 10, randomFromValueOption(addPut = false), rangeValue = randomUpdateRangeValue())))
                next.lower    _ expects (key: Slice[Byte]) returning IOUtil.successNone
                current.lower _ expects (0: Slice[Byte]) returning IOUtil.successNone
                //@formatter:on
              }
              Lower(key: Slice[Byte]).assertGetOpt shouldBe empty
          }
        }
      }

      //0 ->10
      //0 - 10
      // 1-9
      "range value is removed or expired" in {
        //in this test lower level is read for upper Level's lower toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val timeGenerator = TestTimeGenerator.Empty

          (1 to 10).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.lower     _ expects (key: Slice[Byte]) returning IO(Some(randomRangeKeyValue(0, 10, Some(randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false)), randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false))))
                next.lower        _ expects (key: Slice[Byte]) returning IOUtil.successNone
                current.lower     _ expects (0: Slice[Byte]) returning IOUtil.successNone
                //@formatter:on
              }
              Lower(key: Slice[Byte]).assertGetOpt shouldBe empty
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

          implicit val timeGenerator = TestTimeGenerator.Empty

          (1 to 20).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.lower   _ expects (key: Slice[Byte]) returning IO(Some(randomRangeKeyValue(1, 10, randomFromValueOption(addPut = false), randomUpdateRangeValue())))
                next.lower      _ expects (key: Slice[Byte]) returning IOUtil.successNone
                current.lower   _ expects (1: Slice[Byte]) returning IOUtil.successNone
                //@formatter:on
              }
              Lower(key: Slice[Byte]).assertGetOpt shouldBe empty
          }
        }
      }

      //     10
      // 1 - 10
      // 1-9
      "range value is removed or expired" in {
        //in this test lower level is read for upper Level's lower toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val timeGenerator = TestTimeGenerator.Empty
          (2 to 10).reverse foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]
              inSequence {
                //@formatter:off
                current.lower   _ expects (key: Slice[Byte]) returning IO(Some(randomRangeKeyValue(1, 10, Some(randomRemoveOrUpdateOrFunctionRemoveValue()), randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false))))
                next.lower      _ expects (key: Slice[Byte]) returning IOUtil.successNone
                current.lower   _ expects (1: Slice[Byte]) returning IOUtil.successNone
                //@formatter:on
              }
              Lower(key: Slice[Byte]).assertGetOpt shouldBe empty
          }
        }
      }


      //        12
      // 1 - 10
      // 1-9
      "range value is removed or expired and fromKey has a gap with toKey" in {
        //in this test lower level is read for upper Level's lower toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val timeGenerator = TestTimeGenerator.Empty
          implicit val current = mock[CurrentWalker]
          implicit val next = mock[NextWalker]
          inSequence {
            //@formatter:off
            current.lower   _ expects (12: Slice[Byte]) returning IO(Some(randomRangeKeyValue(1, 10, Some(randomRemoveOrUpdateOrFunctionRemoveValue()), randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false))))
            next.lower      _ expects (12: Slice[Byte]) returning IOUtil.successNone
            current.lower   _ expects (1: Slice[Byte])  returning IOUtil.successNone
            //@formatter:on
          }
          Lower(12: Slice[Byte]).assertGetOpt shouldBe empty
        }
      }
    }
  }
}
