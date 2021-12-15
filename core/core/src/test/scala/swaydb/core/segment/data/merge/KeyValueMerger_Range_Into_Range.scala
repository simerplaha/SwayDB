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

package swaydb.core.segment.data.merge

import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.TestTimer
import swaydb.core.segment.data.Value.FromValueOption
import swaydb.core.segment.data.{Memory, Value}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._

class KeyValueMerger_Range_Into_Range extends AnyWordSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val testTimer = TestTimer.Empty

  "Range into Range" when {
    "1" in {
      //1   -     15
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 15, randomFromValue(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValue(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 1,
              toKey = 10,
              fromValue = newKeyValue.fromValue,
              rangeValue = newKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 10,
              toKey = 15,
              fromValue = oldKeyValue.fromValue.flatMapS(fromValue => FixedMerger(newKeyValue.rangeValue.toMemory(10), fromValue.toMemory(10)).toFromValue()),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            ),
            Memory.Range(
              fromKey = 15,
              toKey = 20,
              fromValue = Value.FromValue.Null,
              rangeValue = oldKeyValue.rangeValue
            )
          )

        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(keyValue.key)))

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //        println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "2" in {
      //1      -      20
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 20, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 1,
              toKey = 10,
              fromValue = newKeyValue.fromValue,
              rangeValue = newKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue = oldKeyValue.fromValue.flatMapS(oldFromValue => FixedMerger(newKeyValue.rangeValue.toMemory(10), oldFromValue.toMemory(10)).toFromValue()),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            )
          )

        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "3" in {
      //1      -         21
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 21, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 1,
              toKey = 10,
              fromValue = newKeyValue.fromValue,
              rangeValue = newKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue = oldKeyValue.fromValue.flatMapS(oldFromValue => FixedMerger(newKeyValue.rangeValue.toMemory(10), oldFromValue.toMemory(10)).toFromValue()),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            ),
            Memory.Range(
              fromKey = 20,
              toKey = 21,
              fromValue = Value.FromValue.Null,
              rangeValue = newKeyValue.rangeValue
            )
          )

        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "4" in {
      //      10 - 15
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(10, 15, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        //different approach used pattern matching instead of using similar approach just like in SegmentMerge.scala
        val from: FromValueOption =
          oldKeyValue.fromValue match {
            case oldFromValue: Value.FromValue =>
              FixedMerger(newKeyValue.fromValue.getOrElseS(newKeyValue.rangeValue).toMemory(10), oldFromValue.toMemory(10)).toFromValue()
            case Value.FromValue.Null =>
              newKeyValue.fromValue match {
                case newFromValue: Value.FromValue =>
                  FixedMerger(newFromValue.toMemory(10), oldKeyValue.fromValue.getOrElseS(oldKeyValue.rangeValue).toMemory(10)).toFromValue()
                case Value.FromValue.Null =>
                  Value.FromValue.Null
              }
          }

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 15,
              fromValue = from,
              rangeValue = ValueMerger(newKeyValue.rangeValue: Value.RangeValue, oldKeyValue.rangeValue: Value.RangeValue).runRandomIO.right.value
            ),
            Memory.Range(
              fromKey = 15,
              toKey = 20,
              fromValue = Value.FromValue.Null,
              rangeValue = oldKeyValue.rangeValue
            )
          )
        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "5" in {
      //      10   -  20
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue =
              //different approach used pattern matching instead of using similar approach like in SegmentMerge.scala
                oldKeyValue.fromValue match {
                  case oldFromValue: Value.FromValue =>
                    FixedMerger(newKeyValue.fromValue.getOrElseS(newKeyValue.rangeValue).toMemory(10), oldFromValue.toMemory(10)).toFromValue()
                  case Value.FromValue.Null =>
                    newKeyValue.fromValue match {
                      case newFromValue: Value.FromValue =>
                        FixedMerger(newFromValue.toMemory(10), oldKeyValue.fromValue.getOrElseS(oldKeyValue.rangeValue).toMemory(10)).toFromValue()
                      case Value.FromValue.Null =>
                        Value.FromValue.Null
                    }
                },
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            )
          )
        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.toOptionS.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "6" in {
      //      10   -     21
      //      10   -  20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(10, 21, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 20,
              fromValue =
              //different approach used pattern matching instead of using similar approach just like in SegmentMerge.scala
                oldKeyValue.fromValue match {
                  case oldFromValue: Value.FromValue =>
                    FixedMerger(newKeyValue.fromValue.getOrElseS(newKeyValue.rangeValue).toMemory(10), oldFromValue.toMemory(10)).toFromValue()
                  case Value.FromValue.Null =>
                    newKeyValue.fromValue match {
                      case newFromValue: Value.FromValue =>
                        FixedMerger(newFromValue.toMemory(10), oldKeyValue.fromValue.getOrElseS(oldKeyValue.rangeValue).toMemory(10)).toFromValue()
                      case Value.FromValue.Null =>
                        Value.FromValue.Null
                    }
                },
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            ),
            Memory.Range(
              fromKey = 20,
              toKey = 21,
              fromValue = Value.FromValue.Null,
              rangeValue = newKeyValue.rangeValue
            )
          )
        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.toOptionS.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //        println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "7" in {
      //        11 - 15
      //      10   -   20
      runThis(10000.times) {
        val newKeyValue = Memory.Range(11, 15, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 11,
              fromValue = oldKeyValue.fromValue,
              rangeValue = oldKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 11,
              toKey = 15,
              fromValue = newKeyValue.fromValue.flatMapS(fromValue => FixedMerger(fromValue.toMemory(11), oldKeyValue.rangeValue.toMemory(11)).toFromValue()),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            ),
            Memory.Range(
              fromKey = 15,
              toKey = 20,
              fromValue = Value.FromValue.Null,
              rangeValue = oldKeyValue.rangeValue
            )
          )
        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "8" in {
      //        11 -   20
      //      10   -   20
      runThis(1000.times) {
        val newKeyValue = Memory.Range(11, 20, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 11,
              fromValue = oldKeyValue.fromValue,
              rangeValue = oldKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 11,
              toKey = 20,
              fromValue = newKeyValue.fromValue.flatMapS(fromValue => FixedMerger(fromValue.toMemory(11), oldKeyValue.rangeValue.toMemory(11)).toFromValue()),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            )
          )
        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "9" in {
      //        11 -     21
      //      10   -   20
      runThis(1000.times) {
        val newKeyValue = Memory.Range(11, 21, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

        val expectedKeyValue =
          Slice(
            Memory.Range(
              fromKey = 10,
              toKey = 11,
              fromValue = oldKeyValue.fromValue,
              rangeValue = oldKeyValue.rangeValue
            ),
            Memory.Range(
              fromKey = 11,
              toKey = 20,
              fromValue = newKeyValue.fromValue.flatMapS(fromValue => FixedMerger(fromValue.toMemory(11), oldKeyValue.rangeValue.toMemory(11)).toFromValue()),
              rangeValue = FixedMerger(newKeyValue.rangeValue.toMemory(Slice.emptyBytes), oldKeyValue.rangeValue.toMemory(Slice.emptyBytes)).toRangeValue()
            ),
            Memory.Range(
              fromKey = 20,
              toKey = 21,
              fromValue = Value.FromValue.Null,
              rangeValue = newKeyValue.rangeValue
            )
          )

        val expectedLastLevel: Slice[Memory.Fixed] =
          expectedKeyValue.flatMapToSliceSlow(keyValue => keyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(keyValue.key)).toList)

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }
  }
}
