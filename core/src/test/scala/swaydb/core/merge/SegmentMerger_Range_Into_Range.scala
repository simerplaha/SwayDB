/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.merge

import org.scalatest.WordSpec
import swaydb.core.CommonAssertions
import swaydb.core.data.Memory
import swaydb.core.segment.merge.KeyValueMerger
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class SegmentMerger_Range_Into_Range extends WordSpec with CommonAssertions {

  override implicit val ordering = KeyOrder.default
  implicit val compression = groupingStrategy

  "Range into Range" when {
    "1" in {
      //1   -     15
      //      10   -  20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
          val newKeyValue = Memory.Range(1, 15, randomFromValueOption(), randomRangeValue())
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
                toKey = 15,
                fromValue = oldKeyValue.fromValue.map(KeyValueMerger.applyValue(newKeyValue.rangeValue, _, atLeastTimeLeft).assertGet),
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              ),
              Memory.Range(
                fromKey = 15,
                toKey = 20,
                fromValue = None,
                rangeValue = oldKeyValue.rangeValue
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "2" in {
      //1      -      20
      //      10   -  20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
                fromValue = oldKeyValue.fromValue.map(KeyValueMerger.applyValue(newKeyValue.rangeValue, _, atLeastTimeLeft).assertGet),
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "3" in {
      //1      -         21
      //      10   -  20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
                fromValue = oldKeyValue.fromValue.map(KeyValueMerger.applyValue(newKeyValue.rangeValue, _, atLeastTimeLeft).assertGet),
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              ),
              Memory.Range(
                fromKey = 20,
                toKey = 21,
                fromValue = None,
                rangeValue = newKeyValue.rangeValue
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "4" in {
      //      10 - 15
      //      10   -  20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
          val newKeyValue = Memory.Range(10, 15, randomFromValueOption(), randomRangeValue())
          val oldKeyValue = Memory.Range(10, 20, randomFromValueOption(), randomRangeValue())

          val expectedKeyValue =
            Slice(
              Memory.Range(
                fromKey = 10,
                toKey = 15,
                fromValue =
                  //different approach used pattern matching instead of using similar approach just like in SegmentMerge.scala
                  oldKeyValue.fromValue match {
                    case Some(oldFromValue) =>
                      Some(KeyValueMerger.applyValue(newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue), oldFromValue, atLeastTimeLeft).assertGet)
                    case None =>
                      newKeyValue.fromValue match {
                        case Some(newFromValue) =>
                          Some(KeyValueMerger.applyValue(newFromValue, oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue), atLeastTimeLeft).assertGet)
                        case None =>
                          None
                      }
                  },
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              ),
              Memory.Range(
                fromKey = 15,
                toKey = 20,
                fromValue = None,
                rangeValue = oldKeyValue.rangeValue
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "5" in {
      //      10   -  20
      //      10   -  20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
                    case Some(oldFromValue) =>
                      Some(KeyValueMerger.applyValue(newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue), oldFromValue, atLeastTimeLeft).assertGet)
                    case None =>
                      newKeyValue.fromValue match {
                        case Some(newFromValue) =>
                          Some(KeyValueMerger.applyValue(newFromValue, oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue), atLeastTimeLeft).assertGet)
                        case None =>
                          None
                      }
                  },
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }


    "6" in {
      //      10   -     21
      //      10   -  20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
                    case Some(oldFromValue) =>
                      Some(KeyValueMerger.applyValue(newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue), oldFromValue, atLeastTimeLeft).assertGet)
                    case None =>
                      newKeyValue.fromValue match {
                        case Some(newFromValue) =>
                          Some(KeyValueMerger.applyValue(newFromValue, oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue), atLeastTimeLeft).assertGet)
                        case None =>
                          None
                      }
                  },
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              ),
              Memory.Range(
                fromKey = 20,
                toKey = 21,
                fromValue = None,
                rangeValue = newKeyValue.rangeValue
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "7" in {
      //        11 - 15
      //      10   -   20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
                fromValue = newKeyValue.fromValue.map(KeyValueMerger.applyValue(_, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet),
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              ),
              Memory.Range(
                fromKey = 15,
                toKey = 20,
                fromValue = None,
                rangeValue = oldKeyValue.rangeValue
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "8" in {
      //        11 -   20
      //      10   -   20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
                fromValue = newKeyValue.fromValue.map(KeyValueMerger.applyValue(_, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet),
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "9" in {
      //        11 -     21
      //      10   -   20
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
                fromValue = newKeyValue.fromValue.map(KeyValueMerger.applyValue(_, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet),
                rangeValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue.rangeValue, atLeastTimeLeft).assertGet
              ),
              Memory.Range(
                fromKey = 20,
                toKey = 21,
                fromValue = None,
                rangeValue = newKeyValue.rangeValue
              )
            )
          val expectedLastLevel = expectedKeyValue.flatMap(keyValue => keyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(keyValue.key))).toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("expectedLastLevel: \n" + expectedLastLevel.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

  }
}
