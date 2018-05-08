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
import swaydb.core.data.{Memory, Transient, Value}
import swaydb.core.segment.KeyValueMerger
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class SegmentMerger_Fixed_Into_Range extends WordSpec with CommonAssertions {

  implicit val ordering = KeyOrder.default

  "Single into Range" when {
    "left" in {
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
          val newKeyValue = randomFixedKeyValue(1)
          val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
          val expectedFromValue = KeyValueMerger.applyValue(newKeyValue, oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue), atLeastTimeLeft).get
          val expectedKeyValue = Memory.Range(1, 10, expectedFromValue, oldKeyValue.rangeValue)
          val expectedLastLevel = expectedFromValue.toExpectedLastLevelKeyValue(newKeyValue.key)

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: " + expectedKeyValue)

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastLevel,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "mid" in {
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
          val newKeyValue = randomFixedKeyValue(5)
          val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
          val expectedFromValue = KeyValueMerger.applyValue(newKeyValue, oldKeyValue.rangeValue, atLeastTimeLeft).get
          val expectedKeyValue =
            Slice(
              oldKeyValue.copy(fromKey = 1, toKey = 5),
              Memory.Range(5, 10, expectedFromValue, oldKeyValue.rangeValue)
            )

          val expectedLastLevelFromLowerSplit = oldKeyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(oldKeyValue.key))
          val expectedLastLevelFromUpperSplit = expectedFromValue.toExpectedLastLevelKeyValue(newKeyValue.key)

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "right" in {
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
          val newKeyValue = randomFixedKeyValue(10)
          val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
          val expectedKeyValue = Slice(oldKeyValue, newKeyValue)

          val expectedLastLevelFromLowerSplit = oldKeyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(oldKeyValue.key))

          val expectedLastLevelFromUpperSplit = if (newKeyValue.isExpectedInLastLevel) Some(newKeyValue) else None

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "split the range if the input key-values key overlaps range's multiple keys (random mix test)" in {
      val deadline1 = 20.seconds.fromNow
      val deadline2 = 30.seconds.fromNow

      //9, 10, 11, 15, 18,    23,      27,  30
      //   10      -     20        25   -   30
      val newKeyValues: Slice[Memory] =
      Slice(
        Memory.Put(9, 9),
        Memory.Put(10, 10),
        Memory.Put(11, 11, deadline1),
        Memory.Remove(15),
        Memory.Put(18, 18),
        Memory.Remove(21),
        Memory.Put(23, 23),
        Memory.Remove(25),
        Memory.Put(27, 27),
        Memory.Put(30, 30)
      )

      val oldKeyValues: Slice[Memory] =
        Slice(
          Memory.Range(10, 20, Option.empty[Value.Put], Value.Update("ranges value 1")),
          Memory.Range(25, 30, Some(Value.Put(25)), Value.Update("ranges value 2", deadline2))
        )

      val expected: Slice[Memory] =
        Slice(
          Memory.Put(9, 9),
          Memory.Range(10, 11, Some(Value.Put(10)), Value.Update("ranges value 1")),
          Memory.Range(11, 15, Some(Value.Put(11, deadline1)), Value.Update("ranges value 1")),
          Memory.Range(15, 18, Some(Value.Remove(None)), Value.Update("ranges value 1")),
          Memory.Range(18, 20, Some(Value.Put(18)), Value.Update("ranges value 1")),
          Memory.Remove(21),
          Memory.Put(23, 23),
          Memory.Range(25, 27, Some(Value.Remove(None)), Value.Update("ranges value 2", deadline2)),
          Memory.Range(27, 30, Some(Value.Put(27)), Value.Update("ranges value 2", deadline2)),
          Memory.Put(30, 30)
        )

      //last level check
      val expectedInLastLevel: Slice[Memory] =
        Slice(
          Memory.Put(9, 9),
          Memory.Put(10, 10),
          Memory.Put(11, 11, deadline1),
          Memory.Put(18, 18),
          Memory.Put(23, 23),
          Memory.Put(27, 27),
          Memory.Put(30, 30)
        )

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = expected,
        lastLevelExpect = expectedInLastLevel,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

}
