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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.merge

import org.scalatest.WordSpec
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.{Memory, Value}
import swaydb.core.merge.FixedMerger
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class SegmentMerger_Range_Into_Fixed extends WordSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def groupBy = randomGroupByOption(randomNextInt(1000))

  implicit val testTimer = TestTimer.Empty

  "Range into Single" when {
    "Left - when Single key-value matches Range's fromKey" in {
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = randomFixedKeyValue(1)

        val expectedFromValue =
          FixedMerger(
            newKeyValue = newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue).toMemory(oldKeyValue.key),
            oldKeyValue = oldKeyValue
          ).runRandomIO.value

        val expectedKeyValue =
          (newKeyValue.fromValue, newKeyValue.rangeValue) match {
            case (None, Value.Remove(None, _)) =>
              newKeyValue
            case (Some(Value.Put(_, None, _)), Value.Remove(None, _)) =>
              newKeyValue
            case (Some(Value.Remove(None, _)), Value.Remove(None, _)) =>
              newKeyValue
            case _ =>
              Memory.Range(1, 10, expectedFromValue.toFromValue().runRandomIO.value, newKeyValue.rangeValue)
          }

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: " + expectedKeyValue)

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedKeyValue.toLastLevelExpected
        )
      }
    }

    "Mid - When Single's key-value belongs to Range's fromKey to toKey" in {
      runThis(10000.times) {
        val midKey = Random.shuffle((2 to 9).toList).head
        val newKeyValue = Memory.Range(1, 10, randomFromValue(), randomRangeValue())
        val oldKeyValue = randomRemoveKeyValue(midKey)
        val merged = FixedMerger(newKeyValue.rangeValue.toMemory(midKey), oldKeyValue).runRandomIO.value

        val expectedKeyValue =
          newKeyValue.rangeValue match {
            case Value.Remove(None, _) =>
              Slice(newKeyValue)
            case _ =>
              Slice(
                newKeyValue.copy(fromKey = 1, toKey = midKey),
                Memory.Range(midKey, 10, merged.toFromValue().runRandomIO.value, newKeyValue.rangeValue)
              )
          }

        def expectedLastLevelFromLowerSplit = expectedKeyValue.head.fromValue.flatMap(_.toExpectedLastLevelKeyValue(expectedKeyValue.head.key))

        def expectedLastLevelFromUpperSplit = expectedKeyValue.last.fromValue.flatMap(_.toExpectedLastLevelKeyValue(expectedKeyValue.last.key))

        val lastLevelExpected =
          if (expectedKeyValue.size == 2)
            (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice
          else
            expectedLastLevelFromLowerSplit.toSeq.toSlice

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
        //        println("lastLevelExpected: \n" + lastLevelExpected.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = lastLevelExpected
        )
      }
    }

    "Right - When Single's key does not belong to the Range" in {
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = randomFixedKeyValue(10)
        val expectedKeyValue = Slice(newKeyValue, oldKeyValue)

        val expectedLastLevelFromLowerSplit = newKeyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(newKeyValue.key))

        val expectedLastLevelFromUpperSplit = if (oldKeyValue.isExpectedInLastLevel) Some(oldKeyValue) else None

        ////println
        ////println("newKeyValue: " + newKeyValue)
        ////println("oldKeyValue: " + oldKeyValue)
        ////println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice
        )
      }
    }

    "remove all key-values" in {
      //  0    -         25
      //    2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(0, 25, Some(Value.remove(None)), Value.remove(None)))

      val oldKeyValues: Slice[Memory.SegmentResponse] =
        Slice(
          Memory.put(2, "new value value"),
          Memory.put(7, "new value value"),
          Memory.remove(10),
          Memory.put(20, "new value value")
        )

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = newKeyValues,
        lastLevelExpect = Slice.empty
      )
    }

    "remove all key-values within the range only when range's last key does overlaps and existing key" in {
      implicit val testTimer = TestTimer.Empty

      //       3  -    20
      //  1, 2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(3, 20, None, Value.remove(None)))
      val oldKeyValues =
        Slice(
          Memory.remove(1),
          Memory.put(2, "new value value"),
          Memory.put(7, "new value value"),
          Memory.remove(10),
          Memory.put(20, "new value value")
        )

      val expected =
        Slice(
          Memory.remove(1),
          Memory.put(2, "new value value"),
          Memory.Range(3, 20, None, Value.remove(None)),
          Memory.put(20, "new value value")
        )

      val lastLevelExpected =
        Slice(
          Memory.put(2, "new value value"),
          Memory.put(20, "new value value")
        )

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = expected,
        lastLevelExpect = lastLevelExpected
      )
    }
  }

  "remove all key-values within the range only when range's keys overlaps and existing key" in {
    //     2    -    20
    //  1, 2, 7, 10, 20
    val newKeyValues = Slice(Memory.Range(2, 20, None, Value.remove(None)))
    val oldKeyValues =
      Slice(
        Memory.remove(1),
        Memory.put(2, "new value value"),
        Memory.put(7, "new value value"),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )

    val expected = Slice(
      Memory.remove(1),
      Memory.Range(2, 20, None, Value.remove(None)),
      Memory.put(20, "new value value")
    )

    val lastLevelExpected =
      Slice(
        Memory.put(20, "new value value")
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected
    )
  }

  "remove all key-values within the range only when range's first key does not overlap and existing key" in {
    // 1    -   8
    //     2, 7, 10, 20
    val newKeyValues = Slice(Memory.Range(1, 8, None, Value.remove(None)))

    val oldKeyValues =
      Slice(
        Memory.put(2, "new value value"),
        Memory.put(7, "new value value"),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )

    val expected =
      Slice(
        Memory.Range(1, 8, None, Value.remove(None)),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )

    val lastLevelExpected =
      Slice(
        Memory.put(20, "new value value")
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected
    )
  }

  "remove all key-values but keep the range's fromValue" in {
    // 1            -          100
    // 1,  7, 10, 20
    val newKeyValues = Slice(Memory.Range(1, 100, Some(Value.put(100)), Value.remove(None)))

    val oldKeyValues =
      Slice(
        Memory.put(1, 1),
        Memory.put(7, "new value value"),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )

    val expected = Slice(Memory.Range(1, 100, Some(Value.put(100)), Value.remove(None)))

    val lastLevelExpected =
      Slice(
        Memory.put(1, 100)
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected
    )
  }

  "update all key-values within the range only when range's first key does not overlap and existing key" in {
    // 1     -     15
    //     2, 7, 10, 20
    val newKeyValues = Slice(Memory.Range(1, 15, None, Value.update(15)))
    val oldKeyValues =
      Slice(
        Memory.put(2, "new value value"),
        Memory.put(7, "new value value"),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )
    val expected =
      Slice(
        Memory.Range(1, 2, None, Value.update(15)),
        Memory.Range(2, 7, Some(Value.put(15)), Value.update(15)),
        Memory.Range(7, 10, Some(Value.put(15)), Value.update(15)),
        Memory.Range(10, 15, Some(Value.remove(None)), Value.update(15)),
        Memory.put(20, "new value value")
      )

    val lastLevelExpected = Slice(
      Memory.put(2, 15),
      Memory.put(7, 15),
      Memory.put(20, "new value value")
    )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected
    )
  }

  "update all key-values within the range only when range's last key does not overlap and existing key" in {
    //       6     -     30
    //     2, 7, 10, 20  30, 31
    val newKeyValues = Slice(Memory.Range(6, 30, None, Value.update("updated")))

    val oldKeyValues =
      Slice(
        Memory.put(2, "new value"),
        Memory.put(7, "new value"),
        Memory.remove(10),
        Memory.put(20, "new value"),
        Memory.put(30, "new value"),
        Memory.remove(31)
      )
    val expected = Slice(
      Memory.put(2, "new value"),
      Memory.Range(6, 7, None, Value.update("updated")),
      Memory.Range(7, 10, Some(Value.put("updated")), Value.update("updated")),
      Memory.Range(10, 20, Some(Value.remove(None)), Value.update("updated")),
      Memory.Range(20, 30, Some(Value.put("updated")), Value.update("updated")),
      Memory.put(30, "new value"),
      Memory.remove(31)
    )

    val lastLevelExpected =
      Slice(
        Memory.put(2, "new value"),
        Memory.put(7, "updated"),
        Memory.put(20, "updated"),
        Memory.put(30, "new value")
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected
    )
  }

  "update all key-values when range's last key does not overlap and existing key" in {
    //     2         -           40
    //     2, 7, 10, 20  30, 31
    val newKeyValues = Slice(Memory.Range(2, 40, Some(Value.remove(None)), Value.update("updated")))
    val oldKeyValues =
      Slice(
        Memory.put(2, "old value"),
        Memory.put(7, "old value"),
        Memory.remove(10),
        Memory.put(20, "old value"),
        Memory.put(30, "old value"),
        Memory.remove(31)
      )
    val expected = Slice(
      Memory.Range(2, 7, Some(Value.remove(None)), Value.update("updated")),
      Memory.Range(7, 10, Some(Value.put("updated")), Value.update("updated")),
      Memory.Range(10, 20, Some(Value.remove(None)), Value.update("updated")),
      Memory.Range(20, 30, Some(Value.put("updated")), Value.update("updated")),
      Memory.Range(30, 31, Some(Value.put("updated")), Value.update("updated")),
      Memory.Range(31, 40, Some(Value.remove(None)), Value.update("updated"))
    )

    val lastLevelExpected = Slice(
      Memory.put(7, "updated"),
      Memory.put(20, "updated"),
      Memory.put(30, "updated")
    )
    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected
    )
  }

  "update all key-values when there are multiple new ranges" in {
    //     2    -   11,       31  -   51
    //  1, 2, 7, 10,   20  30, 35, 50,  53, 80
    val newKeyValues =
    Slice(
      Memory.Range(2, 11, None, Value.update("updated")),
      Memory.Range(31, 51, Some(Value.remove(None)), Value.update("updated 2"))
    )

    val oldKeyValues =
      Slice(
        Memory.put(1, "old value"),
        Memory.put(2, "old value"),
        Memory.put(7, "old value"),
        Memory.remove(10),
        Memory.put(20, "old value"),
        Memory.remove(30),
        Memory.remove(35),
        Memory.put(50, "old value"),
        Memory.put(53, "old value"),
        Memory.put(80, "old value")
      )

    val expected = Slice(
      Memory.put(1, "old value"),
      Memory.Range(2, 7, Some(Value.put("updated")), Value.update("updated")),
      Memory.Range(7, 10, Some(Value.put("updated")), Value.update("updated")),
      Memory.Range(10, 11, Some(Value.remove(None)), Value.update("updated")),
      Memory.put(20, "old value"),
      Memory.remove(30),
      Memory.Range(31, 35, Some(Value.remove(None)), Value.update("updated 2")),
      Memory.Range(35, 50, Some(Value.remove(None)), Value.update("updated 2")),
      Memory.Range(50, 51, Some(Value.put("updated 2")), Value.update("updated 2")),
      Memory.put(53, "old value"),
      Memory.put(80, "old value")
    )

    val lastLevelExpected = Slice(
      Memory.put(1, "old value"),
      Memory.put(2, "updated"),
      Memory.put(7, "updated"),
      Memory.put(20, "old value"),
      Memory.put(50, "updated 2"),
      Memory.put(53, "old value"),
      Memory.put(80, "old value")
    )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected
    )
  }
}
