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
import swaydb.core.data.Value.Remove
import swaydb.core.data.{Memory, Value}
import swaydb.core.segment.KeyValueMerger
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class SegmentMerger_Range_Into_Fixed extends WordSpec with CommonAssertions {

  implicit val ordering = KeyOrder.default

  "Range into Single" when {
    "Left - when Single key-value matches Range's fromKey" in {
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
          val newKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
          val oldKeyValue = randomFixedKeyValue(1)

          val (expectedKeyValue: Memory.Range, expectedLastKeyValue: Option[Memory.Fixed]) =
            newKeyValue.fetchFromAndRangeValue.assertGet match {
              //if the input range is a remove range, old key-values get dropped instead of doing a split.
              case (None | Some(Value.Remove(None)), Value.Remove(None)) =>
                (newKeyValue, None)

              case (Some(Value.Put(value, deadline)), Value.Remove(None)) =>
                if (deadline.forall(_.hasTimeLeft()))
                  (newKeyValue, Some(Memory.Put(newKeyValue.key, value, deadline)))
                else
                  (newKeyValue, None)

              case _ =>
                val expectedFromValue =
                  KeyValueMerger.applyValue(
                    newValue = newKeyValue.fromValue.getOrElse(newKeyValue.rangeValue),
                    oldValue = oldKeyValue,
                    hasTimeLeftAtLeast = atLeastTimeLeft
                  ).assertGet

                val expectedKeyValue = Memory.Range(1, 10, expectedFromValue, newKeyValue.rangeValue)
                val expectedLastLevel = expectedFromValue.toExpectedLastLevelKeyValue(newKeyValue.key)
                (expectedKeyValue, expectedLastLevel)
            }

          //
          //          //println
          //          //println("newKeyValue: " + newKeyValue)
          //          //println("oldKeyValue: " + oldKeyValue)
          //          //println("expectedKeyValue: " + expectedKeyValue)

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = expectedLastKeyValue,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "Mid - When Single's key-value belongs to Range's fromKey to toKey" in {
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
          val newKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
          val oldKeyValue = randomFixedKeyValue(5)
          val expectedFromValue = KeyValueMerger.applyValue(newKeyValue.rangeValue, oldKeyValue, atLeastTimeLeft).assertGet
          val expectedKeyValue =
            newKeyValue.rangeValue match {
              case Remove(None) =>
                Slice(newKeyValue)
              case _ =>
                Slice(
                  newKeyValue.copy(fromKey = 1, toKey = 5),
                  Memory.Range(5, 10, expectedFromValue, newKeyValue.rangeValue)
                )
            }

          def expectedLastLevelFromLowerSplit = expectedKeyValue.head.fromValue.flatMap(_.toExpectedLastLevelKeyValue(expectedKeyValue.head.key))

          def expectedLastLevelFromUpperSplit = expectedKeyValue.last.fromValue.flatMap(_.toExpectedLastLevelKeyValue(expectedKeyValue.last.key))

          val lastLevelExpected =
            if (expectedKeyValue.size == 2)
              (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice
            else
              expectedLastLevelFromLowerSplit.toSeq.toSlice

          //println
          //println("newKeyValue: " + newKeyValue)
          //println("oldKeyValue: " + oldKeyValue)
          //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))
          //println("lastLevelExpected: \n" + lastLevelExpected.mkString("\n"))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expectedKeyValue,
            lastLevelExpect = lastLevelExpected,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "Right - When Single's key does not belong to the Range" in {
      val atLeastTimeLeft = 10.seconds
      (1 to 10000) foreach {
        _ =>
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
            lastLevelExpect = (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice,
            hasTimeLeftAtLeast = atLeastTimeLeft
          )
      }
    }

    "remove all key-values" in {
      //  0    -         25
      //    2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(0, 25, Some(Value.Remove(None)), Value.Remove(None)))

      val oldKeyValues: Slice[Memory] =
        Slice(
          Memory.Put(2, "new value value"),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )

      val expected =
        Slice(Memory.Range(0, 25, Some(Value.Remove(None)), Value.Remove(None)))

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = expected,
        lastLevelExpect = Slice.empty[Memory],
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "remove all key-values within the range only when range's last key does overlaps and existing key" in {
      //       3  -    20
      //  1, 2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(3, 20, None, Value.Remove(None)))
      val oldKeyValues =
        Slice(
          Memory.Remove(1),
          Memory.Put(2, "new value value"),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )

      val expected =
        Slice(
          Memory.Remove(1),
          Memory.Put(2, "new value value"),
          Memory.Range(3, 20, None, Value.Remove(None)),
          Memory.Put(20, "new value value")
        )

      val lastLevelExpected =
        Slice(
          Memory.Put(2, "new value value"),
          Memory.Put(20, "new value value")
        )

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = expected,
        lastLevelExpect = lastLevelExpected,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "remove all key-values within the range only when range's keys overlaps and existing key" in {
    //     2    -    20
    //  1, 2, 7, 10, 20
    val newKeyValues = Slice(Memory.Range(2, 20, None, Value.Remove(None)))
    val oldKeyValues =
      Slice(
        Memory.Remove(1),
        Memory.Put(2, "new value value"),
        Memory.Put(7, "new value value"),
        Memory.Remove(10),
        Memory.Put(20, "new value value")
      )

    val expected = Slice(
      Memory.Remove(1),
      Memory.Range(2, 20, None, Value.Remove(None)),
      Memory.Put(20, "new value value")
    )

    val lastLevelExpected =
      Slice(
        Memory.Put(20, "new value value")
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected,
      hasTimeLeftAtLeast = 10.seconds
    )
  }

  "remove all key-values within the range only when range's first key does not overlap and existing key" in {
    // 1    -   8
    //     2, 7, 10, 20
    val newKeyValues = Slice(Memory.Range(1, 8, None, Value.Remove(None)))

    val oldKeyValues =
      Slice(
        Memory.Put(2, "new value value"),
        Memory.Put(7, "new value value"),
        Memory.Remove(10),
        Memory.Put(20, "new value value")
      )

    val expected =
      Slice(
        Memory.Range(1, 8, None, Value.Remove(None)),
        Memory.Remove(10),
        Memory.Put(20, "new value value")
      )

    val lastLevelExpected =
      Slice(
        Memory.Put(20, "new value value")
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected,
      hasTimeLeftAtLeast = 10.seconds
    )
  }

  "remove all key-values but keep the range's fromValue" in {
    // 1            -          100
    // 1,  7, 10, 20
    val newKeyValues = Slice(Memory.Range(1, 100, Some(Value.Put(100)), Value.Remove(None)))

    val oldKeyValues =
      Slice(
        Memory.Put(1, 1),
        Memory.Put(7, "new value value"),
        Memory.Remove(10),
        Memory.Put(20, "new value value")
      )

    val expected = Slice(Memory.Range(1, 100, Some(Value.Put(100)), Value.Remove(None)))

    val lastLevelExpected =
      Slice(
        Memory.Put(1, 100)
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected,
      hasTimeLeftAtLeast = 10.seconds
    )
  }

  "update all key-values within the range only when range's first key does not overlap and existing key" in {
    // 1     -     15
    //     2, 7, 10, 20
    val newKeyValues = Slice(Memory.Range(1, 15, None, Value.Update(15)))
    val oldKeyValues =
      Slice(
        Memory.Put(2, "new value value"),
        Memory.Put(7, "new value value"),
        Memory.Remove(10),
        Memory.Put(20, "new value value")
      )
    val expected =
      Slice(
        Memory.Range(1, 2, None, Value.Update(15)),
        Memory.Range(2, 7, Some(Value.Put(15)), Value.Update(15)),
        Memory.Range(7, 10, Some(Value.Put(15)), Value.Update(15)),
        Memory.Range(10, 15, Some(Value.Remove(None)), Value.Update(15)),
        Memory.Put(20, "new value value")
      )

    val lastLevelExpected = Slice(
      Memory.Put(2, 15),
      Memory.Put(7, 15),
      Memory.Put(20, "new value value")
    )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected,
      hasTimeLeftAtLeast = 10.seconds
    )
  }

  "update all key-values within the range only when range's last key does not overlap and existing key" in {
    //       6     -     30
    //     2, 7, 10, 20  30, 31
    val newKeyValues = Slice(Memory.Range(6, 30, None, Value.Update("updated")))

    val oldKeyValues =
      Slice(
        Memory.Put(2, "new value"),
        Memory.Put(7, "new value"),
        Memory.Remove(10),
        Memory.Put(20, "new value"),
        Memory.Put(30, "new value"),
        Memory.Remove(31)
      )
    val expected = Slice(
      Memory.Put(2, "new value"),
      Memory.Range(6, 7, None, Value.Update("updated")),
      Memory.Range(7, 10, Some(Value.Put("updated")), Value.Update("updated")),
      Memory.Range(10, 20, Some(Value.Remove(None)), Value.Update("updated")),
      Memory.Range(20, 30, Some(Value.Put("updated")), Value.Update("updated")),
      Memory.Put(30, "new value"),
      Memory.Remove(31)
    )

    val lastLevelExpected =
      Slice(
        Memory.Put(2, "new value"),
        Memory.Put(7, "updated"),
        Memory.Put(20, "updated"),
        Memory.Put(30, "new value")
      )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected,
      hasTimeLeftAtLeast = 10.seconds
    )
  }

  "update all key-values when range's last key does not overlap and existing key" in {
    //     2         -           40
    //     2, 7, 10, 20  30, 31
    val newKeyValues = Slice(Memory.Range(2, 40, Some(Value.Remove(None)), Value.Update("updated")))
    val oldKeyValues =
      Slice(
        Memory.Put(2, "old value"),
        Memory.Put(7, "old value"),
        Memory.Remove(10),
        Memory.Put(20, "old value"),
        Memory.Put(30, "old value"),
        Memory.Remove(31)
      )
    val expected = Slice(
      Memory.Range(2, 7, Some(Value.Remove(None)), Value.Update("updated")),
      Memory.Range(7, 10, Some(Value.Put("updated")), Value.Update("updated")),
      Memory.Range(10, 20, Some(Value.Remove(None)), Value.Update("updated")),
      Memory.Range(20, 30, Some(Value.Put("updated")), Value.Update("updated")),
      Memory.Range(30, 31, Some(Value.Put("updated")), Value.Update("updated")),
      Memory.Range(31, 40, Some(Value.Remove(None)), Value.Update("updated"))
    )

    val lastLevelExpected = Slice(
      Memory.Put(7, "updated"),
      Memory.Put(20, "updated"),
      Memory.Put(30, "updated")
    )
    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected,
      hasTimeLeftAtLeast = 10.seconds
    )
  }

  "update all key-values when there are multiple new ranges" in {
    //     2    -   11,       31  -   51
    //  1, 2, 7, 10,   20  30, 35, 50,  53, 80
    val newKeyValues =
    Slice(
      Memory.Range(2, 11, None, Value.Update("updated")),
      Memory.Range(31, 51, Some(Value.Remove(None)), Value.Update("updated 2"))
    )

    val oldKeyValues =
      Slice(
        Memory.Put(1, "old value"),
        Memory.Put(2, "old value"),
        Memory.Put(7, "old value"),
        Memory.Remove(10),
        Memory.Put(20, "old value"),
        Memory.Remove(30),
        Memory.Remove(35),
        Memory.Put(50, "old value"),
        Memory.Put(53, "old value"),
        Memory.Put(80, "old value")
      )

    val expected = Slice(
      Memory.Put(1, "old value"),
      Memory.Range(2, 7, Some(Value.Put("updated")), Value.Update("updated")),
      Memory.Range(7, 10, Some(Value.Put("updated")), Value.Update("updated")),
      Memory.Range(10, 11, Some(Value.Remove(None)), Value.Update("updated")),
      Memory.Put(20, "old value"),
      Memory.Remove(30),
      Memory.Range(31, 35, Some(Value.Remove(None)), Value.Update("updated 2")),
      Memory.Range(35, 50, Some(Value.Remove(None)), Value.Update("updated 2")),
      Memory.Range(50, 51, Some(Value.Put("updated 2")), Value.Update("updated 2")),
      Memory.Put(53, "old value"),
      Memory.Put(80, "old value")
    )

    val lastLevelExpected = Slice(
      Memory.Put(1, "old value"),
      Memory.Put(2, "updated"),
      Memory.Put(7, "updated"),
      Memory.Put(20, "old value"),
      Memory.Put(50, "updated 2"),
      Memory.Put(53, "old value"),
      Memory.Put(80, "old value")
    )

    assertMerge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      expected = expected,
      lastLevelExpect = lastLevelExpected,
      hasTimeLeftAtLeast = 10.seconds
    )
  }


}
