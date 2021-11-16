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

package swaydb.core.merge

import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.{Memory, Value}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

import scala.util.Random

class KeyValueMerger_Range_Into_Fixed extends AnyWordSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val testTimer = TestTimer.Empty

  "Range into Single" when {
    "Left - when Single key-value matches Range's fromKey" in {
      runThis(10000.times) {
        val newKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val oldKeyValue = randomFixedKeyValue(1)

        val expectedFromValue =
          FixedMerger(
            newKeyValue = newKeyValue.fromValue.getOrElseS(newKeyValue.rangeValue).toMemory(oldKeyValue.key),
            oldKeyValue = oldKeyValue
          )

        val expectedKeyValue =
          (newKeyValue.fromValue, newKeyValue.rangeValue) match {
            case (Value.FromValue.Null, Value.Remove(None, _)) =>
              newKeyValue
            case (Value.Put(_, None, _), Value.Remove(None, _)) =>
              newKeyValue
            case (Value.Remove(None, _), Value.Remove(None, _)) =>
              newKeyValue
            case _ =>
              Memory.Range(1, 10, expectedFromValue.toFromValue(), newKeyValue.rangeValue)
          }

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: " + expectedKeyValue)

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedKeyValue.toLastLevelExpected getOrElse Memory.Null
        )
      }
    }

    "Mid - When Single's key-value belongs to Range's fromKey to toKey" in {
      runThis(10000.times) {
        val midKey = Random.shuffle((2 to 9).toList).head
        val newKeyValue = Memory.Range(1, 10, randomFromValue(), randomRangeValue())
        val oldKeyValue = randomRemoveKeyValue(midKey)
        val merged = FixedMerger(newKeyValue.rangeValue.toMemory(midKey), oldKeyValue)

        val expectedKeyValue =
          newKeyValue.rangeValue match {
            case Value.Remove(None, _) =>
              Slice(newKeyValue)
            case _ =>
              Slice(
                newKeyValue.copy(fromKey = 1, toKey = midKey),
                Memory.Range(midKey, 10, merged.toFromValue(), newKeyValue.rangeValue)
              )
          }

        def expectedLastLevelFromLowerSplit = expectedKeyValue.head.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(expectedKeyValue.head.key))

        def expectedLastLevelFromUpperSplit = expectedKeyValue.last.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(expectedKeyValue.last.key))

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

        val expectedLastLevelFromLowerSplit = newKeyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(newKeyValue.key))

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
      val newKeyValues = Slice(Memory.Range(0, 25, Value.remove(None), Value.remove(None)))

      val oldKeyValues: Slice[Memory] =
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
      val newKeyValues = Slice(Memory.Range(3, 20, Value.FromValue.Null, Value.remove(None)))
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
          Memory.Range(3, 20, Value.FromValue.Null, Value.remove(None)),
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
    val newKeyValues = Slice(Memory.Range(2, 20, Value.FromValue.Null, Value.remove(None)))
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
      Memory.Range(2, 20, Value.FromValue.Null, Value.remove(None)),
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
    val newKeyValues = Slice(Memory.Range(1, 8, Value.FromValue.Null, Value.remove(None)))

    val oldKeyValues =
      Slice(
        Memory.put(2, "new value value"),
        Memory.put(7, "new value value"),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )

    val expected =
      Slice(
        Memory.Range(1, 8, Value.FromValue.Null, Value.remove(None)),
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
    val newKeyValues = Slice(Memory.Range(1, 100, Value.put(100), Value.remove(None)))

    val oldKeyValues =
      Slice(
        Memory.put(1, 1),
        Memory.put(7, "new value value"),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )

    val expected = Slice(Memory.Range(1, 100, Value.put(100), Value.remove(None)))

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
    val newKeyValues = Slice(Memory.Range(1, 15, Value.FromValue.Null, Value.update(15)))
    val oldKeyValues =
      Slice(
        Memory.put(2, "new value value"),
        Memory.put(7, "new value value"),
        Memory.remove(10),
        Memory.put(20, "new value value")
      )
    val expected =
      Slice(
        Memory.Range(1, 2, Value.FromValue.Null, Value.update(15)),
        Memory.Range(2, 7, Value.put(15), Value.update(15)),
        Memory.Range(7, 10, Value.put(15), Value.update(15)),
        Memory.Range(10, 15, Value.remove(None), Value.update(15)),
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
    val newKeyValues = Slice(Memory.Range(6, 30, Value.FromValue.Null, Value.update("updated")))

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
      Memory.Range(6, 7, Value.FromValue.Null, Value.update("updated")),
      Memory.Range(7, 10, Value.put("updated"), Value.update("updated")),
      Memory.Range(10, 20, Value.remove(None), Value.update("updated")),
      Memory.Range(20, 30, Value.put("updated"), Value.update("updated")),
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
    val newKeyValues = Slice(Memory.Range(2, 40, Value.remove(None), Value.update("updated")))
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
      Memory.Range(2, 7, Value.remove(None), Value.update("updated")),
      Memory.Range(7, 10, Value.put("updated"), Value.update("updated")),
      Memory.Range(10, 20, Value.remove(None), Value.update("updated")),
      Memory.Range(20, 30, Value.put("updated"), Value.update("updated")),
      Memory.Range(30, 31, Value.put("updated"), Value.update("updated")),
      Memory.Range(31, 40, Value.remove(None), Value.update("updated"))
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
      Memory.Range(2, 11, Value.FromValue.Null, Value.update("updated")),
      Memory.Range(31, 51, Value.remove(None), Value.update("updated 2"))
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
      Memory.Range(2, 7, Value.put("updated"), Value.update("updated")),
      Memory.Range(7, 10, Value.put("updated"), Value.update("updated")),
      Memory.Range(10, 11, Value.remove(None), Value.update("updated")),
      Memory.put(20, "old value"),
      Memory.remove(30),
      Memory.Range(31, 35, Value.remove(None), Value.update("updated 2")),
      Memory.Range(35, 50, Value.remove(None), Value.update("updated 2")),
      Memory.Range(50, 51, Value.put("updated 2"), Value.update("updated 2")),
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
