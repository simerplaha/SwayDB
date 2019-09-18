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

import scala.concurrent.duration._
import scala.util.Random

class SegmentMerger_Fixed_Into_Range extends WordSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  "Single into Range" when {

    "left out" in {
      implicit val testTimer = TestTimer.Incremental()
      runThis(10000.times) {
        val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val newKeyValue = randomFixedKeyValue(0)

        val expectedKeyValue = Slice(newKeyValue, oldKeyValue)
        val expectedLastLevel: Slice[Memory.Fixed] = expectedKeyValue.map(_.toLastLevelExpected).flatten.toSlice

        //        println
        //        println("newKeyValue: " + newKeyValue)
        //        println("oldKeyValue: " + oldKeyValue)
        //        println("expectedKeyValue: " + expectedKeyValue)

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel.toSlice
        )
      }
    }

    "left" in {
      implicit val testTimer = TestTimer.Incremental()
      runThis(10000.times) {
        val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val newKeyValue = randomFixedKeyValue(1)
        val expectedFromValue = FixedMerger(newKeyValue, oldKeyValue.fromValue.getOrElse(oldKeyValue.rangeValue).toMemory(oldKeyValue.key)).get
        val expectedKeyValue = Memory.Range(1, 10, expectedFromValue.toFromValue().runRandomIO.right.value, oldKeyValue.rangeValue)
        val expectedLastLevel = expectedFromValue.asInstanceOf[Memory.Fixed].toLastLevelExpected

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: " + expectedKeyValue)

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel
        )
      }
    }

    "mid" in {
      runThis(10000.times) {
        implicit val testTimer = TestTimer.Incremental()

        val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val midKey = Random.shuffle((2 to 9).toList).head
        val newKeyValue = randomFixedKeyValue(midKey)
        val expectedFromValue = FixedMerger(newKeyValue, oldKeyValue.rangeValue.toMemory(midKey)).get.asInstanceOf[Memory.Fixed]
        val expectedKeyValue =
          Slice(
            oldKeyValue.copy(fromKey = 1, toKey = midKey),
            Memory.Range(midKey, 10, expectedFromValue.toFromValue().runRandomIO.right.value, oldKeyValue.rangeValue)
          )

        val expectedLastLevelFromLowerSplit = oldKeyValue.fromValue.flatMap(_.toExpectedLastLevelKeyValue(oldKeyValue.key))
        val expectedLastLevelFromUpperSplit = expectedFromValue.toLastLevelExpected

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: \n" + expectedKeyValue.mkString("\n"))

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice
        )
      }
    }

    "right" in {
      runThis(10000.times) {
        implicit val testTimer = TestTimer.Incremental()

        val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val newKeyValue = randomFixedKeyValue(10)
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
          lastLevelExpect = (expectedLastLevelFromLowerSplit ++ expectedLastLevelFromUpperSplit).toSlice
        )
      }
    }

    "split the range if the input key-values key overlaps range's multiple keys (random mix test)" in {
      implicit val testTimer = TestTimer.Empty

      val deadline1 = 20.seconds.fromNow
      val deadline2 = 30.seconds.fromNow

      //9, 10, 11, 15, 18,    23,      27,  30
      //   10      -     20        25   -   30
      val newKeyValues: Slice[Memory] =
      Slice(
        Memory.put(9, 9),
        Memory.put(10, 10),
        Memory.put(11, 11, deadline1),
        Memory.remove(15),
        Memory.put(18, 18),
        Memory.remove(21),
        Memory.put(23, 23),
        Memory.remove(25),
        Memory.put(27, 27),
        Memory.put(30, 30)
      )

      val oldKeyValues: Slice[Memory] =
        Slice(
          Memory.Range(10, 20, Option.empty[Value.Put], Value.update("ranges value 1")),
          Memory.Range(25, 30, Some(Value.put(25)), Value.update("ranges value 2", deadline2))
        )

      val expected: Slice[Memory] =
        Slice(
          Memory.put(9, 9),
          Memory.Range(10, 11, Some(Value.put(10)), Value.update("ranges value 1")),
          Memory.Range(11, 15, Some(Value.put(11, deadline1)), Value.update("ranges value 1")),
          Memory.Range(15, 18, Some(Value.remove(None)), Value.update("ranges value 1")),
          Memory.Range(18, 20, Some(Value.put(18)), Value.update("ranges value 1")),
          Memory.remove(21),
          Memory.put(23, 23),
          Memory.Range(25, 27, Some(Value.remove(None)), Value.update("ranges value 2", deadline2)),
          Memory.Range(27, 30, Some(Value.put(27)), Value.update("ranges value 2", deadline2)),
          Memory.put(30, 30)
        )

      //last level check
      val expectedInLastLevel: Slice[Memory] =
        Slice(
          Memory.put(9, 9),
          Memory.put(10, 10),
          Memory.put(11, 11, deadline1),
          Memory.put(18, 18),
          Memory.put(23, 23),
          Memory.put(27, 27),
          Memory.put(30, 30)
        )

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = expected.toTransient,
        lastLevelExpect = expectedInLastLevel
      )
    }
  }
}
