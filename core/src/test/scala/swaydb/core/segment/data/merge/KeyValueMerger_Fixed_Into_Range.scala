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
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.segment.data.{Memory, Value}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._

import scala.concurrent.duration._
import scala.util.Random

class KeyValueMerger_Fixed_Into_Range extends AnyWordSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  "Single into Range" when {

    "left out" in {
      implicit val testTimer = TestTimer.Incremental()
      runThis(10000.times) {
        val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val newKeyValue = randomFixedKeyValue(0)

        val expectedKeyValue = Slice(newKeyValue, oldKeyValue)
        val expectedLastLevel: Slice[Memory.Fixed] = expectedKeyValue.flatMap(_.toLastLevelExpected.toList).toSlice

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
        val expectedFromValue = FixedMerger(newKeyValue, oldKeyValue.fromValue.getOrElseS(oldKeyValue.rangeValue).toMemory(oldKeyValue.key))
        val expectedKeyValue = Memory.Range(1, 10, expectedFromValue.toFromValue(), oldKeyValue.rangeValue)
        val expectedLastLevel = expectedFromValue.asInstanceOf[Memory.Fixed].toLastLevelExpected

        //println
        //println("newKeyValue: " + newKeyValue)
        //println("oldKeyValue: " + oldKeyValue)
        //println("expectedKeyValue: " + expectedKeyValue)

        assertMerge(
          newKeyValue = newKeyValue,
          oldKeyValue = oldKeyValue,
          expected = expectedKeyValue,
          lastLevelExpect = expectedLastLevel getOrElse Memory.Null
        )
      }
    }

    "mid" in {
      runThis(10000.times) {
        implicit val testTimer = TestTimer.Incremental()

        val oldKeyValue = Memory.Range(1, 10, randomFromValueOption(), randomRangeValue())
        val midKey = Random.shuffle((2 to 9).toList).head
        val newKeyValue = randomFixedKeyValue(midKey)
        val expectedFromValue = FixedMerger(newKeyValue, oldKeyValue.rangeValue.toMemory(midKey)).asInstanceOf[Memory.Fixed]
        val expectedKeyValue =
          Slice(
            oldKeyValue.copy(fromKey = 1, toKey = midKey),
            Memory.Range(midKey, 10, expectedFromValue.toFromValue(), oldKeyValue.rangeValue)
          )

        val expectedLastLevelFromLowerSplit = oldKeyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(oldKeyValue.key))
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

        val expectedLastLevelFromLowerSplit = oldKeyValue.fromValue.flatMapOptionS(_.toExpectedLastLevelKeyValue(oldKeyValue.key))

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
          Memory.Range(10, 20, Value.FromValue.Null, Value.update("ranges value 1")),
          Memory.Range(25, 30, Value.put(25), Value.update("ranges value 2", deadline2))
        )

      val expected: Slice[Memory] =
        Slice(
          Memory.put(9, 9),
          Memory.Range(10, 11, Value.put(10), Value.update("ranges value 1")),
          Memory.Range(11, 15, Value.put(11, deadline1), Value.update("ranges value 1")),
          Memory.Range(15, 18, Value.remove(None), Value.update("ranges value 1")),
          Memory.Range(18, 20, Value.put(18), Value.update("ranges value 1")),
          Memory.remove(21),
          Memory.put(23, 23),
          Memory.Range(25, 27, Value.remove(None), Value.update("ranges value 2", deadline2)),
          Memory.Range(27, 30, Value.put(27), Value.update("ranges value 2", deadline2)),
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
        expected = expected,
        lastLevelExpect = expectedInLastLevel
      )
    }
  }
}
