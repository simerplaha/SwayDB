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
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class SegmentMerger8_Update_Value_Some_Some_Into_Remove_Spec extends WordSpec with CommonAssertions {

  override implicit val ordering = KeyOrder.default
  implicit val compression = groupingStrategy

  /**
    * Update Some Some -> Remove None
    */
  "Update Some Some -> Remove None" when {
    "Remove None" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds

          assertMerge(
            newKeyValue = Memory.Update(i, i, deadline),
            oldKeyValue = Memory.Remove(i, None),
            expected = Memory.Remove(i, None),
            lastLevelExpect = None,
            hasTimeLeftAtLeast = 10.seconds
          )
      }
    }
  }

  /**
    * Update Some Some -> Remove Some
    */
  "Update Some Some -> Remove Some" when {
    "Remove HasTimeLeft-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, 1, deadline),
        oldKeyValue = Memory.Remove(1, deadline2),
        expected = Memory.Update(1, 1, deadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Remove HasTimeLeft-Lesser" in {
      val deadline = 20.seconds.fromNow
      val deadline2 = 30.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, 1, deadline),
        oldKeyValue = Memory.Remove(1, deadline2),
        expected = Memory.Update(1, 1, deadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Remove HasNoTimeLeft-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, 1, deadline),
        oldKeyValue = Memory.Remove(1, deadline2),
        expected = Memory.Update(1, 1, deadline2),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Remove HasNoTimeLeft-Lesser" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, 1, deadline),
        oldKeyValue = Memory.Remove(1, deadline2),
        expected = Memory.Update(1, 1, deadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Remove Expired-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()

      assertMerge(
        newKeyValue = Memory.Update(1, 1, deadline),
        oldKeyValue = Memory.Remove(1, deadline2),
        expected = Memory.Remove(1, deadline2),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Remove Expired-Lesser" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 2.seconds

      assertMerge(
        newKeyValue = Memory.Update(1, 1, deadline),
        oldKeyValue = Memory.Remove(1, deadline2),
        expected = Memory.Remove(1, deadline2),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

}
