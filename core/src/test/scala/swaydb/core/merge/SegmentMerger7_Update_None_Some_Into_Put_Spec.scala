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

class SegmentMerger7_Update_None_Some_Into_Put_Spec extends WordSpec with CommonAssertions {

  implicit val ordering = KeyOrder.default

  /**
    * Update None Some -> Put None None
    */
  "Update None Some -> Put None None" when {
    "Put None None" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          assertMerge(
            newKeyValue = Memory.Update(1, None, deadline),
            oldKeyValue = Memory.Put(1, None, None),
            expected = Memory.Put(1, None, deadline),
            lastLevelExpect = if (deadline.hasTimeLeft()) Some(Memory.Put(1, None, deadline)) else None,
            hasTimeLeftAtLeast = 10.seconds
          )
      }
    }
  }

  /**
    * Update None Some -> Put None Some
    */
  "Update None Some -> Put None Some" when {
    "Put None HasTimeLeft-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, None, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = Memory.Put(1, None, deadline),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put None HasTimeLeft-Lesser" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, None, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = Memory.Put(1, None, deadline),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put None HasNoTimeLeft-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, None, deadline2),
        expected = Memory.Put(1, None, deadline2),
        lastLevelExpect = Memory.Put(1, None, deadline2),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put None HasNoTimeLeft-Lesser" in {
      val deadline = 1.second.fromNow
      val deadline2 = 2.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, None, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = Memory.Put(1, None, deadline),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put None Expired-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, None, deadline2),
        expected = Memory.Put(1, None, deadline2),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put None Expired-Lesser" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 10.seconds

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, None, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None Some -> Put Some None
    */
  "Update None Some -> Put Some None" when {
    "Put Some None" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          assertMerge(
            newKeyValue = Memory.Update(1, None, deadline),
            oldKeyValue = Memory.Put(1, 1, None),
            expected = Memory.Put(1, None, deadline),
            lastLevelExpect = if (deadline.hasTimeLeft()) Some(Memory.Put(1, None, deadline)) else None,
            hasTimeLeftAtLeast = 10.seconds
          )
      }
    }
  }

  /**
    * Update None Some -> Put Some Some
    */
  "Update None Some -> Put Some Some" when {
    "Put Some HasTimeLeft-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, 1, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = Memory.Put(1, None, deadline),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put Some HasTimeLeft-Lesser" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, 1, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = Memory.Put(1, None, deadline),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put Some HasNoTimeLeft-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, 1, deadline2),
        expected = Memory.Put(1, None, deadline2),
        lastLevelExpect = Memory.Put(1, None, deadline2),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put Some HasNoTimeLeft-Lesser" in {
      val deadline = 1.second.fromNow
      val deadline2 = 2.seconds.fromNow

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, 1, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = Memory.Put(1, None, deadline),
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put Some Expired-Greater" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, 1, deadline2),
        expected = Memory.Put(1, None, deadline2),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Put Some Expired-Lesser" in {
      val deadline = expiredDeadline() - 10.seconds
      val deadline2 = expiredDeadline()

      assertMerge(
        newKeyValue = Memory.Update(1, None, deadline),
        oldKeyValue = Memory.Put(1, 1, deadline2),
        expected = Memory.Put(1, None, deadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

}