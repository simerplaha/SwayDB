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

class SegmentMerger6_Remove_Some_Into_Update_Spec extends WordSpec with CommonAssertions {

  implicit val ordering = KeyOrder.default

  /**
    * Remove Some -> Update None None
    */
  "Remove Some -> Update None None" when {
    "Update None None" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          assertMerge(
            newKeyValue = Memory.Remove(1, deadline),
            oldKeyValue = Memory.Update(1, None, None),
            expected = Memory.Update(1, None, deadline),
            lastLevelExpect = None,
            hasTimeLeftAtLeast = 10.seconds
          )
      }

    }
  }

  /**
    * Remove Some -> Update Some None
    */
  "Remove Some -> Update Some None" when {
    "Update None None" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          assertMerge(
            newKeyValue = Memory.Remove(1, deadline),
            oldKeyValue = Memory.Update(1, i.toString, None),
            expected = Memory.Update(1, i.toString, deadline),
            lastLevelExpect = None,
            hasTimeLeftAtLeast = 10.seconds
          )
      }

    }
  }

  /**
    * Remove Some -> Update None Some
    */
  "Remove Some -> Update None Some" when {
    "Update None HasTimeLeft-Greater" in {
      val newDeadline = 20.seconds.fromNow
      val oldDeadline = 15.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, None, oldDeadline),
        expected = Memory.Update(1, None, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update None HasTimeLeft-Lesser" in {
      val newDeadline = 10.seconds.fromNow
      val oldDeadline = 15.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, None, oldDeadline),
        expected = Memory.Update(1, None, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update None HasNoTimeLeft-Greater" in {
      val newDeadline = 20.seconds.fromNow
      val oldDeadline = 2.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, None, oldDeadline),
        expected = Memory.Update(1, None, oldDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update None HasNoTimeLeft-Lesser" in {
      val newDeadline = 1.seconds.fromNow
      val oldDeadline = 2.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, None, oldDeadline),
        expected = Memory.Update(1, None, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update None Expired-Greater" in {
      val newDeadline = 20.seconds.fromNow
      val oldDeadline = expiredDeadline()
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, None, oldDeadline),
        expected = Memory.Update(1, None, oldDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update None Expired-Lesser" in {
      val newDeadline = expiredDeadline() - 10.seconds
      val oldDeadline = expiredDeadline()
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, None, oldDeadline),
        expected = Memory.Update(1, None, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Remove Some -> Update Some Some
    */
  "Remove Some -> Update Some Some" when {
    "Update Some HasTimeLeft-Greater" in {
      val newDeadline = 20.seconds.fromNow
      val oldDeadline = 15.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, 1, oldDeadline),
        expected = Memory.Update(1, 1, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update Some HasTimeLeft-Lesser" in {
      val newDeadline = 10.seconds.fromNow
      val oldDeadline = 15.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, 1, oldDeadline),
        expected = Memory.Update(1, 1, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update Some HasNoTimeLeft-Greater" in {
      val newDeadline = 20.seconds.fromNow
      val oldDeadline = 2.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, 1, oldDeadline),
        expected = Memory.Update(1, 1, oldDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update Some HasNoTimeLeft-Lesser" in {
      val newDeadline = 1.seconds.fromNow
      val oldDeadline = 2.seconds.fromNow
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, 1, oldDeadline),
        expected = Memory.Update(1, 1, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update Some Expired-Greater" in {
      val newDeadline = 20.seconds.fromNow
      val oldDeadline = expiredDeadline()
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, 1, oldDeadline),
        expected = Memory.Update(1, 1, oldDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Update Some Expired-Lesser" in {
      val newDeadline = expiredDeadline() - 10.seconds
      val oldDeadline = expiredDeadline()
      assertMerge(
        newKeyValue = Memory.Remove(1, newDeadline),
        oldKeyValue = Memory.Update(1, 1, oldDeadline),
        expected = Memory.Update(1, 1, newDeadline),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }
}
