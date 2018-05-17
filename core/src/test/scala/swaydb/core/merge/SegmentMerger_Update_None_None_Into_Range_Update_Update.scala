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
import swaydb.core.data.{Memory, Value}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

/**
  * These test cases are already covered under [[SegmentMerger_Fixed_Into_Range]] which randomly tests
  * all combinations of Single key-value merges into Range key-values.
  */
class SegmentMerger_Update_None_None_Into_Range_Update_Update extends WordSpec with CommonAssertions {

  implicit val ordering = KeyOrder.default

  /**
    * Update None None -> _
    */

  "Update None None -> Range Update(None, None) Update(None, None)" when {
    "Range Update(None, None) Update(None, None) - Left" in {
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, None) - Mid" in {
      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, None) - Right" in {
      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(None, None) Update(None, Some)
    */
  "Update None None -> Range Update(None, None) Update(None, HasTimeLeft)" when {
    "Range Update(None, None) Update(None, HasTimeLeft) - Left" in {
      val deadline = Some(20.seconds.fromNow)
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, HasTimeLeft) - Mid" in {
      val deadline = Some(20.seconds.fromNow)
      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(None, deadline)),
          Memory.Range(5, 10, Value.Update(None, deadline), Value.Update(None, deadline))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, HasTimeLeft) - Right" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(None, None) Update(None, HasNoTimeLeft)" when {
    "Range Update(None, None) Update(None, HasNoTimeLeft) - Left" in {
      val deadline = Some(1.seconds.fromNow)
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, HasNoTimeLeft) - Mid" in {
      val deadline = Some(1.seconds.fromNow)
      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(None, deadline)),
          Memory.Range(5, 10, Value.Update(None, deadline), Value.Update(None, deadline))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, HasNoTimeLeft) - Right" in {
      val deadline = Some(1.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(None, None) Update(None, Expired)" when {
    "Range Update(None, None) Update(None, HasNoTimeLeft) - Left" in {
      val deadline = Some(expiredDeadline())
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, HasNoTimeLeft) - Mid" in {
      val deadline = Some(expiredDeadline())
      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(None, deadline)),
          Memory.Range(5, 10, Value.Update(None, deadline), Value.Update(None, deadline))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, HasNoTimeLeft) - Right" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, deadline)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(None, None) Update(Some, None)
    */
  "Update None None -> Range Update(None, None) Update(Some, None)" when {
    "Range Update(None, None) Update(Some, None) - Left" in {

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, None)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, None) - Mid" in {
      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(1, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(1, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, None) - Right" in {
      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(None, None) Update(Some, Some)
    */
  "Update None None -> Range Update(None, None) Update(Some, HasTimeLeft)" when {
    "Range Update(None, None) Update(Some, HasTimeLeft) - Left" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, HasTimeLeft) - Mid" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(1, deadline)),
          Memory.Range(5, 10, Value.Update(None, deadline), Value.Update(1, deadline))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, HasTimeLeft) - Right" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(None, None) Update(Some, HasNoTimeLeft)" when {
    "Range Update(None, None) Update(Some, HasNoTimeLeft) - Left" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, HasNoTimeLeft) - Mid" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(1, deadline)),
          Memory.Range(5, 10, Value.Update(None, deadline), Value.Update(1, deadline))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, HasNoTimeLeft) - Right" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(None, None) Update(Some, Expired)" when {
    "Range Update(None, None) Update(Some, Expired) - Left" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, Expired) - Mid" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, None), Value.Update(1, deadline)),
          Memory.Range(5, 10, Value.Update(None, deadline), Value.Update(1, deadline))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(Some, Expired) - Right" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, None), Value.Update(1, deadline)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(None, Some) Update(None, None)
    */

  "Update None None -> Range Update(None, HasTimeLeft) Update(None, None)" when {
    "Range Update(None, HasTimeLeft) Update(None, None) - Left" in {
      val deadline = Some(20.seconds.fromNow)
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, HasTimeLeft) Update(None, None) - Mid" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, deadline), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, HasTimeLeft) Update(None, None) - Right" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(None, HasNoTimeLeft) Update(None, None)" when {
    "Range Update(None, HasNoTimeLeft) Update(None, None) - Left" in {
      val deadline = Some(5.seconds.fromNow)
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, HasNoTimeLeft) Update(None, None) - Mid" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, deadline), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, HasNoTimeLeft) Update(None, None) - Right" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(None, Expired) Update(None, None)" when {
    "Range Update(None, Expired) Update(None, None) - Left" in {
      val deadline = Some(expiredDeadline())
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, Expired) Update(None, None) - Mid" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(None, deadline), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, Expired) Update(None, None) - Right" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(Some, None) Update(None, None)
    */

  "Update None None -> Range Update(Some, None) Update(None, None)" when {
    "Range Update(Some, None) Update(None, None) - Left" in {
      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, None), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, None), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, None) - Mid" in {

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, None), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, None), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(None, None) Update(None, None) - Right" in {

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, None), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, None), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(Some, Some) Update(None, None)
    */

  "Update None None -> Range Update(Some, HasTimeLeft) Update(None, None)" when {
    "Range Update(Some, HasTimeLeft) Update(None, None) - Left" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, None) - Mid" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, None) - Right" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasNoTimeLeft) Update(None, None)" when {
    "Range Update(Some, HasNoTimeLeft) Update(None, None) - Left" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(None, None) - Mid" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(None, None) - Right" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, Expired) Update(None, None)" when {
    "Range Update(Some, Expired) Update(None, None) - Left" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, Expired) Update(None, None) - Mid" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(None, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(None, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, Expired) Update(None, None) - Right" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(Some, Some) Update(Some, None)
    */

  "Update None None -> Range Update(Some, HasTimeLeft) Update(Some, None)" when {
    "Range Update(Some, HasTimeLeft) Update(Some, None) - Left" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, None) - Mid" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(2, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, None) - Right" in {
      val deadline = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasNoTimeLeft) Update(Some, None)" when {
    "Range Update(Some, HasNoTimeLeft) Update(Some, None) - Left" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(Some, None) - Mid" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(2, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(Some, None) - Right" in {
      val deadline = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, Expired) Update(Some, None)" when {
    "Range Update(Some, Expired) Update(Some, None) - Left" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, None)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, Expired) Update(Some, None) - Mid" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, None)),
          Memory.Range(5, 10, Value.Update(None, None), Value.Update(2, None))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, Expired) Update(Some, None) - Right" in {
      val deadline = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, None)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(Some, Some) Update(None, Some)
    */

  "Update None None -> Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Greater" when {
    "Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Left - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Mid - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(None, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(None, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Right - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Lesser" when {
    "Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Left - Lesser" in {
      val deadline = Some(20.seconds.fromNow)
      val deadline2 = Some(40.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Mid - Lesser" in {
      val deadline = Some(20.seconds.fromNow)
      val deadline2 = Some(40.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(None, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(None, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, HasTimeLeft) - Right - Lesser" in {
      val deadline = Some(20.seconds.fromNow)
      val deadline2 = Some(40.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasTimeLeft) Update(None, HasNoTimeLeft) - Greater" when {
    "Range Update(Some, HasNoTimeLeft) Update(None, HasTimeLeft) - Left - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, HasNoTimeLeft) - Mid - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(None, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(None, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, HasNoTimeLeft) - Right - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasTimeLeft) Update(None, Expired) - Greater" when {
    "Range Update(Some, HasNoTimeLeft) Update(None, Expired) - Left - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(None, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, Expired) - Mid - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(None, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(None, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(None, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(None, Expired) - Right - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(Some, HasTimeLeft) Update(Some, Some)
    */

  "Update None None -> Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Greater" when {
    "Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Left - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Mid - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(2, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Right - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Lesser" when {
    "Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Left - Greater" in {
      val deadline = Some(20.seconds.fromNow)
      val deadline2 = Some(40.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Mid - Greater" in {
      val deadline = Some(20.seconds.fromNow)
      val deadline2 = Some(40.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(2, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, HasTimeLeft) - Right - Greater" in {
      val deadline = Some(20.seconds.fromNow)
      val deadline2 = Some(40.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasTimeLeft) Update(Some, HasNoTimeLeft) - Greater" when {
    "Range Update(Some, HasTimeLeft) Update(Some, HasNoTimeLeft) - Left - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, HasNoTimeLeft) - Mid - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(2, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, HasNoTimeLeft) - Right - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(5.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasTimeLeft) Update(Some, Expired) - Greater" when {
    "Range Update(Some, HasTimeLeft) Update(Some, Expired) - Left - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, Expired) - Mid - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(2, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasTimeLeft) Update(Some, Expired) - Right - Greater" in {
      val deadline = Some(40.seconds.fromNow)
      val deadline2 = Some(expiredDeadline())

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  /**
    * Update None None -> Range Update(Some, HasNoTimeLeft) Update(Some, Some)
    */

  "Update None None -> Range Update(Some, HasNoTimeLeft) Update(Some, HasTimeLeft) - Greater" when {
    "Range Update(Some, HasNoTimeLeft) Update(Some, HasTimeLeft) - Left - Greater" in {
      val deadline = Some(5.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(Some, HasTimeLeft) - Mid - Greater" in {
      val deadline = Some(5.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(2, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(Some, HasTimeLeft) - Right - Greater" in {
      val deadline = Some(5.seconds.fromNow)
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, Expired) Update(Some, HasTimeLeft) - Greater" when {
    "Range Update(Some, Expired) Update(Some, HasTimeLeft) - Left - Greater" in {
      val deadline = Some(expiredDeadline())
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, Expired) Update(Some, HasTimeLeft) - Mid - Greater" in {
      val deadline = Some(expiredDeadline())
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(2, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, Expired) Update(Some, HasTimeLeft) - Right - Greater" in {
      val deadline = Some(expiredDeadline())
      val deadline2 = Some(20.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

  "Update None None -> Range Update(Some, HasNoTimeLeft) Update(Some, HasNoTimeLeft) - Greater" when {
    "Range Update(Some, HasNoTimeLeft) Update(Some, HasNoTimeLeft) - Left - Greater" in {
      val deadline = Some(5.seconds.fromNow)
      val deadline2 = Some(1.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(1, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Memory.Range(1, 10, Value.Update(None, deadline), Value.Update(2, deadline2)),
        lastLevelExpect = None,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(Some, HasNoTimeLeft) - Mid - Greater" in {
      val deadline = Some(5.seconds.fromNow)
      val deadline2 = Some(1.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(5, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 5, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Range(5, 10, Value.Update(None, deadline2), Value.Update(2, deadline2))
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }

    "Range Update(Some, HasNoTimeLeft) Update(Some, HasNoTimeLeft) - Right - Greater" in {
      val deadline = Some(5.seconds.fromNow)
      val deadline2 = Some(1.seconds.fromNow)

      assertMerge(
        newKeyValue = Memory.Update(10, None, None),
        oldKeyValue = Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
        expected = Slice(
          Memory.Range(1, 10, Value.Update(1, deadline), Value.Update(2, deadline2)),
          Memory.Update(10, None, None)
        ),
        lastLevelExpect = Slice.empty,
        hasTimeLeftAtLeast = 10.seconds
      )
    }
  }

}
