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

package swaydb.core.finders

import swaydb.core.data.KeyValue
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice

import scala.util.{Success, Try}

object Max {

  /**
    * Given two key-values returns the lowest. If both are expired then it fetches the next lowest.
    * This function never invokes lowerInNextLevel. 'next' should be pre-fetched and applied to the current Level's
    * lowest Range key-value (if overlapping required) before invoking this function.
    */
  def apply(current: KeyValue.ReadOnly.Fixed,
            next: Option[KeyValue.ReadOnly.Put])(implicit ordering: Ordering[Slice[Byte]]): Try[Option[KeyValue.ReadOnly.Put]] = {
    import ordering._
    current match {
      case current: KeyValue.ReadOnly.Put =>
        next match {
          case Some(next) =>
            if (current.hasTimeLeft()) {
              //    2  or  5   (current)
              //    2          (next)
              if (current.key >= next.key)
                Success(Some(current))
              //0          (current)
              //    2      (next)
              else //else next is highest
                Success(Some(next))
            } else {
              //0 or 1      (current)
              //       2    (next)
              if (next.key > current.key)
                Success(Some(next))
              //     2
              //     2
              else
                TryUtil.successNone
            }

          case None =>
            if (current.hasTimeLeft())
              Success(Some(current))
            else
              TryUtil.successNone
        }

      case current: KeyValue.ReadOnly.Remove =>
        next match {
          case Some(next) =>
            if (current.hasTimeLeft()) {
              //    2
              //    2
              if (current.key equiv next.key)
                Success(current.deadline.map(next.updateDeadline) orElse Some(next))
              //    2
              //         5
              else if (next.key > current.key)
                Success(Some(next))
              //    2
              //0
              else
                TryUtil.successNone
            } else { //lower remove from current is expired.
              //    2
              //        5
              if (next.key > current.key)
                Success(Some(next))
              //     2
              //0 or 2
              else
                TryUtil.successNone
            }

          case None =>
            TryUtil.successNone
        }

      case current: KeyValue.ReadOnly.Update =>
        next match {
          case Some(next) =>
            if (current.hasTimeLeft()) {
              //    2
              //    2
              if (next.key equiv current.key)
                if (current.deadline.isDefined)
                  Success(Some(current.toPut()))
                else
                  Success(next.deadline.map(current.toPut) orElse Some(current.toPut()))
              //    2
              //         5
              else if (next.key > current.key)
                Success(Some(next))
              //    2
              //0
              else
                TryUtil.successNone
            } else { //lower update from current is expired.
              //    2
              //         5
              if (next.key > current.key)
                Success(Some(next))
              //     2
              //0 or 2
              else
                TryUtil.successNone
            }

          case None =>
            TryUtil.successNone
        }
    }
  }

}
