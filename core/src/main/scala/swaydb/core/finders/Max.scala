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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.finders

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import swaydb.core.data.KeyValue
import swaydb.core.function.FunctionStore
import swaydb.core.merge._
import swaydb.core.util.TryUtil
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[core] object Max {

  /**
    * Given two key-values returns the highest [[KeyValue.ReadOnly.Put]] else None.
    *
    * @return Maximum of both key-values. None is returned if the key-value(s) are expired or removed.
    *
    */
  @tailrec
  def apply(current: KeyValue.ReadOnly.Fixed,
            next: Option[KeyValue.ReadOnly.Put])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                 functionStore: FunctionStore): Try[Option[KeyValue.ReadOnly.Put]] = {
    import keyOrder._
    current match {
      case current: KeyValue.ReadOnly.Put =>
        next match {
          case Some(next) =>
            if (current.hasTimeLeft()) {
              if (current.key equiv next.key)
                Try(Some(PutMerger(current, next)))
              //      3  or  5 (current)
              //    2          (next)
              else if (current.key > next.key)
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
                Max(RemoveMerger(current, next), None)
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
                Max(UpdateMerger(current, next), None)

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

      case current: KeyValue.ReadOnly.Function =>
        next match {
          case Some(next) =>
            //    2
            //    2
            if (next.key equiv current.key)
              FunctionMerger(current, next) match {
                case Success(mergedKeyValue) =>
                  Max(mergedKeyValue, None)

                case Failure(exception) =>
                  Failure(exception)
              }

            //    2
            //         5
            else if (next.key > current.key)
              Success(Some(next))
            //    2
            //0
            else
              TryUtil.successNone

          case None =>
            TryUtil.successNone
        }

      case current: KeyValue.ReadOnly.PendingApply =>
        next match {
          case Some(next) =>
            //    2
            //    2
            if (next.key equiv current.key)
              PendingApplyMerger(current, next) match {
                case Success(mergedKeyValue) =>
                  Max(mergedKeyValue, None)

                case Failure(exception) =>
                  Failure(exception)
              }

            //    2
            //         5
            else if (next.key > current.key)
              Success(Some(next))
            //    2
            //0
            else
              TryUtil.successNone

          case None =>
            TryUtil.successNone
        }
    }
  }

}
