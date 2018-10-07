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

import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice

import scala.util.{Success, Try}

object Get {

  def apply(key: Slice[Byte],
            getFromCurrentLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Response]],
            getFromNextLevel: Slice[Byte] => Try[Option[KeyValue.ReadOnly.Put]])(implicit ordering: Ordering[Slice[Byte]]): Try[Option[KeyValue.ReadOnly.Put]] = {
    import ordering._

    def returnRangeValue(current: Value): Try[Option[KeyValue.ReadOnly.Put]] =
      current match {
        case current @ Value.Remove(currentDeadline) =>
          if (current.hasTimeLeft())
            getFromNextLevel(key) map {
              case Some(next) =>
                currentDeadline.map(next.updateDeadline) orElse Some(next)

              case None =>
                None
            }
          else
            TryUtil.successNone

        case current: Value.Put =>

          if (current.hasTimeLeft())
            Success(Some(Memory.Put(key, current.value, current.deadline)))
          else
            TryUtil.successNone

        case current: Value.Update =>
          if (current.hasTimeLeft())
            getFromNextLevel(key) map {
              case Some(next) =>
                Some(Memory.Put(key, current.value, current.deadline orElse next.deadline))

              case None =>
                None
            }
          else
            TryUtil.successNone

      }

    getFromCurrentLevel(key) flatMap {
      case Some(current) =>
        current match {
          case current: KeyValue.ReadOnly.Fixed =>
            current match {
              case current: KeyValue.ReadOnly.Remove =>

                if (current.hasTimeLeft())
                  current.deadline map {
                    currentDeadline =>
                      getFromNextLevel(key) map {
                        next =>
                          next.map(_.updateDeadline(currentDeadline))
                      }
                  } getOrElse getFromNextLevel(key)
                else
                  TryUtil.successNone

              case current: KeyValue.ReadOnly.Put =>
                if (current.hasTimeLeft())
                  Success(Some(current))
                else
                  TryUtil.successNone

              case current: KeyValue.ReadOnly.Update =>

                if (current.hasTimeLeft())
                  getFromNextLevel(key) map {
                    next =>
                      if (next.isDefined) {
                        if (current.deadline.isDefined)
                          Some(current.toPut())
                        else
                          next.flatMap(_.deadline).map(current.toPut) orElse Some(current.toPut())
                      }
                      else
                        None
                  }
                else
                  TryUtil.successNone

              case current: KeyValue.ReadOnly.UpdateFunction =>
                if (current.hasTimeLeft())
                  getFromNextLevel(key) flatMap {
                    nextOption =>
                      nextOption map {
                        _.getOrFetchValue flatMap {
                          nextValue =>
                            current.toPut(nextValue).map(Some(_))
                        }
                      } getOrElse {
                        TryUtil.successNone
                      }
                  }
                else
                  TryUtil.successNone
            }

          case current: KeyValue.ReadOnly.Range =>
            current.fetchFromAndRangeValue flatMap {
              case (fromValue, rangeValue) =>
                if (current.fromKey equiv key)
                  returnRangeValue(fromValue getOrElse rangeValue)
                else
                  returnRangeValue(rangeValue)
            }
        }

      case None =>
        getFromNextLevel(key)
    }
  }
}