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

import swaydb.core.data.KeyValue.ReadOnly.{Fixed, Range}
import swaydb.core.data.{KeyValue, Memory, Persistent, Value}
import swaydb.data.slice.Slice

import scala.util.{Success, Try}

object Get {

  def apply(key: Slice[Byte],
            getInCurrentLevelOnly: Slice[Byte] => Try[Option[KeyValue.ReadOnly]],
            getFromNextLevel: Slice[Byte] => Try[Option[KeyValue.FindResponse]])(implicit ordering: Ordering[Slice[Byte]]): Try[Option[KeyValue.FindResponse]] = {
    import ordering._
    getInCurrentLevelOnly(key) flatMap {
      case Some(getFromCurrent) =>
        getFromCurrent match {
          case fixed: Fixed =>
            fixed match {
              case _: Memory.Remove | _: Persistent.Remove =>
                Success(None)

              case put: Memory.Put =>
                Success(Some(put))

              case put: Persistent.Put =>
                Success(Some(put))
            }

          case range: Range =>
            range.fetchFromValue flatMap {
              case Some(_: Value.Remove) if range.fromKey equiv key =>
                Success(None)

              case Some(fromValue: Value.Put) if range.fromKey equiv key =>
                Success(Some(Memory.Put(key, fromValue.value)))

              case _ =>
                range.fetchRangeValue flatMap {
                  case Value.Remove =>
                    Success(None)

                  case rangeValue: Value.Put =>
                    getFromNextLevel(key) map {
                      case Some(_) =>
                        Some(Memory.Put(key, rangeValue.value))

                      case None =>
                        None
                    }
                }
            }
        }

      case None =>
        getFromNextLevel(key)
    }
  }
}