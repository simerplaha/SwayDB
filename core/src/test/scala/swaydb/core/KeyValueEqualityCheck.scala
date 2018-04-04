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

package swaydb.core

import swaydb.core.data._

object KeyValueEqualityCheck {

  private def isEqual(left: Any, right: Any): Boolean =
    (left, right) match {
      case (left: Transient.Put, right: Transient.Put) =>
        left == right

      case (left: Transient.Put, right: Memory.Put) =>
        left.key == right.key &&
          left.isRemove == right.isRemove &&
          left.getOrFetchValue.get == right.value

      case (left: Transient.Put, right: Persistent.Put) =>
        left.key == right.key &&
          left.isRemove == right.isRemove &&
          left.getOrFetchValue == right.getOrFetchValue

//      case (left: Transient.Put, right: Persistent.WriteOnly.Put) =>
//        left.key == right.key &&
//          left.isRange == right.isRange &&
//          left.isRemoveRange == right.isRemoveRange &&
//          left.isRemove == right.isRemove &&
//          left.getOrFetchValue == right.getOrFetchValue &&
//          left.id == right.id

      case (left: Transient.Remove, right: Transient.Remove) =>
        left == right &&
          left.isRange == right.isRange &&
          left.isRemoveRange == right.isRemoveRange &&
          left.isRemove == right.isRemove

      case (left: Transient.Remove, right: Memory.Remove) =>
        left.key == right.key &&
          left.isRemove == right.isRemove &&
          left.getOrFetchValue.get.isEmpty

      case (left: Transient.Remove, right: Persistent.Remove) =>
        left.key == right.key &&
          left.isRemove == right.isRemove &&
          left.getOrFetchValue == right.getOrFetchValue

//      case (left: Transient.Remove, right: Persistent.WriteOnly.Remove) =>
//        left.key == right.key &&
//          left.isRange == right.isRange &&
//          left.isRemoveRange == right.isRemoveRange &&
//          left.isRemove == right.isRemove &&
//          left.getOrFetchValue == right.getOrFetchValue &&
//          left.id == right.id

      case (left: KeyValue.WriteOnly.Range, right: KeyValue.WriteOnly.Range) =>
        left.fromKey == right.fromKey &&
          left.toKey == right.toKey &&
          left.fetchFromAndRangeValue.get == right.fetchFromAndRangeValue.get &&
          left.fetchFromValue.get == right.fetchFromValue.get &&
          left.fetchRangeValue.get == right.fetchRangeValue.get &&
          left.fullKey == right.fullKey &&
          left.id == right.id &&
          left.isRange == right.isRange &&
          left.isRemove == right.isRemove &&
          left.isRemoveRange == right.isRemoveRange

      case (left: KeyValue.WriteOnly.Range, right: Memory.Range) =>
        left.fromKey == right.fromKey &&
          left.toKey == right.toKey &&
          left.fetchFromValue.get == right.fromValue &&
          left.fetchRangeValue.get == right.rangeValue &&
          left.key == right.key &&
          left.isRemove == right.isRemove

      case (left: KeyValue.WriteOnly.Range, right: Transient.Range) =>
        left.fromKey == right.fromKey &&
          left.toKey == right.toKey &&
          left.fetchFromValue.get == right.fromValue &&
          left.fetchRangeValue.get == right.rangeValue &&
          left.fetchFromAndRangeValue.get == right.fetchFromAndRangeValue.get &&
          left.fullKey == right.fullKey &&
          left.key == right.key &&
          left.isRange == right.isRange &&
          left.isRemoveRange == right.isRemoveRange &&
          left.isRemove == right.isRemove

      case (left: Transient.Range, right: Persistent.Range) =>
        left.fromKey == right.fromKey &&
          left.toKey == right.toKey &&
          left.fetchFromValue.get == right.fetchFromValue.get &&
          left.fetchRangeValue.get == right.fetchRangeValue.get &&
          left.fetchFromAndRangeValue.get == right.fetchFromAndRangeValue.get &&
          left.key == right.key &&
          left.isRemove == right.isRemove

      case _ =>
        false

    }

  def apply(left: KeyValue, right: Any): Boolean =
    isEqual(left, right) || isEqual(right, left)

}
