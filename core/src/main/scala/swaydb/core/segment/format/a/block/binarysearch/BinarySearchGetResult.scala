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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.block.binarysearch

import swaydb.core.data.{Persistent, PersistentOptional}

private[block] sealed trait BinarySearchGetResult {
  def toPersistentOptional: PersistentOptional
}

private[block] object BinarySearchGetResult {

  val none: BinarySearchGetResult.None =
    new BinarySearchGetResult.None(Persistent.Partial.Null)

  class None(val lower: Persistent.PartialOptional) extends BinarySearchGetResult {
    override def toPersistentOptional: PersistentOptional = Persistent.Null
  }

  class Some(val value: Persistent.Partial) extends BinarySearchGetResult {
    override def toPersistentOptional: PersistentOptional =
      value.toPersistentOptional
  }
}
