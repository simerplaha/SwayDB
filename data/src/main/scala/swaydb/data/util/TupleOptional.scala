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

package swaydb.data.util

sealed trait TupleOptional[+L, +R] extends SomeOrNoneCovariant[TupleOptional[L, R], TupleOptional.Some[L, R]] {
  override def noneC: TupleOptional[Nothing, Nothing] = TupleOptional.None
}

object TupleOptional {
  final object None extends TupleOptional[Nothing, Nothing] {
    override def isNoneC: Boolean = true
    override def getC: Some[Nothing, Nothing] = throw new Exception("KeyValue is of type Null")
  }

  case class Some[+L, +R](left: L, right: R) extends TupleOptional[L, R] {
    override def isNoneC: Boolean = false
    override def getC: Some[L, R] = this
  }
}
