/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

sealed trait TupleOrNone[+L, +R] extends SomeOrNoneCovariant[TupleOrNone[L, R], TupleOrNone.Some[L, R]] {
  override def noneC: TupleOrNone[Nothing, Nothing] = TupleOrNone.None
}

object TupleOrNone {
  final object None extends TupleOrNone[Nothing, Nothing] {
    override def isNoneC: Boolean = true
    override def getC: Some[Nothing, Nothing] = throw new Exception("KeyValue is of type Null")
  }

  case class Some[+L, +R](left: L, right: R) extends TupleOrNone[L, R] {
    override def isNoneC: Boolean = false
    override def getC: Some[L, R] = this
  }
}
