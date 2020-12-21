/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level

import swaydb.core.util.ReserveRange
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

sealed trait LevelReserveResult[+A] {
  @inline def map[B](f: A => B): LevelReserveResult[B]
}
case object LevelReserveResult {

  case class Reserved[A](result: A,
                         key: Slice[Byte])(implicit reserve: ReserveRange.State[Unit],
                                           keyOrder: KeyOrder[Slice[Byte]]) extends LevelReserveResult[A] {
    def free(): Unit =
      ReserveRange.free(key)(reserve, keyOrder)

    @inline def map[B](f: A => B): LevelReserveResult.Reserved[B] =
      new Reserved[B](
        result = f(result),
        key = key
      )
  }

  object Failed {
    @inline def apply(error: swaydb.Error.Level): LevelReserveResult.Failed =
      new Failed(error)
  }

  class Failed(val error: swaydb.Error.Level) extends LevelReserveResult[Nothing] {
    @inline override def map[B](f: Nothing => B): LevelReserveResult[B] =
      this
  }
}
