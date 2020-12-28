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

package swaydb.core.level.compaction.reception

import swaydb.core.util.AtomicRanges
import swaydb.data.order.KeyOrder

sealed trait LevelReservation[+A] {
  @inline def map[B](f: A => B): LevelReservation[B]
}

case object LevelReservation {

  case class Reserved[A, K](result: A,
                            keyOrNull: AtomicRanges.Key[K])(implicit reserve: AtomicRanges[K]) extends LevelReservation[A] {
    def checkout(): Unit =
      if (keyOrNull != null)
        reserve.remove(keyOrNull)

    @inline def map[B](f: A => B): LevelReservation.Reserved[B, K] =
      new Reserved[B, K](
        result = f(result),
        keyOrNull = keyOrNull
      )
  }

  object Failed {
    @inline def apply(error: swaydb.Error.Level): LevelReservation.Failed =
      new Failed(error)
  }

  class Failed(val error: swaydb.Error.Level) extends LevelReservation[Nothing] {
    @inline override def map[B](f: Nothing => B): LevelReservation[B] =
      this
  }
}
