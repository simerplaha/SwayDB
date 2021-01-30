/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.java
import swaydb.utils.StorageUnits._

object StorageUnits {

  @inline final def bytes(measure: Int): Int =
    measure

  @inline final def byte(measure: Int): Int =
    measure

  @inline final def mb(measure: Double): Int =
    measure.mb

  @inline final def gb(measure: Double): Int =
    measure.gb

  @inline final def kb(measure: Double): Int =
    measure.kb

  @inline final def mb_long(measure: Double): Long =
    measure.mb_long

  @inline final def gb_long(measure: Double): Long =
    measure.gb_long

  @inline final def kb_long(measure: Double): Long =
    measure.kb_long
}
