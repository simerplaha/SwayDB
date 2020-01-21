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
 */

package swaydb.data.util

object ByteSizeOf {
  val byte = java.lang.Byte.BYTES
  val short = java.lang.Short.BYTES
  val int = java.lang.Integer.BYTES
  val varInt = int + 1 //5
  val long = java.lang.Long.BYTES
  val varLong = long + 2 //10
  val boolean = java.lang.Byte.BYTES
  val char = java.lang.Character.BYTES
  val double = java.lang.Double.BYTES
  val float = java.lang.Float.BYTES
}