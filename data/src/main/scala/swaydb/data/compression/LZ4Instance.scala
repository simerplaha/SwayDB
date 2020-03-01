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

package swaydb.data.compression

import swaydb.macros.Sealed

import scala.util.Random

sealed trait LZ4Instance
object LZ4Instance {
  def fastestInstance: LZ4Instance = Fastest
  case object Fastest extends LZ4Instance

  def fastestJavaInstance: LZ4Instance = FastestJava
  case object FastestJava extends LZ4Instance

  def nativeInstance: LZ4Instance = Native
  case object Native extends LZ4Instance

  def safeInstance: LZ4Instance = Safe
  case object Safe extends LZ4Instance

  def unsafeInstance: LZ4Instance = Unsafe
  case object Unsafe extends LZ4Instance

  def instances(): List[LZ4Instance] =
    Sealed.list[LZ4Instance]

  def random(): LZ4Instance =
    Random.shuffle(instances()).head
}
