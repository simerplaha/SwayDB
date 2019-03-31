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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.io.converter

import scala.util.Try
import swaydb.data.IO

trait BlockingIOConverter[O[_]] {
  def apply[I](result: IO[I]): O[I]
  def toIO[I](result: O[I]): IO[I]
}

object BlockingIOConverter {

  implicit object IOToIO extends BlockingIOConverter[IO] {
    override def apply[I](result: IO[I]): IO[I] = result
    override def toIO[I](result: IO[I]): IO[I] = result
  }

  implicit object IOToTry extends BlockingIOConverter[Try] {
    override def apply[I](result: IO[I]): Try[I] = result.toTry
    override def toIO[I](result: Try[I]): IO[I] = IO.fromTry(result)
  }
}