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

package swaydb.data.io

import scala.util.Try
import swaydb.data.IO

/**
  * SwayDB supports non-blocking (Future) and blocking APIs ([[IO]] & Try).
  *
  * The APIs can be converted to other external async and/or blocking types by providing the
  * following implementation for blocking api or use [[AsyncIOTransformer]] for non-blocking APIs.
  */
trait BlockingIOTransformer[O[_]] {
  /**
    * Converts [[IO]] to other blocking type.
    */
  def toOther[I](io: IO[I]): O[I]
  /**
    * Converts other type to [[IO]].
    */
  def toIO[I](io: O[I]): IO[I]
}

object BlockingIOTransformer {

  implicit object IOToIO extends BlockingIOTransformer[IO] {
    override def toOther[I](io: IO[I]): IO[I] = io
    override def toIO[I](io: IO[I]): IO[I] = io
  }

  implicit object IOToTry extends BlockingIOTransformer[Try] {
    override def toOther[I](io: IO[I]): Try[I] = io.toTry
    override def toIO[I](io: Try[I]): IO[I] = IO.fromTry(io)
  }
}