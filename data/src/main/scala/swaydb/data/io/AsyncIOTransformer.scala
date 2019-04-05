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

import scala.concurrent.Future
import swaydb.data.IO

/**
  * SwayDB supports non-blocking (Future) and blocking APIs ([[IO]] & Try).
  *
  * The APIs can be converted to other external async and/or blocking types by providing the
  * following implementation for non-blocking api or use [[BlockingIOTransformer]] for blocking APIs.
  */
trait AsyncIOTransformer[O[_]] {
  /**
    * Converts a Future to other async type
    */
  def toOther[I](future: Future[I]): O[I]
  /**
    * Converts other async type to Future
    */
  def toFuture[I](io: O[I]): Future[I]
}

object AsyncIOTransformer {

  implicit object IOToFuture extends AsyncIOTransformer[Future] {
    override def toOther[I](future: Future[I]): Future[I] = future
    override def toFuture[I](future: Future[I]): Future[I] = future
  }
}