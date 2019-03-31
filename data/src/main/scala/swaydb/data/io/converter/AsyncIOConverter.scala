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

import scala.concurrent.Future
import swaydb.data.IO

/**
  * SwayDB supports non-blocking (Future) and blocking APIs ([[IO]] & Try).
  *
  * The APIs can be converted to other external async and/or blocking types by providing the
  * following implementation for non-blocking api or use [[BlockingIOConverter]] for blocking APIs.
  */
trait AsyncIOConverter[O[_]] {
  /**
    * Converts a Future to other async type
    */
  def apply[I](result: Future[I]): O[I]
  /**
    * Converts other async type to Future
    */
  def toFuture[I](result: O[I]): Future[I]

  /**
    * None value of other type.
    */
  def none[I]: O[Option[I]]
}

object AsyncIOConverter {

  implicit object IOToFuture extends AsyncIOConverter[Future] {
    override def apply[I](result: Future[I]): Future[I] = result
    override def toFuture[I](result: Future[I]): Future[I] = result
    override def none[I]: Future[Option[I]] = Future.successful(None)
  }
}