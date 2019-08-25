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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.Reserve

trait ErrorHandler[+E] {
  def toException[F >: E](f: F): Throwable
  def fromException(e: Throwable): E
  def reserve[F >: E](f: F): Option[Reserve[Unit]]
}

object ErrorHandler extends LazyLogging {
  def toException[E](error: E)(implicit errorHandler: ErrorHandler[E]): Throwable =
    errorHandler.toException(error)

  def fromException[E](exception: Throwable)(implicit errorHandler: ErrorHandler[E]): E =
    errorHandler.fromException(exception)

  def reserve[E](e: E)(implicit errorHandler: ErrorHandler[E]): Option[Reserve[Unit]] =
    try
      errorHandler.reserve(e)
    catch {
      case exception: Throwable =>
        logger.error("Failed to fetch Reserve. Stopping recovery.", exception)
        None
    }

  object Nothing extends ErrorHandler[Nothing] {
    override def fromException(e: Throwable): Nothing = throw new scala.Exception("Nothing cannot be created from Exception.", e)
    override def toException[F >: Nothing](f: F): Throwable = new Exception("Nothing value.")
    override def reserve[F >: Nothing](f: F): Option[Reserve[Unit]] = None
  }

  object Unit extends ErrorHandler[Unit] {
    override def reserve[F >: Unit](f: F): Option[Reserve[Unit]] = None
    override def fromException(e: Throwable): Unit = throw new scala.Exception("Unit cannot be created from Exception.", e)
    override def toException[F >: Unit](f: F): Throwable = new Exception("Unit value.")
  }

  implicit object Throwable extends ErrorHandler[Throwable] {
    override def fromException(e: Throwable): Throwable = e
    override def toException[F >: Throwable](f: F): Throwable = f.asInstanceOf[Throwable]
    override def reserve[F >: Throwable](f: F): Option[Reserve[Unit]] = None
  }
}
