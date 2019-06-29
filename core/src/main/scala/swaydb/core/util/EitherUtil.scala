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

package swaydb.core.util

import scala.util.{Either, Right}

/**
  * To fix 2.11 and 2.12 Either compilation issue.
  */
object EitherUtil {
  implicit class EitherImplicits[+A, +B](either: Either[A, B]) {
    @inline def mapEither[B1](f: B => B1): Either[A, B1] =
      either match {
        case Right(b) => Right(f(b))
        case _ => this.asInstanceOf[Either[A, B1]]
      }

    @inline def getOrElseEither[B1 >: B](f: => B1): B1 =
      either match {
        case Right(b) => b
        case _ => f
      }

    @inline def existsEither(f: B => Boolean): Boolean =
      either match {
        case Right(b) => f(b)
        case _ => false
      }
  }
}
