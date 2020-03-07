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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java

protected object ScalaMapToJavaMapOutputConverter {

  @inline implicit def toIO[R](io: swaydb.IO[scala.Throwable, R]): IO[scala.Throwable, R] = new IO[scala.Throwable, R](io)(IO.throwableExceptionHandler)

  @inline implicit def toIOBoolean(io: swaydb.IO[scala.Throwable, Boolean]): IO[scala.Throwable, java.lang.Boolean] =
    new IO[scala.Throwable, java.lang.Boolean](io.asInstanceOf[swaydb.IO[scala.Throwable, java.lang.Boolean]])(IO.throwableExceptionHandler)

  @inline implicit def toIOInteger(io: swaydb.IO[scala.Throwable, scala.Int]): IO[scala.Throwable, java.lang.Integer] =
    new IO[scala.Throwable, java.lang.Integer](io.asInstanceOf[swaydb.IO[scala.Throwable, java.lang.Integer]])(IO.throwableExceptionHandler)

  @inline implicit def fromJavaScalaIntToInteger(io: swaydb.java.IO[scala.Throwable, scala.Int]): IO[scala.Throwable, java.lang.Integer] =
    io.asInstanceOf[swaydb.java.IO[scala.Throwable, java.lang.Integer]]
}
