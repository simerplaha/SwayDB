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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data

import scala.jdk.CollectionConverters._

object Functions {
  implicit def nothing: Functions[Nothing] = Functions[Nothing]()
  implicit def void: Functions[Void] = Functions[Void]()

  //for java
  def create[F](functions: java.lang.Iterable[F]): Functions[F] =
    apply(functions.asScala)

  def create[F](head: F): Functions[F] =
    apply(Seq(head))

  def apply[F](functions: F*): Functions[F] =
    new Functions[F](functions)
}

case class Functions[F](functions: Iterable[F]) extends Iterable[F] {

  override def iterator: Iterator[F] =
    functions.iterator

  def asJava: java.util.Iterator[F] =
    this.iterator.asJava
}
