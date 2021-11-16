/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.config


import scala.jdk.CollectionConverters._

object Functions {
  implicit def nothing: Functions[Nothing] = Functions[Nothing]()
  implicit def void: Functions[Void] = Functions[Void]()

  def apply[F](functions: java.lang.Iterable[F]): Functions[F] =
    new Functions[F](functions.asScala)

  def apply[F](functions: F*): Functions[F] =
    new Functions[F](functions)
}

case class Functions[F](functions: Iterable[F]) extends Iterable[F] {

  override def iterator: Iterator[F] =
    functions.iterator

  def asJava: java.util.Iterator[F] =
    this.iterator.asJava
}
