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

package swaydb.java

import swaydb.Tag
import swaydb.java.data.util.Java._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object Stream {
  def fromScala[A](stream: swaydb.Stream[A, swaydb.IO.ThrowableIO]): StreamIO[A] =
    new StreamIO(stream)

  def fromScala[A](stream: swaydb.Stream[A, scala.concurrent.Future])(implicit ec: ExecutionContext): StreamFuture[A] =
    new StreamFuture(stream)

  def create[A](iterator: java.util.Iterator[A]): StreamIO[A] =
    new StreamIO[A](swaydb.Stream(iterator.asScala.toIterable))

  def create[A](iterator: java.util.List[A]): StreamIO[A] =
    new StreamIO[A](swaydb.Stream(iterator.asScala))

  def create[A](iterator: java.util.Collection[A]): StreamIO[A] =
    new StreamIO[A](swaydb.Stream(iterator.asScala))

  def create[A](ioStreamer: IOStreamer[A]): StreamIO[A] =
    new StreamIO(swaydb.Stream(ioStreamer.toScalaStreamer))

  def create[A](ioStreamer: FutureStreamer[A]): StreamFuture[A] = {
    implicit val ec: ExecutionContext = ioStreamer.executorService.asScala
    implicit val tag: Tag.Async.Retryable[Future] = Tag.future(ec)
    new StreamFuture(swaydb.Stream(ioStreamer.toScalaStreamer))
  }
}
