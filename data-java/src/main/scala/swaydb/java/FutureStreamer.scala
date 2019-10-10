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

import java.util.concurrent.{CompletableFuture, ExecutorService, Future}

import swaydb.Streamer

import scala.compat.java8.FutureConverters

trait FutureStreamer[A] { parent =>
  def executorService: ExecutorService
  def head: Future[Option[A]]
  def next(previous: A): Future[Option[A]]

  def toScalaStreamer: Streamer[A, scala.concurrent.Future] =
    new Streamer[A, scala.concurrent.Future] {
      override def head: concurrent.Future[Option[A]] =
        FutureConverters.toScala(CompletableFuture.supplyAsync(() => parent.head.get))

      override def next(previous: A): concurrent.Future[Option[A]] =
        FutureConverters.toScala(CompletableFuture.supplyAsync(() => parent.next(previous).get))
    }
}
