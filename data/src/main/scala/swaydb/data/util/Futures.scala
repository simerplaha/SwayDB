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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.util

import swaydb.IO

import scala.concurrent.{ExecutionContext, Future}

private[swaydb] object Futures {

  val none = Future.successful(None)
  val unit: Future[Unit] = Future.successful(())
  val `true` = Future.successful(true)
  val `false` = Future.successful(false)

  implicit class FutureImplicits[A](future1: Future[A]) {
    @inline def and[B](future2: => Future[B])(implicit executionContext: ExecutionContext): Future[B] =
      future1.flatMap(_ => future2)

    @inline def flatMapCarry(future2: => Future[Unit])(implicit executionContext: ExecutionContext): Future[A] =
      future1 flatMap {
        onesResult =>
          future2.map(_ => onesResult)
      }

    @inline def andIO[L, R](io: => IO[L, A])(implicit executionContext: ExecutionContext): Future[A] =
      future1.flatMap(_ => io.toFuture)
  }

  implicit class FutureUnitImplicits(future1: Future[Unit]) {
    @inline def flatMapUnit[A](future2: => Future[A])(implicit executionContext: ExecutionContext): Future[A] =
      future1.flatMap(_ => future2)

    @inline def mapUnit[A](future2: => A)(implicit executionContext: ExecutionContext): Future[A] =
      future1.map(_ => future2)

  }
}
