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

package swaydb.data.util

import swaydb.IO

import scala.concurrent.{ExecutionContext, Future}

object Futures {

  val none = Future.successful(None)
  val unit: Future[Unit] = Future.successful(())
  val `true` = Future.successful(true)
  val `false` = Future.successful(false)

  implicit class FutureImplicits[T](future1: Future[T]) {
    def and(future2: Future[T])(implicit executionContext: ExecutionContext) =
      future1.flatMap(_ => future2)

    def and[L, R](io: IO[L, T])(implicit executionContext: ExecutionContext) =
      future1.flatMap(_ => io.toFuture)
  }
}
