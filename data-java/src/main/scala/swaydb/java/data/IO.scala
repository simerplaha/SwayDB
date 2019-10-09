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

package swaydb.java.data

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{Consumer, Predicate, Supplier}

import scala.compat.java8.OptionConverters._
import scala.compat.java8.FutureConverters._
import swaydb.java.data.util.Javaz.JavaFunction

object IO {
  def fromScala[L, R](io: swaydb.IO[L, R], exceptionHandler: swaydb.IO.ExceptionHandler[L]) =
    IO(io)(exceptionHandler)
}

case class IO[L, R](asScala: swaydb.IO[L, R])(implicit val exceptionHandler: swaydb.IO.ExceptionHandler[L]) {

  def left: IO[Throwable, L] =
    IO(asScala.left)

  def right: IO[Throwable, R] =
    IO(asScala.right)

  def isLeft: Boolean =
    asScala.isLeft

  def isRight: Boolean =
    asScala.isRight

  @throws[Throwable]
  def get: R =
    asScala.get

  def getOrElse[B >: R](supplier: Supplier[B]): B =
    asScala.getOrElse(supplier.get())

  def orElse[B >: R](supplier: Supplier[IO[L, B]]): IO[L, B] =
    new IO[L, B](asScala.orElse(supplier.get().asScala))

  def forEach(consumer: Consumer[R]): Unit =
    asScala.foreach(consumer.accept)

  def map[B](function: JavaFunction[R, B]): IO[L, B] =
    IO(asScala.map(result => function.apply(result)))

  def flatMap[B](function: JavaFunction[R, IO[L, B]]): IO[L, B] =
    IO(asScala.flatMap[L, B](result => function.apply(result).asScala))

  def exists(predicate: Predicate[R]): Boolean =
    asScala.exists(predicate.test)

  def filter(predicate: Predicate[R]): IO[L, R] =
    IO(asScala.filter(predicate.test))

  def recoverWith[B >: R](function: JavaFunction[L, IO[L, B]]): IO[L, B] =
    IO(
      asScala.recoverWith {
        case result =>
          function.apply(result).asScala
      }
    )

  def recover[B >: R](function: JavaFunction[L, B]): IO[L, B] =
    IO(
      asScala.recover {
        case result =>
          function(result)
      }
    )

  def onLeftSideEffect(consumer: Consumer[L]): IO[L, R] =
    IO(
      asScala.onLeftSideEffect {
        left =>
          consumer.accept(left.value)
      }
    )

  def onRightSideEffect(consumer: Consumer[R]): IO[L, R] =
    IO {
      asScala.onRightSideEffect(consumer.accept)
    }

  def onCompleteSideEffect(consumer: Consumer[IO[L, R]]): IO[L, R] =
    IO {
      asScala.onCompleteSideEffect {
        io =>
          consumer.accept(IO(io)) //fixme - is will use this be enough?
      }
    }

  def toOption: Optional[R] =
    asScala.toOption.asJava

  def toFuture: CompletionStage[R] =
    asScala.toFuture.toJava
}

