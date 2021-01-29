/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.data.slice

import swaydb.IO

import scala.collection.Iterable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object SliceIOImplicits {

  implicit class IterableIOImplicit[E: IO.ExceptionHandler, A: ClassTag](iterable: Iterable[A]) {

    def foreachIO[R](f: A => IO[E, R], failFast: Boolean = true): Option[IO.Left[E, R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) onLeftSideEffect {
          case failed @ IO.Left(_) =>
            failure = Some(failed)
        }
      }
      failure
    }

    //returns the first IO.Right(Some(_)).
    def untilSome[R](f: A => IO[E, Option[R]]): IO[E, Option[(R, A)]] = {
      iterable foreach {
        item =>
          f(item) match {
            case IO.Right(Some(value)) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Right[E, Option[(R, A)]](Some((value, item)))

            case IO.Right(None) =>
            //continue reading

            case IO.Left(error) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Left(error)
          }
      }
      IO.none
    }

    def untilSomeValue[R](f: A => IO[E, Option[R]]): IO[E, Option[R]] = {
      iterable foreach {
        item =>
          f(item) match {
            case IO.Right(Some(value)) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Right[E, Option[R]](Some(value))

            case IO.Right(None) =>
            //continue reading

            case IO.Left(error) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Left(error)
          }
      }
      IO.none
    }

    def mapRecoverIO[R: ClassTag](block: A => IO[E, R],
                                  recover: (Slice[R], IO.Left[E, Slice[R]]) => Unit = (_: Slice[R], _: IO.Left[E, Slice[R]]) => (),
                                  failFast: Boolean = true): IO[E, Slice[R]] =
      mapRecoverIOIterable(
        iterable = iterable,
        block = block,
        recover = recover,
        failFast = failFast
      )

    private def mapRecoverIOIterable[R: ClassTag](iterable: Iterable[A],
                                                  block: A => IO[E, R],
                                                  recover: (Slice[R], IO.Left[E, Slice[R]]) => Unit = (_: Slice[R], _: IO.Left[E, Slice[R]]) => (),
                                                  failFast: Boolean = true): IO[E, Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, Slice[R]]] = None
      val results = Slice.of[R](iterable.size)

      while ((!failFast || failure.isEmpty) && it.hasNext) {
        block(it.next()) match {
          case IO.Right(value) =>
            results add value

          case failed @ IO.Left(_) =>
            failure = Some(IO.Left[E, Slice[R]](failed.value))
        }
      }

      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Right[E, Slice[R]](results)
      }
    }

    def mapRecover[R: ClassTag](block: A => R,
                                recover: (Slice[R], Throwable) => Unit = (_: Slice[R], _: Throwable) => (),
                                failFast: Boolean = true): Slice[R] = {
      val it = iterable.iterator
      var failure: Throwable = null
      val successes = Slice.of[R](iterable.size)

      while ((!failFast || failure == null) && it.hasNext) {
        try
          successes add block(it.next())
        catch {
          case exception: Throwable =>
            failure = exception
        }
      }

      if (failure == null) {
        successes
      } else {
        recover(successes, failure)
        throw failure
      }
    }

    def flatMapRecoverIO[R](ioBlock: A => IO[E, Iterable[R]],
                            recover: (Iterable[R], IO.Left[E, Slice[R]]) => Unit = (_: Iterable[R], _: IO.Left[E, Iterable[R]]) => (),
                            failFast: Boolean = true): IO[E, Iterable[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, Slice[R]]] = None
      val results = ListBuffer.empty[R]

      while ((!failFast || failure.isEmpty) && it.hasNext) {
        ioBlock(it.next()) match {
          case IO.Right(value) =>
            value foreach (results += _)

          case failed @ IO.Left(_) =>
            failure = Some(IO.Left[E, Slice[R]](failed.value))
        }
      }

      failure match {
        case Some(value) =>
          recover(results, value)
          value

        case None =>
          IO.Right[E, Iterable[R]](results)
      }
    }

    def flatMapRecoverThrowable[R](ioBlock: A => Iterable[R],
                                   recover: (Iterable[R], Throwable) => Unit = (_: Iterable[R], _: Throwable) => (),
                                   failFast: Boolean = true): Iterable[R] = {
      val it = iterable.iterator
      var failure: Option[Throwable] = None
      val results = ListBuffer.empty[R]

      while ((!failFast || failure.isEmpty) && it.hasNext)
        try
          ioBlock(it.next()) foreach (results += _)
        catch {
          case throwable: Throwable =>
            failure = Some(throwable)
        }

      failure match {
        case Some(throwable) =>
          recover(results, throwable)
          throw throwable

        case None =>
          results
      }
    }

    def foldLeftRecoverIO[R](r: R,
                             failFast: Boolean = true,
                             recover: (R, IO.Left[E, R]) => Unit = (_: R, _: IO.Left[E, R]) => ())(f: (R, A) => IO[E, R]): IO[E, R] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, R]] = None
      var result: R = r

      while ((!failFast || failure.isEmpty) && it.hasNext) {
        f(result, it.next()) match {
          case IO.Right(value) =>
            if (failure.isEmpty)
              result = value

          case failed @ IO.Left(_) =>
            failure = Some(IO.Left[E, R](failed.value))
        }
      }

      failure match {
        case Some(failure) =>
          recover(result, failure)
          failure

        case None =>
          IO.Right[E, R](result)
      }
    }

    def foldLeftRecover[R](r: R,
                           failFast: Boolean = true,
                           recover: (R, Throwable) => Unit = (_: R, _: Throwable) => ())(f: (R, A) => R): R = {
      val it = iterable.iterator
      var failure: Option[Throwable] = None
      var result: R = r

      while ((!failFast || failure.isEmpty) && it.hasNext)
        try
          result = f(result, it.next())
        catch {
          case throwable: Throwable =>
            failure = Some(throwable)
        }

      failure match {
        case Some(failure) =>
          recover(result, failure)
          throw failure

        case None =>
          result
      }
    }
  }

}
