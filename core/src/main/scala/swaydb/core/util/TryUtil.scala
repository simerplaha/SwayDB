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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import swaydb.core.io.reader.Reader
import swaydb.core.util.CollectionUtil._
import swaydb.data.io.IO
import swaydb.data.slice.Slice

object IOUtil {

  val successUnit: IO.Success[Unit] = IO.Success()
  val successNone = IO.Success(None)
  val successFalse = IO.Success(false)
  val successTrue = IO.Success(true)
  val emptyReader = IO.Success(Reader(Slice.emptyBytes))
  val successZero = IO.Success(0)
  val successEmptyBytes = IO.Success(Slice.emptyBytes)
  val successNoneTime = IO.Success(None)
  val successEmptySeqBytes = IO.Success(Seq.empty[Slice[Byte]])


  implicit class IterableIOImplicit[T: ClassTag](iterable: Iterable[T]) {

    def tryForeach[R](f: T => IO[R], failFast: Boolean = true): Option[IO.Failure[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) match {
          case failed @ IO.Failure(_) =>
            failure = Some(failed)
          case _ =>
        }
      }
      failure
    }

    //returns the first IO.Success(Some(_)).
    def tryUntilSome[R](f: T => IO[Option[R]]): IO[Option[(R, T)]] = {
      var result: IO[Option[(R, T)]] = IOUtil.successNone
      iterable.iterator foreachBreak {
        item =>
          f(item) match {
            case IO.Success(Some(value)) =>
              result = IO.Success(Some(value, item))
              true
            case IO.Success(None) =>
              false
            case IO.Failure(exception) =>
              result = IO.Failure(exception)
              true
          }
      }
      result
    }

    def tryMap[R: ClassTag](tryBlock: T => IO[R],
                            recover: (Slice[R], IO.Failure[Slice[R]]) => Unit = (_: Slice[R], _: IO.Failure[Slice[R]]) => (),
                            failFast: Boolean = true): IO[Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[Slice[R]]] = None
      val results = Slice.create[R](iterable.size)
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        tryBlock(it.next()) match {
          case IO.Success(value) =>
            results add value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.exception))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Success(results)
      }
    }

    def tryFlattenIterable[R: ClassTag](tryBlock: T => IO[Iterable[R]],
                                        recover: (Iterable[R], IO.Failure[Slice[R]]) => Unit = (_: Iterable[R], _: IO.Failure[Iterable[R]]) => (),
                                        failFast: Boolean = true): IO[Iterable[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[Slice[R]]] = None
      val results = ListBuffer.empty[R]
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        tryBlock(it.next()) match {
          case IO.Success(value) =>
            value foreach (results += _)

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.exception))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Success(results)
      }
    }

    def tryFoldLeft[R: ClassTag](r: R,
                                 failFast: Boolean = true,
                                 recover: (R, IO.Failure[R]) => Unit = (_: R, _: IO.Failure[R]) => ())(f: (R, T) => IO[R]): IO[R] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[R]] = None
      var result: R = r
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        f(result, it.next()) match {
          case IO.Success(value) =>
            if (failure.isEmpty)
              result = value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.exception))
        }
      }
      failure match {
        case Some(failure) =>
          recover(result, failure)
          failure
        case None =>
          IO.Success(result)
      }
    }
  }

  def tryOrNone[T](tryThis: => T): Option[T] =
    try
      Option(tryThis)
    catch {
      case ex: Exception =>
        None
    }

}
