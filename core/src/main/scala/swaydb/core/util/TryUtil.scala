/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import swaydb.data.slice.Slice

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[core] object TryUtil {

  final val successUnit: Success[Unit] = Success()

  implicit class IterableTryImplicit[T: ClassTag](iterable: Iterable[T]) {

    def foreachBreak[R](f: T => Boolean): Unit = {
      val it = iterable.iterator
      var break: Boolean = false
      while (it.hasNext && !break)
        break = f(it.next())
    }

    def tryForeach[R](f: T => Try[R], failFast: Boolean = true): Option[Failure[R]] = {
      val it = iterable.iterator
      var failure: Option[Failure[R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) match {
          case failed @ Failure(_) =>
            failure = Some(failed)
          case _ =>
        }
      }
      failure
    }

    def tryMap[R: ClassTag](tryBlock: T => Try[R],
                            recover: (Slice[R], Failure[Slice[R]]) => Unit = (_: Slice[R], _: Failure[Slice[R]]) => (),
                            failFast: Boolean = true): Try[Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[Failure[Slice[R]]] = None
      val results = Slice.create[R](iterable.size)
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        tryBlock(it.next()) match {
          case Success(value) =>
            results add value

          case failed @ Failure(_) =>
            failure = Some(Failure(failed.exception))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          Success(results)
      }
    }

    def tryFoldLeft[R: ClassTag](r: R,
                                 failFast: Boolean = true,
                                 recover: (R, Failure[R]) => Unit = (_: R, _: Failure[R]) => ())(f: (R, T) => Try[R]): Try[R] = {
      val it = iterable.iterator
      var failure: Option[Failure[R]] = None
      var result: R = r
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        f(result, it.next()) match {
          case Success(value) =>
            if (failure.isEmpty)
              result = value

          case failed @ Failure(_) =>
            failure = Some(Failure(failed.exception))
        }
      }
      failure match {
        case Some(value) =>
          recover(result, value)
          value
        case None =>
          Success(result)
      }
    }
  }

  implicit class TryImplicits[T](tryBlock: => Try[T]) {
    /**
      * Runs the tryBlock in a Future asynchronously and returns the result of
      * the Try in Future.
      */
    def tryInFuture(implicit ec: ExecutionContext): Future[T] =
      Future(tryBlock).flatMap(Future.fromTry)
  }

  def tryOrNone[T](tryThis: => T): Option[T] =
    try
      Option(tryThis)
    catch {
      case ex: Exception =>
        None
    }

}