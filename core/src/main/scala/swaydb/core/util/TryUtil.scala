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

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import swaydb.core.io.reader.Reader
import swaydb.core.util.CollectionUtil._
import swaydb.data.slice.Slice

object TryUtil {

  val successUnit: Success[Unit] = Success()
  val successNone = Success(None)
  val successFalse = Success(false)
  val successTrue = Success(true)
  val emptyReader = Success(Reader(Slice.emptyBytes))
  val successZero = Success(0)
  val successEmptyBytes = Success(Slice.emptyBytes)
  val successNoneTime = Success(None)
  val successEmptySeqBytes = Success(Seq.empty[Slice[Byte]])

  object Catch {
    def apply[T](f: => Try[T]): Try[T] =
      try
        f
      catch {
        case ex: Exception =>
          Failure(ex)
      }
  }

  implicit class IterableTryImplicit[T: ClassTag](iterable: Iterable[T]) {

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

    //returns the first Success(Some(_)).
    def tryUntilSome[R](f: T => Try[Option[R]]): Try[Option[(R, T)]] = {
      var result: Try[Option[(R, T)]] = TryUtil.successNone
      iterable.iterator foreachBreak {
        item =>
          f(item) match {
            case Success(Some(value)) =>
              result = Success(Some(value, item))
              true
            case Success(None) =>
              false
            case Failure(exception) =>
              result = Failure(exception)
              true
          }
      }
      result
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

    def tryFlattenIterable[R: ClassTag](tryBlock: T => Try[Iterable[R]],
                                        recover: (Iterable[R], Failure[Slice[R]]) => Unit = (_: Iterable[R], _: Failure[Iterable[R]]) => (),
                                        failFast: Boolean = true): Try[Iterable[R]] = {
      val it = iterable.iterator
      var failure: Option[Failure[Slice[R]]] = None
      val results = ListBuffer.empty[R]
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        tryBlock(it.next()) match {
          case Success(value) =>
            value foreach (results += _)

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
        case Some(failure) =>
          recover(result, failure)
          failure
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
