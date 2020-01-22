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
 */

package swaydb.data.stream.step

import swaydb.Monad._
import swaydb.data.stream.step
import swaydb.{Bag, Monad}

import scala.annotation.tailrec

private[swaydb] object Step {

  def foldLeft[A, B, T[_]](initial: B, after: Option[A], stream: swaydb.Stream[A], drop: Int, take: Option[Int])(f: (B, A) => B)(implicit bag: Bag[T]): T[B] =
    bag match {
      case bag: Bag.Sync[T] =>
        step.Step.foldLeftSync(initial, after, stream, drop, take)(f)(bag)

      case bag: Bag.Async[T] =>
        bag.point(step.Step.foldLeftAsync(initial, after, stream, drop, take, f)(bag.monad, bag))
    }

  def collectFirst[A, T[_]](previous: A, stream: swaydb.Stream[A])(condition: A => Boolean)(implicit bag: Bag[T]): T[Option[A]] =
    bag match {
      case bag: Bag.Sync[T] =>
        step.Step.collectFirstSync(previous, stream)(condition)(bag)

      case bag: Bag.Async[T] =>
        bag.point(step.Step.collectFirstAsync(previous, stream, condition)(bag.monad, bag))
    }

  def foldLeftSync[A, U, T[_]](initial: U, after: Option[A], stream: swaydb.Stream[A], drop: Int, take: Option[Int])(operation: (U, A) => U)(implicit bag: Bag.Sync[T]): T[U] = {
    @tailrec
    def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): T[U] =
      if (take.contains(currentSize)) {
        bag.success(previousResult)
      } else {
        val nextBagged = stream.next(previous)(bag)
        if (bag.isSuccess(nextBagged)) {
          bag.getUnsafe(nextBagged) match {
            case Some(next) =>
              if (drop >= 1) {
                fold(next, drop - 1, currentSize, previousResult)
              } else {
                val nextResult =
                  try {
                    operation(previousResult, next)
                  } catch {
                    case exception: Throwable =>
                      return bag.failure(exception)
                  }
                fold(next, drop, currentSize + 1, nextResult)
              }

            case None =>
              bag.success(previousResult)
          }
        } else {
          nextBagged.asInstanceOf[T[U]]
        }
      }

    if (take.contains(0)) {
      bag.success(initial)
    } else {
      val someFirstBagged = after.map(stream.next(_)(bag)).getOrElse(stream.headOption(bag))
      if (bag.isSuccess(someFirstBagged)) {
        bag.getUnsafe(someFirstBagged) match {
          case Some(first) =>
            if (drop >= 1)
              fold(first, drop - 1, 0, initial)
            else {
              val next =
                try {
                  operation(initial, first)
                } catch {
                  case throwable: Throwable =>
                    return bag.failure(throwable)
                }
              fold(first, drop, 1, next)
            }

          case None =>
            bag.success(initial)
        }
      } else {
        someFirstBagged.asInstanceOf[T[U]]
      }
    }
  }

  @tailrec
  def collectFirstSync[A, T[_]](previous: A, stream: swaydb.Stream[A])(condition: A => Boolean)(implicit bag: Bag.Sync[T]): T[Option[A]] = {
    val next = stream.next(previous)(bag)
    if (bag.isSuccess(next)) {
      bag.getUnsafe(next) match {
        case Some(nextA) =>
          if (condition(nextA))
            next
          else
            collectFirstSync(nextA, stream)(condition)(bag)

        case None =>
          bag.none
      }
    } else {
      next
    }
  }

  def foldLeftAsync[A, U, T[_]](initial: U, after: Option[A], stream: swaydb.Stream[A], drop: Int, take: Option[Int], operation: (U, A) => U)(implicit monad: Monad[T],
                                                                                                                                              bag: Bag.Async[T]): T[U] = {
    def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): T[U] =
      if (take.contains(currentSize))
        monad.success(previousResult)
      else
        stream
          .next(previous)(bag)
          .flatMap {
            case Some(next) =>
              if (drop >= 1) {
                fold(next, drop - 1, currentSize, previousResult)
              } else {
                try {
                  val newResult = operation(previousResult, next)
                  fold(next, drop, currentSize + 1, newResult)
                } catch {
                  case throwable: Throwable =>
                    monad.failed(throwable)
                }
              }

            case None =>
              monad.success(previousResult)
          }

    if (take.contains(0))
      monad.success(initial)
    else
      after
        .map(stream.next(_)(bag))
        .getOrElse(stream.headOption(bag))
        .flatMap {
          case Some(first) =>
            if (drop >= 1) {
              fold(first, drop - 1, 0, initial)
            } else {
              try {
                val nextResult = operation(initial, first)
                fold(first, drop, 1, nextResult)
              } catch {
                case throwable: Throwable =>
                  monad.failed(throwable)
              }
            }

          case None =>
            monad.success(initial)
        }
  }

  def collectFirstAsync[A, T[_]](previous: A, stream: swaydb.Stream[A], condition: A => Boolean)(implicit monad: Monad[T],
                                                                                                 bag: Bag.Async[T]): T[Option[A]] =
    stream
      .next(previous)(bag)
      .flatMap {
        case some @ Some(nextA) =>
          if (condition(nextA))
            monad.success(some)
          else
            collectFirstAsync(nextA, stream, condition)

        case None =>
          monad.success(None)
      }
}
