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

  def foldLeft[A, B, T[_]](initial: B, afterOrNull: A, stream: swaydb.Stream[A], drop: Int, take: Option[Int])(f: (B, A) => B)(implicit bag: Bag[T]): T[B] =
    bag match {
      case bag: Bag.Sync[T] =>
        step.Step.foldLeftSync(initial, afterOrNull, stream, drop, take)(f)(bag)

      case bag: Bag.Async[T] =>
        bag.point(step.Step.foldLeftAsync(initial, afterOrNull, stream, drop, take, f)(bag.monad, bag))
    }

  def collectFirst[A, T[_]](previous: A, stream: swaydb.Stream[A])(condition: A => Boolean)(implicit bag: Bag[T]): T[A] =
    bag match {
      case bag: Bag.Sync[T] =>
        step.Step.collectFirstSync(previous, stream)(condition)(bag)

      case bag: Bag.Async[T] =>
        bag.point(step.Step.collectFirstAsync(previous, stream, condition)(bag.monad, bag))
    }

  def foldLeftSync[A, U, T[_]](initial: U, afterOrNull: A, stream: swaydb.Stream[A], drop: Int, take: Option[Int])(operation: (U, A) => U)(implicit bag: Bag.Sync[T]): T[U] = {
    @tailrec
    def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): T[U] =
      if (take.contains(currentSize)) {
        bag.success(previousResult)
      } else {
        val nextBagged = stream.nextOrNull(previous)(bag)
        if (bag.isSuccess(nextBagged)) {
          val next = bag.getUnsafe(nextBagged)
          if (next == null)
            bag.success(previousResult)
          else if (drop >= 1) {
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
        } else {
          nextBagged.asInstanceOf[T[U]]
        }
      }

    if (take.contains(0)) {
      bag.success(initial)
    } else {
      val someFirstBagged =
        if (afterOrNull == null)
          stream.headOrNull(bag)
        else
          stream.nextOrNull(afterOrNull)(bag)

      if (bag.isSuccess(someFirstBagged)) {
        val first = bag.getUnsafe(someFirstBagged)
        if (first == null)
          bag.success(initial)
        else if (drop >= 1)
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
      } else {
        someFirstBagged.asInstanceOf[T[U]]
      }
    }
  }

  @tailrec
  def collectFirstSync[A, T[_]](previous: A, stream: swaydb.Stream[A])(condition: A => Boolean)(implicit bag: Bag.Sync[T]): T[A] = {
    val next = stream.nextOrNull(previous)(bag)
    if (bag.isSuccess(next)) {
      val nextA = bag.getUnsafe(next)
      if (nextA == null || condition(nextA))
        next
      else
        collectFirstSync(nextA, stream)(condition)(bag)
    } else {
      next
    }
  }

  def foldLeftAsync[A, U, T[_]](initial: U, afterOrNull: A, stream: swaydb.Stream[A], drop: Int, take: Option[Int], operation: (U, A) => U)(implicit monad: Monad[T],
                                                                                                                                            bag: Bag.Async[T]): T[U] = {
    def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): T[U] =
      if (take.contains(currentSize))
        monad.success(previousResult)
      else
        stream
          .nextOrNull(previous)(bag)
          .flatMap {
            case null =>
              monad.success(previousResult)

            case next =>
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
          }

    if (take.contains(0)) {
      monad.success(initial)
    } else {
      val someFirstBagged =
        if (afterOrNull == null)
          stream.headOrNull(bag)
        else
          stream.nextOrNull(afterOrNull)(bag)

      someFirstBagged
        .flatMap {
          case null =>
            monad.success(initial)

          case first =>
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
        }
    }
  }

  def collectFirstAsync[A, T[_]](previous: A, stream: swaydb.Stream[A], condition: A => Boolean)(implicit monad: Monad[T],
                                                                                                 bag: Bag.Async[T]): T[A] =
    stream
      .nextOrNull(previous)(bag)
      .flatMap {
        case null =>
          monad.success(null.asInstanceOf[A])

        case nextA =>
          if (condition(nextA))
            monad.success(nextA)
          else
            collectFirstAsync(nextA, stream, condition)
      }
}
