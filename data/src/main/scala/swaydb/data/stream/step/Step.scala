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

import swaydb.Bag
import swaydb.data.stream.step

import scala.annotation.tailrec

private[swaydb] object Step {

  def foldLeft[A, B, BAG[_]](initial: B, afterOrNull: A, stream: swaydb.Stream[A], drop: Int, take: Option[Int])(f: (B, A) => B)(implicit bag: Bag[BAG]): BAG[B] =
    bag match {
      case bag: Bag.Sync[BAG] =>
        bag.safe(step.Step.foldLeftSync(initial, afterOrNull, stream, drop, take)(f)(bag))

      case bag: Bag.Async[BAG] =>
        bag.safe(step.Step.foldLeftAsync(initial, afterOrNull, stream, drop, take, f)(bag))
    }

  def collectFirst[A, BAG[_]](previous: A, stream: swaydb.Stream[A])(condition: A => Boolean)(implicit bag: Bag[BAG]): BAG[A] =
    bag match {
      case bag: Bag.Sync[BAG] =>
        bag.safe(step.Step.collectFirstSync(previous, stream)(condition)(bag))

      case bag: Bag.Async[BAG] =>
        bag.safe(step.Step.collectFirstAsync(previous, stream, condition)(bag))
    }

  private def foldLeftSync[A, U, BAG[_]](initial: U, afterOrNull: A, stream: swaydb.Stream[A], drop: Int, take: Option[Int])(operation: (U, A) => U)(implicit bag: Bag.Sync[BAG]): BAG[U] = {
    @tailrec
    def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): BAG[U] =
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
            val nextResult = operation(previousResult, next)
            fold(next, drop, currentSize + 1, nextResult)
          }
        } else {
          nextBagged.asInstanceOf[BAG[U]]
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
          val next = operation(initial, first)
          fold(first, drop, 1, next)
        }
      } else {
        someFirstBagged.asInstanceOf[BAG[U]]
      }
    }
  }

  @tailrec
  private def collectFirstSync[A, BAG[_]](previous: A, stream: swaydb.Stream[A])(condition: A => Boolean)(implicit bag: Bag.Sync[BAG]): BAG[A] = {
    val next = stream.nextOrNull(previous)(bag)
    if (bag.isSuccess(next)) {
      val firstMaybe = bag.getUnsafe(next)
      if (firstMaybe == null || condition(firstMaybe))
        next
      else
        collectFirstSync(firstMaybe, stream)(condition)(bag)
    } else {
      next
    }
  }

  private def foldLeftAsync[A, U, BAG[_]](initial: U, afterOrNull: A, stream: swaydb.Stream[A], drop: Int, take: Option[Int], operation: (U, A) => U)(implicit bag: Bag.Async[BAG]): BAG[U] = {
    def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): BAG[U] =
      if (take.contains(currentSize))
        bag.success(previousResult)
      else
        bag.flatMap(stream.nextOrNull(previous)(bag)) {
          case null =>
            bag.success(previousResult)

          case next =>
            if (drop >= 1) {
              fold(next, drop - 1, currentSize, previousResult)
            } else {
              val newResult = operation(previousResult, next)
              fold(next, drop, currentSize + 1, newResult)
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

      bag.flatMap(someFirstBagged) {
        case null =>
          bag.success(initial)

        case first =>
          if (drop >= 1) {
            fold(first, drop - 1, 0, initial)
          } else {
            val nextResult = operation(initial, first)
            fold(first, drop, 1, nextResult)
          }
      }
    }
  }

  private def collectFirstAsync[A, BAG[_]](previous: A, stream: swaydb.Stream[A], condition: A => Boolean)(implicit bag: Bag.Async[BAG]): BAG[A] =
    bag.flatMap(stream.nextOrNull(previous)(bag)) {
      case null =>
        bag.success(null.asInstanceOf[A])

      case nextA =>
        if (condition(nextA))
          bag.success(nextA)
        else
          collectFirstAsync(nextA, stream, condition)
    }
}
