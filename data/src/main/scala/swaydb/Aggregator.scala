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

package swaydb

import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * [[scala.collection.mutable.Builder]] requires two implementations for 2.13 and 2.12.
 *
 * 2.13 requires addOne and 2.12 requires +=. So this type simply wrapper around
 * Builder which is used internally to avoid having 2 implementation of Builder.
 */
protected trait Aggregator[-A, +T] extends ForEach[A] {
  def add(item: A): Unit

  final def addAll(items: Iterable[A]): Unit =
    items foreach add

  def result: T

  final override def apply(item: A): Unit =
    add(item)
}

protected case object Aggregator {

  /**
   * Allows creating [[Aggregator]] instances on demand where the final maximum size of
   * the aggregator is know.
   */
  trait CreatorSizeable[-A, T] {
    def createNewSizeHint(size: Int): Aggregator[A, T]
  }

  /**
   * Allows creating [[Aggregator]] instances on demand.
   */
  trait Creator[-A, T] {

    def createNew(): Aggregator[A, T]

    def createNew(item: A): Aggregator[A, T] = {
      val fresh = createNew()
      fresh add item
      fresh
    }
  }

  case object Creator {
    def listBuffer[A](): Aggregator.Creator[A, ListBuffer[A]] =
      () =>
        Aggregator.listBuffer[A]

    def slice[A: ClassTag]() =
      new CreatorSizeable[A, Slice[A]] {
        override def createNewSizeHint(size: Int): Aggregator[A, Slice[A]] =
          Slice.newAggregator(size)
      }

    /**
     * Nothing disables inserting and adding elements the [[Aggregator]].
     *
     * TODO - remove the need for this type since it throws runtime exception.
     */
    def nothing[A](): Creator[A, Nothing] =
      () => Aggregator.nothingAggregator()
  }

  def fromBuilder[A, T](builder: mutable.Builder[A, T]): Aggregator[A, T] =
    new Aggregator[A, T] {
      override def add(item: A): Unit =
        builder += item

      override def result: T =
        builder.result()
    }

  /**
   * Type to disable aggregation.
   *
   * TODO - remove the need for this type since it throws runtime exception.
   */
  def nothingAggregator[A](): Aggregator[A, Nothing] =
    new Aggregator[A, Nothing] {
      override def add(item: A): Unit =
        throw new Exception(s"Cannot add to Nothing ${Aggregator.productPrefix}")

      override def result: Nothing =
        throw new Exception(s"Cannot fetch Nothing ${Aggregator.productPrefix}")
    }


  def listBuffer[A]: Aggregator[A, ListBuffer[A]] =
    new Aggregator[A, ListBuffer[A]] {
      val buffer = ListBuffer.empty[A]

      override def add(item: A): Unit =
        buffer += item

      override def result: ListBuffer[A] =
        buffer
    }
}
