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

  def result: T

  final override def apply(item: A): Unit =
    add(item)
}

protected object Aggregator {

  /**
   * Allows creating [[Aggregator]] instances on demand.
   */
  trait CreatorSizeable[-A, +T] {
    def createNewSizeHint(size: Int): Aggregator[A, T]
  }

  trait Creator[-A, +T] extends CreatorSizeable[A, T] {
    def createNew(): Aggregator[A, T]
  }

  case object Creator {
    def listBuffer[A](): Aggregator.Creator[A, ListBuffer[A]] =
      new Creator[A, ListBuffer[A]] {
        override def createNew(): Aggregator[A, ListBuffer[A]] =
          Aggregator.listBuffer[A]

        override def createNewSizeHint(size: Int): Aggregator[A, ListBuffer[A]] =
          createNew()
      }

    def slice[A: ClassTag]() =
      new CreatorSizeable[A, Slice[A]] {
        override def createNewSizeHint(size: Int): Aggregator[A, Slice[A]] =
          Slice.newAggregator(size)
      }

    def nothing[A]() =
      new Creator[A, Nothing] {
        override def createNew(): Aggregator[A, Nothing] = throw new Exception(s"Cannot create Nothing ${Creator.productPrefix}")
        override def createNewSizeHint(size: Int): Aggregator[A, Nothing] = createNew()
      }
  }

  def fromBuilder[A, T](builder: mutable.Builder[A, T]): Aggregator[A, T] =
    new Aggregator[A, T] {
      override def add(item: A): Unit =
        builder += item

      override def result: T =
        builder.result()
    }

  def listBuffer[A]: Aggregator[A, ListBuffer[A]] =
    Aggregator.fromBuilder(ListBuffer.newBuilder)
}
