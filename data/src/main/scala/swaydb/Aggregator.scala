/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
