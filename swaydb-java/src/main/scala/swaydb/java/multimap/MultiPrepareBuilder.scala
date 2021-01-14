/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.java.multimap

import java.util
import java.util.stream.Stream

import swaydb.java.MultiMap
import swaydb.multimap.MultiPrepare
import swaydb.{Aggregator, Prepare}

import scala.jdk.CollectionConverters._

object MultiPrepareBuilder {

  /**
   * Creates a single [[MultiPrepare]] instance from the given [[Prepare]] statements
   */
  def of[M, K, V, F](map: MultiMap[M, K, V, F],
                     prepare: Prepare[K, V, F]): MultiPrepare[M, K, V, F] =
    swaydb.multimap.MultiPrepare(
      map = map.asScala,
      prepare = prepare
    )

  /**
   * Adds the new [[MultiPrepare]] instances to the [[builder]].
   *
   * @return the same input builder
   */
  def stream[M, K, V, F](map: MultiMap[M, K, V, F],
                         prepare: java.lang.Iterable[Prepare[K, V, F]],
                         builder: Stream.Builder[MultiPrepare[M, K, V, F]]): Stream.Builder[MultiPrepare[M, K, V, F]] = {

    implicit val aggregator: Aggregator[MultiPrepare[M, K, V, F], Stream[MultiPrepare[M, K, V, F]]] =
      new Aggregator[MultiPrepare[M, K, V, F], Stream[MultiPrepare[M, K, V, F]]] {

        override def add(item: MultiPrepare[M, K, V, F]): Unit =
          builder.accept(item)

        override def result: Stream[MultiPrepare[M, K, V, F]] =
          builder.build()
      }

    swaydb.multimap.MultiPrepare.aggregator(
      map = map.asScala,
      prepare = prepare.asScala
    )

    builder
  }

  /**
   * Builds a [[Stream]] of [[MultiPrepare[M, K, V, F]]].
   */
  def stream[M, K, V, F](map: MultiMap[M, K, V, F],
                         prepare: java.lang.Iterable[Prepare[K, V, F]]): Stream[MultiPrepare[M, K, V, F]] =
    stream(
      map = map,
      prepare = prepare,
      builder = java.util.stream.Stream.builder[MultiPrepare[M, K, V, F]]()
    ).build()

  /**
   * Builds a [[List]] of [[MultiPrepare[M, K, V, F]]].
   */
  def list[M, K, V, F](map: MultiMap[M, K, V, F],
                       prepare: java.lang.Iterable[Prepare[K, V, F]]): util.ArrayList[MultiPrepare[M, K, V, F]] = {

    implicit val aggregator: Aggregator[MultiPrepare[M, K, V, F], util.ArrayList[MultiPrepare[M, K, V, F]]] =
      new Aggregator[MultiPrepare[M, K, V, F], util.ArrayList[MultiPrepare[M, K, V, F]]] {
        val list = new util.ArrayList[MultiPrepare[M, K, V, F]]()

        override def add(item: MultiPrepare[M, K, V, F]): Unit =
          list.add(item)

        override def result: util.ArrayList[MultiPrepare[M, K, V, F]] =
          list
      }

    swaydb.multimap.MultiPrepare.aggregator(
      map = map.asScala,
      prepare = prepare.asScala
    ).result
  }

}
