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

package swaydb.java.multimap

import swaydb.Prepare
import swaydb.java.MultiMap
import swaydb.multimap.MultiPrepare
import swaydb.utils.Aggregator

import java.util
import java.util.stream.Stream
import scala.collection.compat.IterableOnce
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

        override def addOne(item: MultiPrepare[M, K, V, F]): this.type = {
          builder.accept(item)
          this
        }

        override def addAll(items: IterableOnce[MultiPrepare[M, K, V, F]]): this.type = {
          items foreach builder.accept
          this
        }

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

        override def addOne(item: MultiPrepare[M, K, V, F]): this.type = {
          list.add(item)
          this
        }

        override def addAll(items: IterableOnce[MultiPrepare[M, K, V, F]]): this.type = {
          items foreach list.add
          this
        }

        override def result: util.ArrayList[MultiPrepare[M, K, V, F]] =
          list
      }

    swaydb.multimap.MultiPrepare.aggregator(
      map = map.asScala,
      prepare = prepare.asScala
    ).result
  }

}
