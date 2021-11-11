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

package swaydb.multimap

import swaydb.{Aggregator, MultiMap, Prepare}

import scala.collection.mutable
import scala.concurrent.duration.Deadline

object MultiPrepare {

  def apply[M, K, V, F, BAG[_]](map: MultiMap[M, K, V, F, BAG],
                                prepare: Prepare[K, V, F]*): Seq[MultiPrepare[M, K, V, F]] =
    prepare.map(MultiPrepare(map, _))

  def apply[M, K, V, F, BAG[_]](map: MultiMap[M, K, V, F, BAG],
                                prepare: Iterable[Prepare[K, V, F]]): Iterable[MultiPrepare[M, K, V, F]] =
    prepare.map(MultiPrepare(map, _))

  def apply[M, K, V, F, BAG[_]](map: MultiMap[M, K, V, F, BAG],
                                prepare: Prepare[K, V, F]): MultiPrepare[M, K, V, F] =
    new MultiPrepare(map.mapId, map.defaultExpiration, prepare)

  def builder[M, K, V, F, BAG[_], C[_]](map: MultiMap[M, K, V, F, BAG],
                                        prepare: Iterable[Prepare[K, V, F]])(implicit builder: mutable.Builder[MultiPrepare[M, K, V, F], C[MultiPrepare[M, K, V, F]]]): mutable.Builder[MultiPrepare[M, K, V, F], C[MultiPrepare[M, K, V, F]]] = {
    prepare foreach {
      prepare =>
        builder += MultiPrepare(map, prepare)
    }

    builder
  }

  def aggregator[M, K, V, F, BAG[_], C[_]](map: MultiMap[M, K, V, F, BAG],
                                           prepare: Iterable[Prepare[K, V, F]])(implicit aggregator: Aggregator[MultiPrepare[M, K, V, F], C[MultiPrepare[M, K, V, F]]]): Aggregator[MultiPrepare[M, K, V, F], C[MultiPrepare[M, K, V, F]]] = {
    prepare foreach {
      prepare =>
        aggregator addOne MultiPrepare(map, prepare)
    }

    aggregator
  }
}

/**
 * Holds [[Prepare]] statements which than get converted to [[MultiMap.innerMap]]'s [[Prepare]] type.
 *
 * @param mapId             [[MultiMap]] key's
 * @param defaultExpiration [[MultiMap]] default expiration
 * @param prepare           The [[Prepare]] statemented created for the [[MultiMap]]
 */
case class MultiPrepare[+M, +K, +V, +F](mapId: Long,
                                        defaultExpiration: Option[Deadline],
                                        prepare: Prepare[K, V, F]) { self =>
  def ++[M2 >: M, K2 >: K, V2 >: V, F2 >: F](other: MultiPrepare[M2, K2, V2, F2]): List[MultiPrepare[M2, K2, V2, F2]] =
    List(self, other)

  def concatJava[M2 >: M, K2 >: K, V2 >: V, F2 >: F](other: MultiPrepare[M2, K2, V2, F2]): java.util.stream.Stream[MultiPrepare[M2, K2, V2, F2]] =
    java.util.stream.Stream.of(self, other)
}
