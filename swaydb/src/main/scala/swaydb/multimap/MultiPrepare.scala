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
        aggregator add MultiPrepare(map, prepare)
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
