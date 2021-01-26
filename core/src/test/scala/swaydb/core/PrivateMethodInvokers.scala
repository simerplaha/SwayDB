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

package swaydb.core

import org.scalatest.PrivateMethodTester._
import swaydb.core.map.timer.Timer
import swaydb.core.map.{Map, MapCache, Maps}
import swaydb.core.segment.PersistentSegment
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.util.{HashedMap, IDGenerator}
import swaydb.core.util.queue.VolatileQueue
import swaydb.data.slice.Slice

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}

object PrivateMethodInvokers {

  def getMaps[K, V, C <: MapCache[K, V]](maps: Maps[K, V, C]): VolatileQueue[Map[K, V, C]] = {
    val function = PrivateMethod[VolatileQueue[Map[K, V, C]]](Symbol("queue"))
    maps.invokePrivate(function())
  }

  def getTimer[K, V, C <: MapCache[K, V]](maps: Maps[K, V, C]): Timer = {
    val function = PrivateMethod[Timer](Symbol("timer"))
    maps.invokePrivate(function())
  }

  def getJavaMap[K, OV, V <: OV](maps: HashedMap.Concurrent[K, OV, V]): ConcurrentHashMap[K, V] = {
    val function = PrivateMethod[ConcurrentHashMap[K, V]](Symbol("map"))
    maps.invokePrivate(function())
  }

  def getSegmentsCache(segment: PersistentSegment): ConcurrentSkipListMap[Slice[Byte], SegmentRef] = {
    val function = PrivateMethod[ConcurrentSkipListMap[Slice[Byte], SegmentRef]](Symbol("segmentsCache"))
    segment.invokePrivate(function())
  }

  def getAtomicLong(generator: IDGenerator): AtomicLong = {
    val function = PrivateMethod[AtomicLong](Symbol("atomicID"))
    generator.invokePrivate(function())
  }

}
