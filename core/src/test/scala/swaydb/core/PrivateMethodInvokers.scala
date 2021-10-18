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
