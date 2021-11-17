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

package swaydb.core.segment.ref.search

import swaydb.core.series.map.LimitHashMap

import java.nio.file.Path
import scala.util.Random

private[swaydb] sealed trait ThreadReadState {
  def getSegmentState(path: Path): SegmentReadStateOption
  def setSegmentState(path: Path, nextIndexOffset: SegmentReadState): Unit
}

private[swaydb] object ThreadReadState {

  def hashMap(): ThreadReadState =
    new HashMapState(new java.util.HashMap[Path, SegmentReadState]())

  def limitHashMap(maxSize: Int,
                   probe: Int): ThreadReadState =
    new LimitHashMapState(LimitHashMap[Path, SegmentReadState](maxSize, probe))

  def limitHashMap(maxSize: Int): ThreadReadState =
    new LimitHashMapState(LimitHashMap[Path, SegmentReadState](maxSize))

  private class HashMapState(map: java.util.HashMap[Path, SegmentReadState]) extends ThreadReadState {

    def getSegmentState(path: Path): SegmentReadStateOption = {
      val state = map.get(path)
      if (state == null)
        SegmentReadState.Null
      else
        state
    }

    def setSegmentState(path: Path, nextIndexOffset: SegmentReadState): Unit =
      map.put(path, nextIndexOffset)
  }

  private class LimitHashMapState(map: LimitHashMap[Path, SegmentReadState]) extends ThreadReadState {

    def getSegmentState(path: Path): SegmentReadStateOption = {
      val state = map.getOrNull(path)
      if (state == null)
        SegmentReadState.Null
      else
        state
    }

    def setSegmentState(path: Path, nextIndexOffset: SegmentReadState): Unit =
      map.put(path, nextIndexOffset)

    override def toString: String =
      map.toString
  }

  def random: ThreadReadState =
    if (scala.util.Random.nextBoolean())
      ThreadReadState.hashMap()
    else if (scala.util.Random.nextBoolean())
      ThreadReadState.limitHashMap(10, Random.nextInt(10))
    else
      ThreadReadState.limitHashMap(20)
}
