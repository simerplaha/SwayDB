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

package swaydb.core.map.applied

import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.util.skiplist.SkipListConcurrent
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}


object AppliedFunctionsMapCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    new MapCacheBuilder[AppliedFunctionsMapCache] {
      override def create(): AppliedFunctionsMapCache =
        AppliedFunctionsMapCache(
          SkipListConcurrent(
            nullKey = Slice.Null,
            nullValue = Slice.Null
          )
        )
    }
}

case class AppliedFunctionsMapCache(skipList: SkipListConcurrent[SliceOption[Byte], Slice.Null.type, Slice[Byte], Slice.Null.type]) extends MapCache[Slice[Byte], Slice.Null.type] {

  override def writeAtomic(entry: MapEntry[Slice[Byte], Slice.Null.type]): Unit =
    writeNonAtomic(entry) //AppliedFunctions do not need atomicity.

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Slice.Null.type]): Unit =
    entry.entries foreach {
      point =>
        point applyPoint skipList
    }

  override def iterator: Iterator[(Slice[Byte], Slice.Null.type)] =
    skipList.iterator

  override def isEmpty: Boolean =
    skipList.isEmpty

  override def maxKeyValueCount: Int =
    skipList.size

}
