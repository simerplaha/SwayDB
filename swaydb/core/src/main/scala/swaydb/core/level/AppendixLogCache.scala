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

package swaydb.core.level

import swaydb.Glass
import swaydb.core.log.{LogCache, LogCacheBuilder, LogEntry}
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.slice.order.KeyOrder
import swaydb.slice.{Slice, SliceOption}

import scala.reflect.ClassTag

/**
 * Default [[SkipListMerger]] implementation for Level's Appendix. Currently appendix does not implement
 * Range APIs so merger should never be used.
 */

object AppendixLogCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    new LogCacheBuilder[AppendixLogCache] {
      override def create(): AppendixLogCache =
        new AppendixLogCache(SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null))
    }
}

/**
 * In history of this file's commit at revision "992965da26cba711c7b42a9a555d04de46b9fa37 - backup atomicSkipList for AppendixMapCache"
 * uses [[swaydb.core.skiplist.SkipList.atomicRead]] and [[swaydb.core.skiplist.SkipList.atomicRead]] but
 * for Appendix file batch is more efficient because millions of concurrent reads will require concurrent writes to the atomic SkipList
 * whereas batch only requires a single copy of the SkipList which is more efficient.
 */
class AppendixLogCache(skipList: SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment]) extends LogCache[Slice[Byte], Segment] {

  override def writeAtomic(entry: LogEntry[Slice[Byte], Segment]): Unit =
    entry applyBatch skipList

  override def writeNonAtomic(entry: LogEntry[Slice[Byte], Segment]): Unit =
    entry.entries.foreach(_.applyPoint(skipList))

  def values(): Iterable[Segment] =
    skipList.values()

  def subMapValues(from: Slice[Byte], fromInclusive: Boolean, to: Slice[Byte], toInclusive: Boolean): Iterable[Segment] =
    skipList.subMapValues(from, fromInclusive, to, toInclusive)

  def floor(key: Slice[Byte]): SegmentOption =
    skipList.floor(key)

  def lower(key: Slice[Byte]): SegmentOption =
    skipList.lower(key)

  def higher(key: Slice[Byte]): SegmentOption =
    skipList.higher(key)

  def headKey(): SliceOption[Byte] =
    skipList.headKey

  def head(): Glass[SegmentOption] =
    skipList.head()

  def maxKey(): SliceOption[Byte] =
    last().flatMapSomeS(Slice.Null: SliceOption[Byte])(_.maxKey.maxKey)

  def last(): SegmentOption =
    skipList.last()

  def get(key: Slice[Byte]): SegmentOption =
    skipList.get(key)

  def contains(key: Slice[Byte]): Boolean =
    skipList.contains(key)

  def foreach[R](f: (Slice[Byte], Segment) => R): Unit =
    skipList.foreach(f)

  def foldLeft[R](r: R)(f: (R, (Slice[Byte], Segment)) => R): R =
    skipList.foldLeft(r)(f)

  def take(count: Int)(implicit classTag: ClassTag[Segment]): Slice[Segment] =
    skipList.take(count)

  def size: Int =
    skipList.size

  override def iterator: Iterator[(Slice[Byte], Segment)] =
    skipList.iterator

  def nonEmpty(): Boolean =
    skipList.nonEmpty

  override def isEmpty: Boolean =
    skipList.isEmpty

  override def maxKeyValueCount: Int =
    skipList.size
}
