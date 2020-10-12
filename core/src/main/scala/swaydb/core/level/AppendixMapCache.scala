/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level

import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.util.skiplist.SkipListConcurrent
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.{Bag, Glass}

import scala.reflect.ClassTag

/**
 * Default [[SkipListMerger]] implementation for Level's Appendix. Currently appendix does not implement
 * Range APIs so merger should never be used.
 */

object AppendixMapCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    new MapCacheBuilder[AppendixMapCache] {
      override def create(): AppendixMapCache =
        new AppendixMapCache(
          SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null),
          atomicSkipList = false
        )
    }
}

class AppendixMapCache(skipList: SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment],
                       atomicSkipList: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]) extends MapCache[Slice[Byte], Segment] {

  override def writeAtomic(entry: MapEntry[Slice[Byte], Segment]): Unit =
    if (atomicSkipList) {
      val entries = entry.entries
      if (entries.size == 1) {
        entries.head applyPoint skipList
      } else {
        val sorted = entries.sortBy(_.key)(keyOrder)

        val fromKey = sorted.head.key

        sorted.last match {
          case MapEntry.Put(_, last: Segment) =>
            last.maxKey match {
              case MaxKey.Fixed(maxKey) =>
                skipList.atomicWrite(from = fromKey, to = maxKey, toInclusive = true) {
                  entries.foreach(_.applyPoint(skipList))
                }(Bag.glass)

              case MaxKey.Range(_, maxKey) =>
                skipList.atomicWrite(from = fromKey, to = maxKey, toInclusive = false) {
                  entries.foreach(_.applyPoint(skipList))
                }(Bag.glass)
            }

          case MapEntry.Remove(key) =>
            skipList.atomicWrite(from = fromKey, to = key, toInclusive = true) {
              entries.foreach(_.applyPoint(skipList))
            }(Bag.glass)
        }
      }
    } else {
      entry applyBatch skipList
    }

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Segment]): Unit =
    entry.entries.foreach(_.applyPoint(skipList))

  def getRangeKeys(segment: Segment) =
    segment.maxKey match {
      case MaxKey.Fixed(maxKey) =>
        (segment.minKey, maxKey, true)

      case MaxKey.Range(_, maxKey) =>
        (segment.minKey, maxKey, false)
    }

  def values(): Iterable[Segment] =
    skipList.values()

  def floor(key: Slice[Byte]): SegmentOption =
    if (atomicSkipList)
      skipList.atomicRead(getRangeKeys)(_.floor(key))(Bag.glass)
    else
      skipList.floor(key)

  def lower(key: Slice[Byte]): SegmentOption =
    if (atomicSkipList)
      skipList.atomicRead(getRangeKeys)(_.lower(key))(Bag.glass)
    else
      skipList.lower(key)

  def higher(key: Slice[Byte]): SegmentOption =
    if (atomicSkipList)
      skipList.atomicRead(getRangeKeys)(_.higher(key))(Bag.glass)
    else
      skipList.higher(key)

  def headKey(): SliceOption[Byte] =
    if (atomicSkipList)
      head().flatMapSomeS(Slice.Null: SliceOption[Byte])(_.minKey)
    else
      skipList.headKey

  def head(): Glass[SegmentOption] =
    if (atomicSkipList)
      skipList.atomicRead(getRangeKeys)(_.head())(Bag.glass)
    else
      skipList.head()

  def maxKey(): SliceOption[Byte] =
    last().flatMapSomeS(Slice.Null: SliceOption[Byte])(_.maxKey.maxKey)

  def last(): SegmentOption =
    if (atomicSkipList)
      skipList.atomicRead(getRangeKeys)(_.last())(Bag.glass)
    else
      skipList.last()

  def get(key: Slice[Byte]): SegmentOption =
    if (atomicSkipList)
      skipList.atomicRead(getRangeKeys)(_.get(key))(Bag.glass)
    else
      skipList.get(key)

  def contains(key: Slice[Byte]) =
    skipList.contains(key)

  def foreach[R](f: (Slice[Byte], Segment) => R): Unit =
    skipList.foreach(f)

  def foldLeft[R](r: R)(f: (R, (Slice[Byte], Segment)) => R): R =
    skipList.foldLeft(r)(f)

  def take(count: Int)(implicit classTag: ClassTag[Segment]): Slice[Segment] =
    skipList.take(count)

  def size =
    skipList.size

  override def iterator: Iterator[(Slice[Byte], Segment)] =
    skipList.iterator

  override def isEmpty: Boolean =
    skipList.isEmpty

  override def maxKeyValueCount: Int =
    skipList.size
}
