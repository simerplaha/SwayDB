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

package swaydb.core.level.zero

import swaydb.Bag
import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.function.FunctionStore
import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent, SkipListSeries}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.{Atomic, OptimiseWrites}

import scala.collection.mutable.ListBuffer

private[core] object LevelZeroMapCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]],
                       timeOrder: TimeOrder[Slice[Byte]],
                       functionStore: FunctionStore,
                       optimiseWrites: OptimiseWrites,
                       atomic: Atomic): MapCacheBuilder[LevelZeroMapCache] =
    () => LevelZeroMapCache()

  object State {
    @inline def apply()(implicit keyOrder: KeyOrder[Slice[Byte]],
                        optimiseWrites: OptimiseWrites): State =
      new State(
        skipList = newSkipList(),
        hasNonPut = false,
        hasRange = false
      )
  }

  class State(val skipList: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
              @volatile private var hasNonPut: Boolean, //if it has key-values other than put
              @volatile private var hasRange: Boolean) { //if it has range key-values

    @inline def setHasRangeToTrue() =
      if (!hasRange) {
        hasRange = true
        setHasNonPutToTrue()
      }

    @inline def setHasNonPutToTrue() =
      if (!hasNonPut)
        hasNonPut = true

    @inline def getHasRange(): Boolean =
      hasRange

    @inline def getHasNonPut(): Boolean =
      hasNonPut
  }

  @inline def apply()(implicit keyOrder: KeyOrder[Slice[Byte]],
                      timeOrder: TimeOrder[Slice[Byte]],
                      functionStore: FunctionStore,
                      optimiseWrites: OptimiseWrites,
                      atomic: Atomic): LevelZeroMapCache =
    new LevelZeroMapCache(State())

  private[zero] def newSkipList()(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  optimiseWrites: OptimiseWrites): SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =
    optimiseWrites match {
      case OptimiseWrites.RandomOrder =>
        SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
          nullKey = Slice.Null,
          nullValue = Memory.Null
        )

      case OptimiseWrites.SequentialOrder(initialSkipListLength) =>
        SkipListSeries[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
          lengthPerSeries = initialSkipListLength,
          nullKey = Slice.Null,
          nullValue = Memory.Null
        )
    }

  /**
   * @return the new SkipList is this write started a transactional write.
   */
  @inline private[zero] def put(entries: ListBuffer[MapEntry.Point[Slice[Byte], Memory]],
                                state: State)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore): Unit =
    entries foreach {
      case remove @ MapEntry.Remove(_) =>
        //this does not occur in reality and should be type-safe instead of having this Exception.
        throw new IllegalAccessException(s"${MapEntry.productPrefix}.${remove.productPrefix} is not allowed in ${LevelZero.productPrefix}.")

      case MapEntry.Put(_, memory: Memory) =>
        LevelZeroMerger.mergeInsert(insert = memory, state = state)
    }
}

/**
 * Ensures atomic and guarantee all or none writes to in-memory SkipList.
 *
 * Creates multi-layered SkipList.
 *
 * Currently all atomic operations defaults to using [[Glass]] which requires
 * blocking on conflicting in-memory SkipList updates. The cost of blocking
 * when concurrently writing and reading in-memory SkipList is very cheap.
 * The maximum time blocking time on benchmarking was between 0.006 to 0.019642 seconds.
 */
private[core] class LevelZeroMapCache private(@volatile private var state: LevelZeroMapCache.State)(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                                                                    functionStore: FunctionStore,
                                                                                                    atomic: Atomic) extends MapCache[Slice[Byte], Memory] {

  /**
   * Should only be invoked after the [[Map]] is compacted.
   */
  def delete(): Unit =
    this.state = LevelZeroMapCache.State()(keyOrder = keyOrder, optimiseWrites = OptimiseWrites.RandomOrder)

  @inline private def write(entry: MapEntry[Slice[Byte], Memory], atomic: Boolean): Unit = {
    val entries = entry.entries

    if (entry.entriesCount > 1 || state.getHasRange() || entry.hasRange || entry.hasUpdate || entry.hasRemoveDeadline)
      if (atomic) {
        val sorted = entries.sortBy(_.key)(keyOrder)

        sorted.last match {
          case MapEntry.Put(_, last: Memory.Fixed) =>
            state.skipList.atomicWrite(from = sorted.head.key, to = last.key, toInclusive = true) {
              LevelZeroMapCache.put(
                entries = entries,
                state = state
              )
            }(Bag.glass)

          case MapEntry.Put(_, last: Memory.Range) =>
            state.skipList.atomicWrite(from = sorted.head.key, to = last.toKey, toInclusive = false) {
              LevelZeroMapCache.put(
                entries = entries,
                state = state
              )
            }(Bag.glass)

          case remove @ MapEntry.Remove(_) =>
            throw new IllegalAccessException(s"${MapEntry.productPrefix}.${remove.productPrefix} is not allowed in ${LevelZero.productPrefix}.")
        }

      } else {
        LevelZeroMapCache.put(
          entries = entries,
          state = state
        )
      }
    else
      entries.head applyPoint state.skipList
  }

  override def writeAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = atomic.enabled)

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = false)

  override def isEmpty: Boolean =
    state.skipList.isEmpty

  @inline def maxKeyValueCount: Int =
    state.skipList.size

  @inline def hasRange =
    state.getHasRange()

  @inline def hasNonPut =
    state.getHasNonPut()

  override def iterator: Iterator[(Slice[Byte], Memory)] =
    state.skipList.iterator

  @inline private def getRangeKeys(memory: Memory): (Slice[Byte], Slice[Byte], Boolean) =
    memory match {
      case fixed: Memory.Fixed =>
        (fixed.key, fixed.key, false)

      case Memory.Range(fromKey, toKey, _, _) =>
        (fromKey, toKey, true)
    }

  def headKeyOptimised: SliceOption[Byte] =
    if (atomic.enabled)
      state
        .skipList
        .atomicRead(getRangeKeys)(_.head())(Bag.glass)
        .flatMapSomeS(Slice.Null: SliceOption[Byte])(_.key)
    else
      state.skipList.headKey

  def lastKeyOptimised: SliceOption[Byte] =
    if (atomic.enabled)
      state
        .skipList
        .atomicRead(getRangeKeys)(_.last())(Bag.glass)
        .flatMapSomeS(Slice.Null: SliceOption[Byte])(_.key)
    else
      state.skipList.lastKey

  def headOptimised: MemoryOption =
    if (atomic.enabled)
      state.skipList.atomicRead(getRangeKeys)(_.head())(Bag.glass)
    else
      state.skipList.head()

  def lastOptimised: MemoryOption =
    if (atomic.enabled)
      state.skipList.atomicRead(getRangeKeys)(_.last())(Bag.glass)
    else
      state.skipList.last()

  def getOptimised(key: Slice[Byte]): MemoryOption =
    if (atomic.enabled)
      state.skipList.atomicRead(getRangeKeys)(_.get(key))(Bag.glass)
    else
      state.skipList.get(key)

  def floorOptimised(key: Slice[Byte]): MemoryOption =
    if (atomic.enabled)
      state.skipList.atomicRead(getRangeKeys)(_.floor(key))(Bag.glass)
    else
      state.skipList.floor(key)

  def lowerOptimised(key: Slice[Byte]): MemoryOption =
    if (atomic.enabled)
      state.skipList.atomicRead(getRangeKeys)(_.lower(key))(Bag.glass)
    else
      state.skipList.lower(key)

  def higherOptimised(key: Slice[Byte]): MemoryOption =
    if (atomic.enabled)
      state.skipList.atomicRead(getRangeKeys)(_.higher(key))(Bag.glass)
    else
      state.skipList.higher(key)

  def ceilingOptimised(key: Slice[Byte]): MemoryOption =
    if (atomic.enabled)
      state.skipList.atomicRead(getRangeKeys)(_.ceiling(key))(Bag.glass)
    else
      state.skipList.ceiling(key)

  def skipList =
    state.skipList
}
