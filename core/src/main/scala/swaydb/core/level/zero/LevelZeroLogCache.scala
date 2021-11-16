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

package swaydb.core.level.zero

import swaydb.Bag
import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.function.FunctionStore
import swaydb.core.log.{LogCache, LogCacheBuilder, LogEntry}
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.skiplist.{SkipList, SkipListConcurrent, SkipListSeries}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{MaxKey, MaxKeyOption, Slice, SliceOption}

import scala.collection.mutable.ListBuffer

private[core] object LevelZeroLogCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]],
                       timeOrder: TimeOrder[Slice[Byte]],
                       functionStore: FunctionStore,
                       optimiseWrites: OptimiseWrites,
                       atomic: Atomic): LogCacheBuilder[LevelZeroLogCache] =
    () => LevelZeroLogCache()

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
                      atomic: Atomic): LevelZeroLogCache =
    new LevelZeroLogCache(State())

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
  @inline private[zero] def put(entries: ListBuffer[LogEntry.Point[Slice[Byte], Memory]],
                                state: State)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore): Unit =
    entries foreach {
      case remove @ LogEntry.Remove(_) =>
        //this does not occur in reality and should be type-safe instead of having this Exception.
        throw new IllegalAccessException(s"${LogEntry.productPrefix}.${remove.productPrefix} is not allowed in ${LevelZero.productPrefix}.")

      case LogEntry.Put(_, memory: Memory) =>
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
private[core] class LevelZeroLogCache private(@volatile private var state: LevelZeroLogCache.State)(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                                                                    functionStore: FunctionStore,
                                                                                                    atomic: Atomic) extends LogCache[Slice[Byte], Memory] {

  @inline private def write(entry: LogEntry[Slice[Byte], Memory], atomic: Boolean): Unit = {
    val entries = entry.entries

    if (entry.entriesCount > 1 || state.getHasRange() || entry.hasRange || entry.hasUpdate || entry.hasRemoveDeadline)
      if (atomic) {
        val sorted = entries.sortBy(_.key)(keyOrder)

        sorted.last match {
          case LogEntry.Put(_, last: Memory.Fixed) =>
            state.skipList.atomicWrite(from = sorted.head.key, to = last.key, toInclusive = true) {
              LevelZeroLogCache.put(
                entries = entries,
                state = state
              )
            }(Bag.glass)

          case LogEntry.Put(_, last: Memory.Range) =>
            state.skipList.atomicWrite(from = sorted.head.key, to = last.toKey, toInclusive = false) {
              LevelZeroLogCache.put(
                entries = entries,
                state = state
              )
            }(Bag.glass)

          case remove @ LogEntry.Remove(_) =>
            throw new IllegalAccessException(s"${LogEntry.productPrefix}.${remove.productPrefix} is not allowed in ${LevelZero.productPrefix}.")
        }

      } else {
        LevelZeroLogCache.put(
          entries = entries,
          state = state
        )
      }
    else
      entries.head applyPoint state.skipList
  }

  override def writeAtomic(entry: LogEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = atomic.enabled)

  override def writeNonAtomic(entry: LogEntry[Slice[Byte], Memory]): Unit =
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

  def valuesIterator(): Iterator[Memory] =
    state.skipList.valuesIterator

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

  def maxKey(): MaxKeyOption[Slice[Byte]] =
    skipList.last() match {
      case memory: Memory =>
        memory match {
          case fixed: Memory.Fixed =>
            MaxKey.Fixed(fixed.key)

          case Memory.Range(fromKey, toKey, _, _) =>
            MaxKey.Range(fromKey, toKey)
        }

      case Memory.Null =>
        MaxKey.Null
    }

  def skipList =
    state.skipList
}
