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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level.zero

import swaydb.Aggregator
import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZeroMapCache.{LevelEmbedded, merge}
import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.merge.FixedMerger
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.queue.{VolatileQueue, Walker}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent, SkipListSeries}
import swaydb.data.OptimiseWrites
import swaydb.data.cache.Cache
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

private[core] object LevelZeroMapCache {

  class LevelEmbedded private[zero](val skipList: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                                    @BeanProperty @volatile var hasRange: Boolean)


  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]],
                       timeOrder: TimeOrder[Slice[Byte]],
                       functionStore: FunctionStore,
                       optimiseWrites: OptimiseWrites): MapCacheBuilder[LevelZeroMapCache] =
    () => LevelZeroMapCache()


  @inline def apply()(implicit keyOrder: KeyOrder[Slice[Byte]],
                      timeOrder: TimeOrder[Slice[Byte]],
                      functionStore: FunctionStore,
                      optimiseWrites: OptimiseWrites): LevelZeroMapCache = {
    val level = LevelZeroMapCache.newLevel()

    new LevelZeroMapCache(
      _levels = VolatileQueue[LevelEmbedded](level),
      _zero = level
    )
  }

  private[zero] def newLevel()(implicit keyOrder: KeyOrder[Slice[Byte]],
                               optimiseWrites: OptimiseWrites): LevelZeroMapCache.LevelEmbedded =
    new LevelZeroMapCache.LevelEmbedded(skipList = newSkipList(), hasRange = false)

  private[zero] def newSkipList()(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  optimiseWrites: OptimiseWrites): SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =
    optimiseWrites match {
      case OptimiseWrites.RandomOrder(_) =>
        SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
          nullKey = Slice.Null,
          nullValue = Memory.Null
        )

      case OptimiseWrites.SequentialOrder(_, initialSkipListLength) =>
        SkipListSeries[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
          lengthPerSeries = initialSkipListLength,
          nullKey = Slice.Null,
          nullValue = Memory.Null
        )
    }

  @inline def insert(insert: Memory,
                     level: LevelZeroMapCache.LevelEmbedded,
                     atomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                      timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): Boolean =
    insert match {
      //if insert value is fixed, check the floor entry
      case insertValue: Memory.Fixed =>
        LevelZeroMapCache.insert(insert = insertValue, level = level, atomic = atomic)

      //slice the skip list to keep on the range's key-values.
      //if the insert is a Range stash the edge non-overlapping key-values and keep only the ranges in the skipList
      //that fall within the inserted range before submitting fixed values to the range for further splits.
      case insertRange: Memory.Range =>
        LevelZeroMapCache.insert(insert = insertRange, level = level, atomic = atomic)
    }

  /**
   * Inserts a [[Memory.Fixed]] key-value into skipList.
   */
  def insert(insert: Memory.Fixed,
             level: LevelZeroMapCache.LevelEmbedded,
             atomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                              timeOrder: TimeOrder[Slice[Byte]],
                              functionStore: FunctionStore): Boolean =
    level.skipList.floor(insert.key) match {
      case floorEntry: Memory =>
        import keyOrder._

        floorEntry match {
          //if floor entry for input Fixed entry & if they keys match, do applyValue else simply add the new key-value.
          case floor: Memory.Fixed if floor.key equiv insert.key =>
            val mergedKeyValue =
              FixedMerger(
                newKeyValue = insert,
                oldKeyValue = floor
              ).asInstanceOf[Memory.Fixed]

            level.skipList.put(insert.key, mergedKeyValue)
            true

          //if the floor entry is a range try to do a merge.
          case floorRange: Memory.Range if insert.key < floorRange.toKey =>
            if (atomic && insert.key > floorRange.fromKey) {
              //this will result in split range which will never result in atomic write.
              false
            } else {

              val builder = MergeStats.buffer[Memory, ListBuffer](Aggregator.listBuffer)

              SegmentMerger.merge(
                newKeyValue = insert,
                oldKeyValue = floorRange,
                builder = builder,
                isLastLevel = false
              )

              val mergedKeyValues = builder.keyValues

              if (!atomic || mergedKeyValues.size <= 1) {
                mergedKeyValues foreach {
                  merged: Memory =>
                    if (merged.isRange) level.setHasRange(true)
                    level.skipList.put(merged.key, merged)
                }
                true
              } else {
                false
              }
            }

          case _ =>
            level.skipList.put(insert.key, insert)
            true
        }

      //if there is no floor, simply put.
      case Memory.Null =>
        level.skipList.put(insert.key, insert)
        true
    }

  /**
   * Inserts the input [[Memory.Range]] key-value into skipList and always maintaining the previous state of
   * the skipList before applying the new state so that all read queries read the latest write.
   */
  def insert(insert: Memory.Range,
             level: LevelZeroMapCache.LevelEmbedded,
             atomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                              timeOrder: TimeOrder[Slice[Byte]],
                              functionStore: FunctionStore): Boolean = {
    import keyOrder._

    //value the start position of this range to fetch the range's start and end key-values for the skipList.
    val startKey =
      level.skipList.floor(insert.fromKey) mapS {
        case range: Memory.Range if insert.fromKey < range.toKey =>
          range.fromKey

        case _ =>
          insert.fromKey
      } getOrElse insert.fromKey

    val conflictingKeyValues = level.skipList.subMap(startKey, true, insert.toKey, false)
    if (conflictingKeyValues.isEmpty) {
      level.setHasRange(true) //set this before put so reads know to floor this skipList.
      level.skipList.put(insert.key, insert)
      true
    } else {
      val oldKeyValues = Slice.of[Memory](conflictingKeyValues.size)

      conflictingKeyValues foreach {
        case (_, keyValue) =>
          oldKeyValues add keyValue
      }

      val builder = MergeStats.buffer[Memory, ListBuffer](Aggregator.fromBuilder(ListBuffer.newBuilder))

      SegmentMerger.merge(
        newKeyValues = Slice(insert),
        oldKeyValues = oldKeyValues,
        stats = builder,
        isLastLevel = false
      )

      val mergedKeyValues = builder.keyValues

      if (!atomic) {
        level.setHasRange(true) //set this before put so reads know to floor this skipList.

        oldKeyValues foreach {
          oldKeyValue =>
            level.skipList.remove(oldKeyValue.key)
        }

        mergedKeyValues foreach {
          keyValue =>
            level.skipList.put(keyValue.key, keyValue)
        }

        true

      } else if (mergedKeyValues.size == 1 && conflictingKeyValues.forall(_._1 equiv mergedKeyValues.head.key)) {
        //merged key-value can completely overwrite conflicting key-values in a single insert.

        level.setHasRange(true) //set this before put so reads know to floor this skipList.

        mergedKeyValues foreach {
          keyValue =>
            level.skipList.put(keyValue.key, keyValue)
        }

        true
      } else {
        false
      }
    }
  }

  /**
   * @return the new SkipList is this write started a transactional write.
   */
  @tailrec
  private[zero] def put(head: MapEntry.Point[Slice[Byte], Memory],
                        tail: Iterable[MapEntry.Point[Slice[Byte], Memory]],
                        level: LevelZeroMapCache.LevelEmbedded,
                        atomic: Boolean,
                        startedNewTransaction: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore,
                                                        optimiseWrites: OptimiseWrites): Option[LevelZeroMapCache.LevelEmbedded] =
    if (tail.nonEmpty && !startedNewTransaction)
      put(head = head, tail = tail, level = newLevel(), atomic = false, startedNewTransaction = true)
    else
      head match {
        case head @ MapEntry.Remove(_) =>
          //this does not occur in reality and should be type-safe instead of having this Exception.
          throw new IllegalAccessException(s"${head.productPrefix} is not allowed in ${LevelZero.productPrefix} .")

        case head @ MapEntry.Put(_, memory: Memory) =>
          val inserted = LevelZeroMapCache.insert(insert = memory, level = level, atomic = atomic)

          if (!inserted) {
            assert(!startedNewTransaction, "Cannot create multiple transactional skipLists")
            put(head = head, tail = tail, level = newLevel(), atomic = false, startedNewTransaction = true)
          } else if (tail.nonEmpty) {
            put(head = tail.head, tail = tail.tail, level = level, atomic = atomic, startedNewTransaction = startedNewTransaction)
          } else if (startedNewTransaction) {
            Some(level)
          } else {
            None
          }
      }

  @inline def merge(newer: LevelEmbedded,
                    older: LevelEmbedded)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                          timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore,
                                          optimiseWrites: OptimiseWrites): LevelEmbedded =
    if (newer.skipList.isEmpty) {
      older
    } else if (older.skipList.isEmpty) {
      newer
    } else {
      val mergeSkipList =
        merge(
          newerHasRange = newer.hasRange,
          newer = newer.skipList,
          olderHasRange = older.hasRange,
          older = older.skipList
        )

      new LevelEmbedded(
        skipList = mergeSkipList,
        hasRange = newer.hasRange || older.hasRange
      )
    }

  private final type MergeSkipList[_] = SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]
  @inline def merge(newerHasRange: Boolean,
                    newer: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                    olderHasRange: Boolean,
                    older: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                                                           functionStore: FunctionStore,
                                                                                           optimiseWrites: OptimiseWrites): SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =
    if (newer.isEmpty) {
      older
    } else if (older.isEmpty) {
      newer
    } else {
      val optimiseWritesUpdated =
        optimiseWrites match {
          case OptimiseWrites.RandomOrder(_) =>
            optimiseWrites

          case order @ OptimiseWrites.SequentialOrder(_, _) =>
            val newSize =
              (newer.size + older.size) * {
                if (newerHasRange || olderHasRange)
                  3
                else
                  1
              }

            order.copy(initialSkipListLength = newSize)
        }

      val aggregator: Aggregator[Memory, MergeSkipList[Memory]] =
        new Aggregator[Memory, SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]] {
          val skipList = newSkipList()(keyOrder, optimiseWritesUpdated)

          override def add(item: Memory): Unit =
            skipList.put(item.key, item)

          override def result: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =
            skipList
        }

      val stats = MergeStats.memory(aggregator)(MergeStats.memoryToMemory)

      SegmentMerger.merge(
        newKeyValuesCount = newer.size,
        newKeyValues = newer.valuesIterator,
        oldKeyValuesCount = older.size,
        oldKeyValues = older.valuesIterator,
        stats = stats,
        isLastLevel = false
      )

      aggregator.result
    }

  def mergeEndMayBe(maxQueueSize: Int, levels: VolatileQueue[LevelEmbedded])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                                             functionStore: FunctionStore,
                                                                             optimiseWrites: OptimiseWrites): Unit =
    if (levels.size >= (maxQueueSize max 2)) {
      val secondLastAndLast = levels.takeRight2OrNull()

      if (secondLastAndLast != null) {
        val newLast =
          LevelZeroMapCache.merge(
            newer = secondLastAndLast._1,
            older = secondLastAndLast._2
          )

        levels.replaceLastTwo(
          expectedSecondLast = secondLastAndLast._1,
          expectedLast = secondLastAndLast._2,
          replaceWith = newLast
        )
      }
    }
}

/**
 * Ensures atomic and guarantee all or none writes to in-memory SkipList.
 *
 * Creates multi-layered SkipList.
 */
private[core] class LevelZeroMapCache private(@volatile private var _levels: VolatileQueue[LevelEmbedded],
                                              @volatile private var _zero: LevelEmbedded)(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                                                          functionStore: FunctionStore,
                                                                                          optimiseWrites: OptimiseWrites) extends MapCache[Slice[Byte], Memory] {

  @inline def zero = _zero
  @inline def levels = _levels

  @inline private def write(entry: MapEntry[Slice[Byte], Memory], atomic: Boolean): Unit = {
    val entries = entry.entries

    if (entry.entriesCount > 1 || _zero.hasRange || entry.hasUpdate || entry.hasRange || entry.hasRemoveDeadline)
      LevelZeroMapCache.put(
        head = entries.head,
        tail = entries.tail,
        level = _zero,
        atomic = atomic,
        startedNewTransaction = false
      ) foreach {
        newSkipList =>
          levels.addHead(newSkipList)
          LevelZeroMapCache.mergeEndMayBe(
            maxQueueSize = optimiseWrites.transactionQueueMaxSize,
            levels = levels
          )
      }
    else
      entries.head applyPoint _zero.skipList
  }

  override def writeAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = true)

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = false)

  override def isEmpty: Boolean =
    levels.isEmpty || levels.iterator.forall(_.skipList.isEmpty)

  @inline def maxKeyValueCount: Int =
    levels.iterator.foldLeft(0)(_ + _.skipList.size)

  @inline def levelsCount: Int =
    levels.size

  @inline def hasRange =
    levels.iterator.exists(_.hasRange)

  def walker: Walker[LevelEmbedded] =
    levels

  val flatten =
    Cache.noIO[Unit, SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]](synchronised = true, stored = true, initial = None) {
      (_, _) =>

        if (levels.size == 1) {
          zero.skipList
        } else {
          val flattened =
            levels
              .iterator
              .reduceLeft[LevelEmbedded](LevelZeroMapCache.merge)

          this._levels = VolatileQueue(flattened)

          flattened.skipList
        }
    }

  def levelsIterator: Iterator[LevelEmbedded] =
    levels.iterator

  override def iterator: Iterator[(Slice[Byte], Memory)] =
    levels
      .iterator
      .flatMap(_.skipList.iterator)
}
