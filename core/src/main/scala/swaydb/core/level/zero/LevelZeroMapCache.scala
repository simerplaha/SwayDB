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

import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZeroMapCache.newSkipListState
import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.merge.FixedMerger
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent, SkipListSeries}
import swaydb.data.OptimiseWrites
import swaydb.data.cache.Cache
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.mutable.ListBuffer

private[core] object LevelZeroMapCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]],
                       timeOrder: TimeOrder[Slice[Byte]],
                       functionStore: FunctionStore,
                       optimiseWrites: OptimiseWrites): MapCacheBuilder[LevelZeroMapCache] =
    () => {
      val state = newSkipListState()

      val leveledSkipList = LeveledSkipList(skipList = state)

      new LevelZeroMapCache(leveledSkipList)
    }

  private def newSkipListState()(implicit keyOrder: KeyOrder[Slice[Byte]],
                                     optimiseWrites: OptimiseWrites): LeveledSkipList.SkipListState =
    optimiseWrites match {
      case OptimiseWrites.RandomOrder =>
        val skipList =
          SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
            nullKey = Slice.Null,
            nullValue = Memory.Null
          )

        new LeveledSkipList.SkipListState(skipList = skipList, hasRange = false)

      case OptimiseWrites.SequentialOrder(initialLength) =>
        val skipList =
          SkipListSeries[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
            lengthPerSeries = initialLength,
            nullKey = Slice.Null,
            nullValue = Memory.Null
          )

        new LeveledSkipList.SkipListState(skipList = skipList, hasRange = false)
    }

  /**
   * Inserts a [[Memory.Fixed]] key-value into skipList.
   */
  def insert(insert: Memory.Fixed,
             skipList: LeveledSkipList.SkipListState,
             atomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                              timeOrder: TimeOrder[Slice[Byte]],
                              functionStore: FunctionStore): Iterable[Memory] =
    skipList.skipList.floor(insert.key) match {
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

            skipList.skipList.put(insert.key, mergedKeyValue)
            Memory.emtpySeq

          //if the floor entry is a range try to do a merge.
          case floorRange: Memory.Range if insert.key < floorRange.toKey =>
            val builder = MergeStats.buffer[Memory, ListBuffer](ListBuffer.newBuilder)

            SegmentMerger.merge(
              newKeyValue = insert,
              oldKeyValue = floorRange,
              builder = builder,
              isLastLevel = false
            )

            val mergedKeyValues = builder.keyValues

            if (mergedKeyValues.size <= 1 || !atomic) {
              mergedKeyValues foreach {
                merged: Memory =>
                  if (merged.isRange) skipList.setHasRange(true)
                  skipList.skipList.put(merged.key, merged)
              }
              Memory.emtpySeq
            } else {
              mergedKeyValues
            }

          case _ =>
            skipList.skipList.put(insert.key, insert)
            Memory.emtpySeq
        }

      //if there is no floor, simply put.
      case Memory.Null =>
        skipList.skipList.put(insert.key, insert)
        Memory.emtpySeq
    }

  /**
   * Inserts the input [[Memory.Range]] key-value into skipList and always maintaining the previous state of
   * the skipList before applying the new state so that all read queries read the latest write.
   */
  def insert(insert: Memory.Range,
             skipList: LeveledSkipList.SkipListState,
             atomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                              timeOrder: TimeOrder[Slice[Byte]],
                              functionStore: FunctionStore): Iterable[Memory] = {
    import keyOrder._

    //value the start position of this range to fetch the range's start and end key-values for the skipList.
    val startKey =
      skipList.skipList.floor(insert.fromKey) mapS {
        case range: Memory.Range if insert.fromKey < range.toKey =>
          range.fromKey

        case _ =>
          insert.fromKey
      } getOrElse insert.fromKey

    val conflictingKeyValues = skipList.skipList.subMap(startKey, true, insert.toKey, false)
    if (conflictingKeyValues.isEmpty) {
      skipList.setHasRange(true) //set this before put so reads know to floor this skipList.
      skipList.skipList.put(insert.key, insert)
      Memory.emtpySeq
    } else {
      val oldKeyValues = Slice.of[Memory](conflictingKeyValues.size)

      conflictingKeyValues foreach {
        case (_, keyValue) =>
          oldKeyValues add keyValue
      }

      val builder = MergeStats.buffer[Memory, ListBuffer](ListBuffer.newBuilder)

      SegmentMerger.merge(
        newKeyValues = Slice(insert),
        oldKeyValues = oldKeyValues,
        stats = builder,
        isLastLevel = false
      )

      if (builder.keyValues.size <= 1 || !atomic) {
        skipList.setHasRange(true) //set this before put so reads know to floor this skipList.

        oldKeyValues foreach {
          oldKeyValue =>
            skipList.skipList.remove(oldKeyValue.key)
        }

        builder.keyValues foreach {
          keyValue =>
            skipList.skipList.put(keyValue.key, keyValue)
        }

        Memory.emtpySeq
      } else {
        builder.keyValues
      }
    }
  }

  /**
   * @return the new SkipList is this write started a transactional write.
   */
  @tailrec
  private def doWrite(head: MapEntry.Point[Slice[Byte], Memory],
                      tail: Iterable[MapEntry.Point[Slice[Byte], Memory]],
                      skipList: LeveledSkipList.SkipListState,
                      atomic: Boolean,
                      startedNewTransaction: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      optimiseWrites: OptimiseWrites): Option[LeveledSkipList.SkipListState] =
    head match {
      case head @ MapEntry.Remove(_) =>
        //this does not occur in reality and should be type-safe instead of having this Exception.
        throw new IllegalAccessException(s"${head.productPrefix} is not allowed in ${LevelZero.productPrefix} .")

      case head @ MapEntry.Put(_, memory: Memory) =>
        val insertResult =
          memory match {
            //if insert value is fixed, check the floor entry
            case insertValue: Memory.Fixed =>
              LevelZeroMapCache.insert(insert = insertValue, skipList = skipList, atomic = atomic)

            //slice the skip list to keep on the range's key-values.
            //if the insert is a Range stash the edge non-overlapping key-values and keep only the ranges in the skipList
            //that fall within the inserted range before submitting fixed values to the range for further splits.
            case insertRange: Memory.Range =>
              LevelZeroMapCache.insert(insert = insertRange, skipList = skipList, atomic = atomic)
          }

        if (insertResult.nonEmpty) {
          assert(!startedNewTransaction, "Cannot create multiple transactional skipLists")
          doWrite(head = head, tail = tail, skipList = newSkipListState(), atomic = false, startedNewTransaction = true)
        } else if (tail.nonEmpty) {
          doWrite(head = tail.head, tail = tail.tail, skipList = skipList, atomic = atomic, startedNewTransaction = startedNewTransaction)
        } else if (startedNewTransaction) {
          Some(skipList)
        } else {
          None
        }
    }

  @inline private def runMerge(skipLists: LeveledSkipList)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           functionStore: FunctionStore): Slice[Memory] =

    skipLists.iterator.foldLeft(Slice.empty[Memory]) {
      (newerKeyValues, oldKeyValues) =>

        if (newerKeyValues.isEmpty) {
          Slice.from(oldKeyValues.skipList.valuesIterator, oldKeyValues.skipList.size)
        } else {
          val maxSize = (newerKeyValues.size + oldKeyValues.skipList.size) * 3
          val builder = MergeStats.memory(Slice.newBuilder(maxSize))(MergeStats.memoryToMemory)

          SegmentMerger.merge(
            newKeyValues = newerKeyValues,
            oldKeyValuesCount = oldKeyValues.skipList.size,
            oldKeyValues = oldKeyValues.skipList.valuesIterator,
            stats = builder,
            isLastLevel = false
          )

          builder.keyValues
        }
    }
}

/**
 * Ensures atomic and guarantee all or none writes to in-memory SkipList.
 *
 * Creates multi-layered SkipList.
 */
private[core] class LevelZeroMapCache private(val leveledSkipList: LeveledSkipList)(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                                                    functionStore: FunctionStore,
                                                                                    optimiseWrites: OptimiseWrites) extends MapCache[Slice[Byte], Memory] {

  @inline private def write(entry: MapEntry[Slice[Byte], Memory], atomic: Boolean): Unit = {
    val entries = entry.entries

    if (entry.entriesCount > 1 || leveledSkipList.current.hasRange || entry.hasUpdate || entry.hasRange || entry.hasRemoveDeadline)
      LevelZeroMapCache.doWrite(
        head = entries.head,
        tail = entries.tail,
        skipList = leveledSkipList.current,
        atomic = atomic,
        startedNewTransaction = false
      ) foreach {
        newSkipList =>
          leveledSkipList.addFirst(newSkipList)
      }
    else
      entries.head applyPoint leveledSkipList.current.skipList
  }

  override def writeAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = true)

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = false)

  override def isEmpty: Boolean =
    leveledSkipList.isEmpty

  override def maxKeyValueCount: Int =
    leveledSkipList.size

  override def asScala: Iterable[(Slice[Byte], Memory)] =
    leveledSkipList
      .iterator
      .to(Iterable)
      .flatMap(_.skipList.toIterable)

  val mergedKeyValuesCache =
    Cache.noIO[Unit, Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]]](synchronised = true, stored = true, initial = None) {
      (_, _) =>
        if (leveledSkipList.queueSize == 1)
          leveledSkipList.current.skipList match {
            case series: SkipListSeries[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =>
              Right(series.toValuesSlice())

            case skipList: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =>
              Left(skipList)
          }
        else
          Right(LevelZeroMapCache.runMerge(leveledSkipList))
    }

  def mergedKeyValuesIterable: Iterable[Memory] =
    mergedKeyValuesCache.value(()) match {
      case Left(value) =>
        value.values()

      case Right(value) =>
        value
    }

  def mergeKeyValuesCount: Int =
    mergedKeyValuesCache.value(()) match {
      case Left(value) =>
        value.size

      case Right(value) =>
        value.size
    }

  @inline def hasRange =
    leveledSkipList.hasRange

}
