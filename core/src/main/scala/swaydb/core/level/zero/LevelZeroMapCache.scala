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
import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.merge.FixedMerger
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent, SkipListSeries}
import swaydb.data.OptimiseWrites
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

private[core] object LevelZeroMapCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]],
                       timeOrder: TimeOrder[Slice[Byte]],
                       functionStore: FunctionStore,
                       optimiseWrites: OptimiseWrites): MapCacheBuilder[LevelZeroMapCache] =
    () => {
      val skipList = newLevelZeroSkipList()

      val skipLists = LeveledSkipLists(skipList = skipList, hasRange = false, keyValueCount = 0)

      new LevelZeroMapCache(skipLists)
    }

  private def newLevelZeroSkipList()(implicit keyOrder: KeyOrder[Slice[Byte]],
                                     optimiseWrites: OptimiseWrites): LevelSkipList =
    optimiseWrites match {
      case OptimiseWrites.RandomOrder =>
        val skipList =
          SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
            nullKey = Slice.Null,
            nullValue = Memory.Null
          )

        LevelSkipList(skipList = skipList, hasRange = false)

      case OptimiseWrites.SequentialOrder(enableHashIndex, initialLength) =>
        val skipList =
          SkipListSeries[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
            lengthPerSeries = initialLength,
            enableHashIndex = enableHashIndex,
            nullKey = Slice.Null,
            nullValue = Memory.Null
          )

        LevelSkipList(skipList = skipList, hasRange = false)
    }

  /**
   * Inserts a [[Memory.Fixed]] key-value into skipList.
   */
  def insert(insert: Memory.Fixed,
             level: LevelSkipList,
             atomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                              timeOrder: TimeOrder[Slice[Byte]],
                              functionStore: FunctionStore): Iterable[Memory] =
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
                  if (merged.isRange) level.setHasRange(true)
                  level.skipList.put(merged.key, merged)
              }
              Memory.emtpySeq
            } else {
              mergedKeyValues
            }

          case _ =>
            level.skipList.put(insert.key, insert)
            Memory.emtpySeq
        }

      //if there is no floor, simply put.
      case Memory.Null =>
        level.skipList.put(insert.key, insert)
        Memory.emtpySeq
    }

  /**
   * Inserts the input [[Memory.Range]] key-value into skipList and always maintaining the previous state of
   * the skipList before applying the new state so that all read queries read the latest write.
   */
  def insert(insert: Memory.Range,
             level: LevelSkipList,
             atomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                              timeOrder: TimeOrder[Slice[Byte]],
                              functionStore: FunctionStore): Iterable[Memory] = {
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
        level.setHasRange(true) //set this before put so reads know to floor this skipList.

        oldKeyValues foreach {
          oldKeyValue =>
            level.skipList.remove(oldKeyValue.key)
        }

        builder.keyValues foreach {
          keyValue =>
            level.skipList.put(keyValue.key, keyValue)
        }

        Memory.emtpySeq
      } else {
        builder.keyValues
      }
    }
  }

  private def doWrite(memory: Memory,
                      level: LevelSkipList,
                      writeNonAtomic: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore,
                                               optimiseWrites: OptimiseWrites): Iterable[Memory] =
    memory match {
      //if insert value is fixed, check the floor entry
      case insertValue: Memory.Fixed =>
        LevelZeroMapCache.insert(insert = insertValue, level = level, atomic = writeNonAtomic)

      //slice the skip list to keep on the range's key-values.
      //if the insert is a Range stash the edge non-overlapping key-values and keep only the ranges in the skipList
      //that fall within the inserted range before submitting fixed values to the range for further splits.
      case insertRange: Memory.Range =>
        LevelZeroMapCache.insert(insert = insertRange, level = level, atomic = writeNonAtomic)
    }

  @tailrec
  private def doWrite(head: MapEntry[Slice[Byte], Memory],
                      tail: List[MapEntry[Slice[Byte], Memory]],
                      skipList: LevelSkipList,
                      atomic: Boolean,
                      startedNewTransaction: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      optimiseWrites: OptimiseWrites): Option[LevelSkipList] =
    if (head.entriesCount <= 1 || head.hasRange || head.hasUpdate || head.hasRemoveDeadline || skipList.hasRange)
      head match {
        case head @ MapEntry.Remove(_) =>
          //this does not occur in reality and should be type-safe instead of having this Exception.
          throw new IllegalAccessException(s"${head.productPrefix} is not allowed in ${LevelZero.productPrefix} .")

        case head @ MapEntry.Put(_, entry: Memory) =>
          val insertResult = doWrite(entry, skipList, atomic)

          if (insertResult.nonEmpty) {
            assert(!startedNewTransaction, "Cannot create multiple transactional skipLists")
            doWrite(head = head, tail = tail, skipList = newLevelZeroSkipList(), atomic = false, startedNewTransaction = true)
          } else {
            tail match {
              case head :: tail =>
                doWrite(head = head, tail = tail, skipList = skipList, atomic = atomic, startedNewTransaction = startedNewTransaction)

              case Nil =>
                if (startedNewTransaction)
                  Some(skipList)
                else
                  None
            }
          }

        case _ =>
          assert(head.entries.size == 1, s"Entries == ${head.entries.size}")
          doWrite(head = head.entries.head, tail = tail, skipList = skipList, atomic = atomic, startedNewTransaction = startedNewTransaction)
      }
    else
      head.entries match {
        case head :: tail =>
          assert(!startedNewTransaction, "Cannot create multiple transactional skipLists")
          doWrite(head = head, tail = tail, skipList = newLevelZeroSkipList(), atomic = false, startedNewTransaction = true)

        case Nil =>
          if (startedNewTransaction)
            Some(skipList)
          else
            None
      }

  @inline private def runMerge(skipLists: LeveledSkipLists)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                            timeOrder: TimeOrder[Slice[Byte]],
                                                            functionStore: FunctionStore): Slice[Memory] =

    skipLists.asScala.foldLeft(Slice.empty[Memory]) {
      (newerKeyValues, oldKeyValues) =>
        val oldKeyValuesSlice = oldKeyValues.toSliceRemoveNullFromSeries

        if (newerKeyValues.isEmpty) {
          oldKeyValuesSlice
        } else {
          val maxSize = (newerKeyValues.size + oldKeyValuesSlice.size) * 3
          val builder = MergeStats.memory(Slice.newBuilder(maxSize))(MergeStats.memoryToMemory)

          SegmentMerger.merge(
            newKeyValues = newerKeyValues,
            oldKeyValues = oldKeyValuesSlice,
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
private[core] class LevelZeroMapCache private(val leveledSkipList: LeveledSkipLists)(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                                                     functionStore: FunctionStore,
                                                                                     optimiseWrites: OptimiseWrites) extends MapCache[Slice[Byte], Memory] {

  override def writeAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    entry.entries match {
      case head :: tail =>
        LevelZeroMapCache.doWrite(
          head = head,
          tail = tail,
          skipList = leveledSkipList.current,
          atomic = true,
          startedNewTransaction = false
        ) foreach {
          newSkipList =>
            leveledSkipList.addFirst(newSkipList)
        }

      case Nil =>
        ()
    }

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    entry.entries match {
      case head :: tail =>
        val result =
          LevelZeroMapCache.doWrite(
            head = head,
            tail = tail,
            skipList = leveledSkipList.current,
            atomic = false,
            startedNewTransaction = false
          )

        if (result.nonEmpty)
          throw new IllegalStateException("InvalidState! atomic write occurred for non-atomic request.")

      case Nil =>
        ()
    }

  override def isEmpty: Boolean =
    leveledSkipList.isEmpty

  override def maxKeyValueCount: Int =
    leveledSkipList.size

  override def asScala: Iterable[(Slice[Byte], Memory)] =
    leveledSkipList
      .asScala
      .flatMap(_.skipList.asScala)

  val mergedKeyValuesCache =
    Cache.noIO[Unit, Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]]](synchronised = true, stored = true, initial = None) {
      (_, _) =>
        if (leveledSkipList.queueSize == 1)
          leveledSkipList.current.skipList match {
            case _: SkipListSeries[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =>
              Right(leveledSkipList.current.toSliceRemoveNullFromSeries)

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
