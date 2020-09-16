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

import java.util.function.Consumer

import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.function.FunctionStore
import swaydb.core.map.{MapEntry, SkipListMerger}
import swaydb.core.merge.FixedMerger
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}

import scala.collection.mutable.ListBuffer

/**
 * When inserting key-values that alter existing Range key-values in the skipList, they should be inserted into the skipList atomically and should only
 * replace existing keys if all the new inserts have overwritten all the key ranges in the conflicting Range key-value.
 *
 * reverse on the merge results ensures that changes happen atomically.
 */
case class LevelZeroSkipListMerger()(implicit keyOrder: KeyOrder[Slice[Byte]],
                                     timeOrder: TimeOrder[Slice[Byte]],
                                     functionStore: FunctionStore) extends SkipListMerger[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] {

  import keyOrder._

  //.get is no good. Memory key-values will never result in failure since they do not perform IO (no side-effects).
  //But this is a temporary solution until applyValue is updated to accept type classes to perform side effect.
  def applyValue(newKeyValue: Memory.Fixed,
                 oldKeyValue: Memory.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): Memory.Fixed =
    FixedMerger(
      newKeyValue = newKeyValue,
      oldKeyValue = oldKeyValue
    ).asInstanceOf[Memory.Fixed]

  /**
   * Inserts a [[Memory.Fixed]] key-value into skipList.
   */
  def insert(insert: Memory.Fixed,
             skipList: SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Unit =
    skipList.floor(insert.key) match {
      case floorEntry: Memory =>
        floorEntry match {
          //if floor entry for input Fixed entry & if they keys match, do applyValue else simply add the new key-value.
          case floor: Memory.Fixed if floor.key equiv insert.key =>
            skipList.put(insert.key, applyValue(insert, floor))

          //if the floor entry is a range try to do a merge.
          case floorRange: Memory.Range if insert.key < floorRange.toKey =>
            val builder = MergeStats.buffer[Memory, ListBuffer](ListBuffer.newBuilder)

            SegmentMerger.merge(
              newKeyValue = insert,
              oldKeyValue = floorRange,
              builder = builder,
              isLastLevel = false
            )

            skipList batch {
              builder.keyValues map {
                merged: Memory =>
                  SkipList.Batch.Put(merged.key, merged)
              }
            }

          case _ =>
            skipList.put(insert.key, insert)
        }

      //if there is no floor, simply put.
      case Memory.Null =>
        skipList.put(insert.key, insert)
    }

  /**
   * Inserts the input [[Memory.Range]] key-value into skipList and always maintaining the previous state of
   * the skipList before applying the new state so that all read queries read the latest write.
   */
  def insert(insert: Memory.Range,
             skipList: SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Unit = {
    //value the start position of this range to fetch the range's start and end key-values for the skipList.
    val startKey =
      skipList.floor(insert.fromKey) mapS {
        case range: Memory.Range if insert.fromKey < range.toKey =>
          range.fromKey

        case _ =>
          insert.fromKey
      } getOrElse insert.fromKey

    val conflictingKeyValues = skipList.subMap(startKey, true, insert.toKey, false)
    if (conflictingKeyValues.isEmpty) {
      skipList.put(insert.key, insert)
    } else {
      val oldKeyValues = Slice.of[Memory](conflictingKeyValues.size())

      conflictingKeyValues.values() forEach {
        new Consumer[Memory] {
          override def accept(keyValue: Memory): Unit =
            oldKeyValues add keyValue
        }
      }

      val builder = MergeStats.buffer[Memory, ListBuffer](ListBuffer.newBuilder)

      SegmentMerger.merge(
        newKeyValues = Slice(insert),
        oldKeyValues = oldKeyValues,
        stats = builder,
        isLastLevel = false
      )

      val batches = ListBuffer.empty[SkipList.Batch[Slice[Byte], Memory]]

      oldKeyValues foreach {
        oldKeyValue =>
          batches += SkipList.Batch.Remove(oldKeyValue.key)
      }

      builder.keyValues map {
        keyValue =>
          batches += SkipList.Batch.Put(keyValue.key, keyValue)
      }

      skipList batch batches

      //while inserting also clear any conflicting key-values that are not replaced by new inserts.
      //      mergedKeyValues.reverse.foldLeft(Option.empty[Slice[Byte]]) {
      //        case (previousInsertedKey, transient: Memory) =>
      //          skipList.put(transient.key, transient.toMemory)
      //          //remove any entries that are greater than transient.key to the previously inserted entry.
      //          val toKey = previousInsertedKey.getOrElse(conflictingKeyValues.lastKey())
      //          if (transient.key < toKey)
      //            conflictingKeyValues.subMap(transient.key, false, toKey, previousInsertedKey.isEmpty).clear()
      //          Some(transient.key)
      //      }
    }
  }

  override def insert(insertKey: Slice[Byte],
                      insertValue: Memory,
                      skipList: SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Unit =
    insertValue match {
      //if insert value is fixed, check the floor entry
      case insertValue: Memory.Fixed =>
        insert(insertValue, skipList)

      //slice the skip list to keep on the range's key-values.
      //if the insert is a Range stash the edge non-overlapping key-values and keep only the ranges in the skipList
      //that fall within the inserted range before submitting fixed values to the range for further splits.
      case insertRange: Memory.Range =>
        insert(insertRange, skipList)
    }

  override def insert(entry: MapEntry[Slice[Byte], Memory],
                      skipList: SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Unit =
    entry match {
      case MapEntry.Put(key, value: Memory) =>
        insert(key, value, skipList)

      case MapEntry.Remove(_) =>
        entry applyTo skipList

      case _ =>
        entry.entries.foreach(insert(_, skipList))
    }
}
