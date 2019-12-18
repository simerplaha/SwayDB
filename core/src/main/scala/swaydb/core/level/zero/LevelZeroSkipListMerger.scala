/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.level.zero

import java.util.function.Consumer

import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.map.{MapEntry, SkipListMerger}
import swaydb.core.merge.FixedMerger
import swaydb.core.segment.merge.{KeyValueMergeBuilder, SegmentMerger}
import swaydb.core.util.SkipList
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

/**
 * When inserting key-values that alter existing Range key-values in the skipList, they should be inserted into the skipList atomically and should only
 * replace existing keys if all the new inserts have overwritten all the key ranges in the conflicting Range key-value.
 *
 * reverse on the merge results ensures that changes happen atomically.
 */
object LevelZeroSkipListMerger extends SkipListMerger[Slice[Byte], Memory] {

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
             skipList: SkipList.Concurrent[Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                                 functionStore: FunctionStore): Unit = {
    import keyOrder._
    skipList.floor(insert.key) match {
      case Some(floorEntry) =>
        floorEntry match {
          //if floor entry for input Fixed entry & if they keys match, do applyValue else simply add the new key-value.
          case floor: Memory.Fixed if floor.key equiv insert.key =>
            skipList.put(insert.key, applyValue(insert, floor))

          //if the floor entry is a range try to do a merge.
          case floorRange: Memory.Range if insert.key < floorRange.toKey =>
            val builder = KeyValueMergeBuilder.buffer()

            SegmentMerger.merge(
              newKeyValue = insert,
              oldKeyValue = floorRange,
              builder = builder,
              isLastLevel = false
            )

            skipList batch {
              builder map {
                merged: Memory =>
                  SkipList.Batch.Put(merged.key, merged)
              }
            }

          case _ =>
            skipList.put(insert.key, insert)
        }

      //if there is no floor, simply put.
      case None =>
        skipList.put(insert.key, insert)
    }
  }

  /**
   * Inserts the input [[Memory.Range]] key-value into skipList and always maintaining the previous state of
   * the skipList before applying the new state so that all read queries read the latest write.
   */
  def insert(insert: Memory.Range,
             skipList: SkipList.Concurrent[Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                                 functionStore: FunctionStore): Unit = {
    import keyOrder._
    //value the start position of this range to fetch the range's start and end key-values for the skipList.
    val startKey =
      skipList.floor(insert.fromKey) map {
        case range: Memory.Range if insert.fromKey < range.toKey =>
          range.fromKey

        case _ =>
          insert.fromKey
      } getOrElse insert.fromKey

    val conflictingKeyValues = skipList.subMap(startKey, true, insert.toKey, false)
    if (conflictingKeyValues.isEmpty) {
      skipList.put(insert.key, insert)
    } else {
      val oldKeyValues = Slice.create[Memory](conflictingKeyValues.size())

      conflictingKeyValues.values() forEach {
        new Consumer[Memory] {
          override def accept(t: Memory): Unit =
            oldKeyValues add t
        }
      }

      val builder = KeyValueMergeBuilder.buffer()

      SegmentMerger.merge(
        newKeyValues = Slice(insert),
        oldKeyValues = oldKeyValues,
        builder = builder,
        isLastLevel = false
      )

      val batches = ListBuffer.empty[SkipList.Batch[Slice[Byte], Memory]]

      oldKeyValues foreach {
        oldKeyValue =>
          batches += SkipList.Batch.Remove(oldKeyValue.key)
      }

      builder map {
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
                      skipList: SkipList.Concurrent[Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                                          functionStore: FunctionStore): Unit =
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
                      skipList: SkipList.Concurrent[Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                                          functionStore: FunctionStore): Unit =
    entry match {
      case MapEntry.Put(key, value: Memory) =>
        insert(key, value, skipList)

      case MapEntry.Remove(_) =>
        entry applyTo skipList

      case _ =>
        entry.entries.foreach(insert(_, skipList))
    }
}
