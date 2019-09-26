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

package swaydb.core.segment.merge

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, Persistent, Value, _}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.{FixedMerger, ValueMerger}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

private[core] object SegmentMerger extends LazyLogging {

  def transferSmall(buffers: ListBuffer[SegmentBuffer],
                    minSegmentSize: Long,
                    forMemory: Boolean,
                    createdInLevel: Int,
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config): ListBuffer[SegmentBuffer] =
  //if there are any small Segments, merge them into previous Segment.
    if (buffers.length >= 2 && ((forMemory && buffers.last.lastOption.map(_.stats.memorySegmentSize).getOrElse(0) < minSegmentSize) || buffers.last.lastOption.map(_.stats.segmentSize).getOrElse(0) < minSegmentSize)) {
      val newBuffers = buffers dropRight 1
      val newBuffersLast = newBuffers.last
      val newBuffersLastKeyValue = newBuffersLast.last
      newBuffersLast match {
        case flattened: SegmentBuffer.Flattened =>
          buffers.last foreach {
            transient =>
              flattened add
                transient.updatePrevious(
                  valuesConfig = newBuffersLastKeyValue.valuesConfig,
                  sortedIndexConfig = newBuffersLastKeyValue.sortedIndexConfig,
                  binarySearchIndexConfig = newBuffersLastKeyValue.binarySearchIndexConfig,
                  hashIndexConfig = newBuffersLastKeyValue.hashIndexConfig,
                  bloomFilterConfig = newBuffersLastKeyValue.bloomFilterConfig,
                  previous = flattened.lastOption
                )
          }
      }
      newBuffers.filter(_.nonEmpty)
    } else {
      buffers.filter(_.nonEmpty)
    }

  /**
   * If the last Segment is too small merge the last Segment with the previous Segment's key-value.
   */
  def close(buffers: ListBuffer[SegmentBuffer],
            minSegmentSize: Long,
            forMemory: Boolean,
            createdInLevel: Int,
            valuesConfig: ValuesBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            bloomFilterConfig: BloomFilterBlock.Config): ListBuffer[SegmentBuffer] =
    transferSmall(
      buffers = buffers,
      minSegmentSize = minSegmentSize,
      forMemory = forMemory,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig
    )

  def split(keyValues: Iterable[KeyValue.ReadOnly],
            minSegmentSize: Long,
            isLastLevel: Boolean,
            forInMemory: Boolean,
            createdInLevel: Int,
            valuesConfig: ValuesBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            bloomFilterConfig: BloomFilterBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Iterable[Transient]] = {
    val splits = ListBuffer(SegmentBuffer())

    keyValues foreach {
      keyValue =>
        SegmentGrouper.addKeyValue(
          keyValueToAdd = keyValue,
          splits = splits,
          minSegmentSize = minSegmentSize,
          forInMemory = forInMemory,
          isLastLevel = isLastLevel,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig
        )
    }

    close(
      buffers = splits,
      minSegmentSize = minSegmentSize,
      forMemory = forInMemory,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig
    )
  }

  def merge(newKeyValues: Slice[Memory],
            oldKeyValues: Slice[Memory])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                         timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): ListBuffer[Transient] =
    merge(
      newKeyValues = newKeyValues,
      oldKeyValues = oldKeyValues,
      minSegmentSize = Int.MaxValue,
      isLastLevel = false,
      forInMemory = true,
      createdInLevel = 0,
      valuesConfig = ValuesBlock.Config.disabled,
      sortedIndexConfig = SortedIndexBlock.Config.disabled,
      binarySearchIndexConfig = BinarySearchIndexBlock.Config.disabled,
      hashIndexConfig = HashIndexBlock.Config.disabled,
      bloomFilterConfig = BloomFilterBlock.Config.disabled
    )(keyOrder, timeOrder, functionStore)
      .flatten
      .asInstanceOf[ListBuffer[Transient]]

  def merge(newKeyValue: Memory,
            oldKeyValue: Memory)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 functionStore: FunctionStore): ListBuffer[Transient] =
    merge(
      newKeyValues = Slice(newKeyValue),
      oldKeyValues = Slice(oldKeyValue),
      minSegmentSize = Int.MaxValue,
      isLastLevel = false,
      forInMemory = true,
      createdInLevel = 0,
      valuesConfig = ValuesBlock.Config.disabled,
      sortedIndexConfig = SortedIndexBlock.Config.disabled,
      binarySearchIndexConfig = BinarySearchIndexBlock.Config.disabled,
      hashIndexConfig = HashIndexBlock.Config.disabled,
      bloomFilterConfig = BloomFilterBlock.Config.disabled
    )(keyOrder, timeOrder, functionStore)
      .flatten
      .asInstanceOf[ListBuffer[Transient]]

  def merge(newKeyValues: Slice[KeyValue.ReadOnly],
            oldKeyValues: Slice[KeyValue.ReadOnly],
            minSegmentSize: Long,
            isLastLevel: Boolean,
            forInMemory: Boolean,
            createdInLevel: Int,
            valuesConfig: ValuesBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            bloomFilterConfig: BloomFilterBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore): Iterable[Iterable[Transient]] = {
    val segments =
      merge(
        newKeyValues = MergeList(newKeyValues),
        oldKeyValues = MergeList(oldKeyValues),
        splits = ListBuffer(SegmentBuffer()),
        minSegmentSize = minSegmentSize,
        isLastLevel = isLastLevel,
        forInMemory = forInMemory,
        valuesConfig = valuesConfig,
        createdInLevel = createdInLevel,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )

    close(
      buffers = segments,
      minSegmentSize = minSegmentSize,
      forMemory = forInMemory,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig
    )
  }

  private def merge(newKeyValues: MergeList[Memory.Range, KeyValue.ReadOnly],
                    oldKeyValues: MergeList[Memory.Range, KeyValue.ReadOnly],
                    splits: ListBuffer[SegmentBuffer],
                    minSegmentSize: Long,
                    isLastLevel: Boolean,
                    forInMemory: Boolean,
                    createdInLevel: Int,
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                functionStore: FunctionStore): ListBuffer[SegmentBuffer] = {

    import keyOrder._

    def add(nextKeyValue: KeyValue.ReadOnly): Unit =
      SegmentGrouper.addKeyValue(
        keyValueToAdd = nextKeyValue,
        splits = splits,
        minSegmentSize = minSegmentSize,
        forInMemory = forInMemory,
        isLastLevel = isLastLevel,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )

    @tailrec
    def doMerge(newKeyValues: MergeList[Memory.Range, KeyValue.ReadOnly],
                oldKeyValues: MergeList[Memory.Range, KeyValue.ReadOnly]): ListBuffer[SegmentBuffer] =
      (newKeyValues.headOption, oldKeyValues.headOption) match {

        case (Some(newKeyValue: KeyValue.ReadOnly.Fixed), Some(oldKeyValue: KeyValue.ReadOnly.Fixed)) =>
          if (oldKeyValue.key < newKeyValue.key) {
            add(oldKeyValue)
            doMerge(newKeyValues, oldKeyValues.dropHead())
          } else if (newKeyValue.key < oldKeyValue.key) {
            add(newKeyValue)
            doMerge(newKeyValues.dropHead(), oldKeyValues)
          } else {
            val mergedKeyValue =
              FixedMerger(
                newKeyValue = newKeyValue,
                oldKeyValue = oldKeyValue
              )
            add(mergedKeyValue)
            doMerge(newKeyValues.dropHead(), oldKeyValues.dropHead())
          }

        /**
         * When the input is an overwrite key-value and the existing is a range key-value.
         */
        case (Some(newKeyValue: KeyValue.ReadOnly.Fixed), Some(oldRangeKeyValue: ReadOnly.Range)) =>
          if (newKeyValue.key < oldRangeKeyValue.fromKey) {
            add(newKeyValue)
            doMerge(newKeyValues.dropHead(), oldKeyValues)
          } else if (newKeyValue.key >= oldRangeKeyValue.toKey) {
            add(oldRangeKeyValue)
            doMerge(newKeyValues, oldKeyValues.dropHead())
          } else { //is in-range key
            val (oldFromValue, oldRangeValue) = oldRangeKeyValue.fetchFromAndRangeValueUnsafe
            if (newKeyValue.key equiv oldRangeKeyValue.fromKey) {
              val newFromValue =
                FixedMerger(
                  newKeyValue = newKeyValue,
                  oldKeyValue = oldFromValue.getOrElse(oldRangeValue).toMemory(newKeyValue.key)
                ).toFromValue()

              val toPrepend =
                Memory.Range(
                  fromKey = oldRangeKeyValue.fromKey,
                  toKey = oldRangeKeyValue.toKey,
                  fromValue = Some(newFromValue),
                  rangeValue = oldRangeValue
                )

              doMerge(newKeyValues.dropHead(), oldKeyValues.dropPrepend(toPrepend))
            } else { //else it's a mid range value - split required.
              val newFromValue =
                FixedMerger(
                  newKeyValue = newKeyValue,
                  oldKeyValue = oldRangeValue.toMemory(newKeyValue.key)
                ).toFromValue()

              val lowerSplit = Memory.Range(oldRangeKeyValue.fromKey, newKeyValue.key, oldFromValue, oldRangeValue)
              val upperSplit = Memory.Range(newKeyValue.key, oldRangeKeyValue.toKey, Some(newFromValue), oldRangeValue)
              add(lowerSplit)
              doMerge(newKeyValues.dropHead(), oldKeyValues.dropPrepend(upperSplit))
            }
          }

        /**
         * When the input is a range and the existing is a fixed key-value.
         */
        case (Some(newRangeKeyValue: ReadOnly.Range), Some(oldKeyValue: KeyValue.ReadOnly.Fixed)) =>
          if (oldKeyValue.key >= newRangeKeyValue.toKey) {
            add(newRangeKeyValue)
            doMerge(newKeyValues.dropHead(), oldKeyValues)
          } else if (oldKeyValue.key < newRangeKeyValue.fromKey) {
            add(oldKeyValue)
            doMerge(newKeyValues, oldKeyValues.dropHead())
          } else { //is in-range key
            val (newRangeFromValue, newRangeRangeValue) = newRangeKeyValue.fetchFromAndRangeValueUnsafe
            if (newRangeKeyValue.fromKey equiv oldKeyValue.key) {
              val fromOrRange = newRangeFromValue.getOrElse(newRangeRangeValue)
              fromOrRange match {
                //the range is remove or put simply drop old key-value. No need to merge! Important! do a time check.
                case Value.Remove(None, _) | _: Value.Put if fromOrRange.time > oldKeyValue.time =>
                  doMerge(newKeyValues, oldKeyValues.dropHead())

                case _ =>
                  //if not then do a merge.
                  val newFromValue: Value.FromValue =
                    FixedMerger(
                      newKeyValue = newRangeFromValue.getOrElse(newRangeRangeValue).toMemory(oldKeyValue.key),
                      oldKeyValue = oldKeyValue
                    ).toFromValue()

                  val newKeyValue =
                    Memory.Range(
                      fromKey = newRangeKeyValue.fromKey,
                      toKey = newRangeKeyValue.toKey,
                      fromValue = Some(newFromValue),
                      rangeValue = newRangeRangeValue
                    )

                  doMerge(newKeyValues.dropPrepend(newKeyValue), oldKeyValues.dropHead())
              }
            } else {
              newRangeRangeValue match {
                //the range is remove or put simply remove all old key-values. No need to merge! Important! do a time check.
                case Value.Remove(None, rangeTime) if rangeTime > oldKeyValue.time =>
                  doMerge(newKeyValues, oldKeyValues.dropHead())

                case _ =>
                  val newFromValue =
                    FixedMerger(
                      newKeyValue = newRangeRangeValue.toMemory(oldKeyValue.key),
                      oldKeyValue = oldKeyValue
                    ).toFromValue()

                  val lowerSplit = Memory.Range(newRangeKeyValue.fromKey, oldKeyValue.key, newRangeFromValue, newRangeRangeValue)
                  val upperSplit = Memory.Range(oldKeyValue.key, newRangeKeyValue.toKey, Some(newFromValue), newRangeRangeValue)
                  add(lowerSplit)
                  doMerge(newKeyValues.dropPrepend(upperSplit), oldKeyValues.dropHead())
              }
            }
          }

        /**
         * When both the key-values are ranges.
         */
        case (Some(newRangeKeyValue: ReadOnly.Range), Some(oldRangeKeyValue: ReadOnly.Range)) =>
          if (newRangeKeyValue.toKey <= oldRangeKeyValue.fromKey) {
            add(newRangeKeyValue)
            doMerge(newKeyValues.dropHead(), oldKeyValues)
          } else if (oldRangeKeyValue.toKey <= newRangeKeyValue.fromKey) {
            add(oldRangeKeyValue)
            doMerge(newKeyValues, oldKeyValues.dropHead())
          } else {
            val (newRangeFromValue, newRangeRangeValue) = newRangeKeyValue.fetchFromAndRangeValueUnsafe
            val (oldRangeFromValue, oldRangeRangeValue) = oldRangeKeyValue.fetchFromAndRangeValueUnsafe
            val newRangeFromKey = newRangeKeyValue.fromKey
            val newRangeToKey = newRangeKeyValue.toKey
            val oldRangeFromKey = oldRangeKeyValue.fromKey
            val oldRangeToKey = oldRangeKeyValue.toKey

            if (newRangeFromKey < oldRangeFromKey) {
              //1   -     15
              //      10   -  20
              if (newRangeToKey < oldRangeToKey) {
                val upperSplit = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                val middleSplit =
                  Memory.Range(
                    fromKey = oldRangeFromKey,
                    toKey = newRangeToKey,
                    fromValue = oldRangeFromValue.map(ValueMerger(oldRangeFromKey, newRangeRangeValue, _)),
                    rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                  )
                val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)

                add(upperSplit)
                add(middleSplit)
                doMerge(newKeyValues.dropHead(), oldKeyValues.dropPrepend(lowerSplit))

              } else if (newRangeToKey equiv oldRangeToKey) {
                //1      -      20
                //      10   -  20
                val upperSplit = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)

                val lowerSplit =
                  Memory.Range(
                    fromKey = oldRangeFromKey,
                    toKey = oldRangeToKey,
                    fromValue = oldRangeFromValue.map(ValueMerger(oldRangeFromKey, newRangeRangeValue, _)),
                    rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                  )

                add(upperSplit)
                add(lowerSplit)
                doMerge(newKeyValues.dropHead(), oldKeyValues.dropHead())

              } else {
                //1      -         21
                //      10   -  20
                val upperSplit = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                val middleSplit =
                  Memory.Range(
                    fromKey = oldRangeFromKey,
                    toKey = oldRangeToKey,
                    fromValue = oldRangeFromValue.map(ValueMerger(oldRangeFromKey, newRangeRangeValue, _)),
                    rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                  )

                val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)

                add(upperSplit)
                add(middleSplit)
                doMerge(newKeyValues.dropPrepend(lowerSplit), oldKeyValues.dropHead())
              }
            } else if (newRangeFromKey equiv oldRangeFromKey) {
              //      10 - 15
              //      10   -  20
              if (newRangeToKey < oldRangeToKey) {
                val upperSplit = Memory.Range(
                  fromKey = newRangeFromKey,
                  toKey = newRangeToKey,
                  fromValue =
                    oldRangeFromValue.map(ValueMerger(newRangeFromKey, newRangeFromValue.getOrElse(newRangeRangeValue), _)) orElse {
                      newRangeFromValue.map(ValueMerger(newRangeFromKey, _, oldRangeRangeValue))
                    },
                  rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                )
                val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)

                add(upperSplit)
                doMerge(newKeyValues.dropHead(), oldKeyValues.dropPrepend(lowerSplit))

              } else if (newRangeToKey equiv oldRangeToKey) {
                //      10   -  20
                //      10   -  20
                val update = Memory.Range(
                  fromKey = newRangeFromKey,
                  toKey = newRangeToKey,
                  fromValue =
                    oldRangeFromValue.map(ValueMerger(newRangeFromKey, newRangeFromValue.getOrElse(newRangeRangeValue), _)) orElse {
                      newRangeFromValue.map(ValueMerger(newRangeFromKey, _, oldRangeRangeValue))
                    },
                  rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                )

                add(update)
                doMerge(newKeyValues.dropHead(), oldKeyValues.dropHead())

              } else {
                //      10   -     21
                //      10   -  20
                val upperSplit = Memory.Range(
                  fromKey = newRangeFromKey,
                  toKey = oldRangeToKey,
                  fromValue =
                    oldRangeFromValue.map(ValueMerger(newRangeFromKey, newRangeFromValue.getOrElse(newRangeRangeValue), _)) orElse {
                      newRangeFromValue.map(ValueMerger(newRangeFromKey, _, oldRangeRangeValue))
                    },
                  rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                )
                val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)

                add(upperSplit)
                doMerge(newKeyValues.dropPrepend(lowerSplit), oldKeyValues.dropHead())
              }
            } else {
              //        11 - 15
              //      10   -   20
              if (newRangeToKey < oldRangeToKey) {
                val upperSplit = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)

                val middleSplit =
                  Memory.Range(
                    fromKey = newRangeFromKey,
                    toKey = newRangeToKey,
                    fromValue = newRangeFromValue.map(ValueMerger(newRangeFromKey, _, oldRangeRangeValue)),
                    rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                  )

                val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)

                add(upperSplit)
                add(middleSplit)
                doMerge(newKeyValues.dropHead(), oldKeyValues.dropPrepend(lowerSplit))

              } else if (newRangeToKey equiv oldRangeToKey) {
                //        11 -   20
                //      10   -   20
                val upperSplit = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)

                val lowerSplit = Memory.Range(
                  fromKey = newRangeFromKey,
                  toKey = newRangeToKey,
                  fromValue = newRangeFromValue.map(ValueMerger(newRangeFromKey, _, oldRangeRangeValue)),
                  rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                )

                add(upperSplit)
                add(lowerSplit)
                doMerge(newKeyValues.dropHead(), oldKeyValues.dropHead())

              } else {
                //        11 -     21
                //      10   -   20
                val upperSplit = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)

                val middleSplit =
                  Memory.Range(
                    fromKey = newRangeFromKey,
                    toKey = oldRangeToKey,
                    fromValue = newRangeFromValue.map(ValueMerger(newRangeFromKey, _, oldRangeRangeValue)),
                    rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                  )

                val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)

                add(upperSplit)
                add(middleSplit)
                doMerge(newKeyValues.dropPrepend(lowerSplit), oldKeyValues.dropHead())
              }
            }
          }

        //there are no more oldKeyValues. Add all remaining newKeyValues
        case (Some(_), None) =>
          newKeyValues foreach add
          splits

        //there are no more newKeyValues. Add all remaining oldKeyValues
        case (None, Some(_)) =>
          oldKeyValues foreach add
          splits

        case (None, None) =>
          splits
      }

    doMerge(newKeyValues, oldKeyValues)
  }
}
