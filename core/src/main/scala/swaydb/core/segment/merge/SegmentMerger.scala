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

package swaydb.core.segment.merge

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.{FixedMerger, ValueMerger}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.util.DropIterator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

private[core] object SegmentMerger extends LazyLogging {

  def merge(newKeyValue: Memory,
            oldKeyValue: Memory,
            builder: MergeStats[Memory, Iterable],
            isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore): Unit =
    merge(
      headGap = Assignable.emptyIterable,
      tailGap = Assignable.emptyIterable,
      newKeyValues = Slice(newKeyValue),
      oldKeyValues = Slice(oldKeyValue),
      stats = builder,
      isLastLevel = isLastLevel
    )

  def merge(newKeyValues: Slice[KeyValue],
            oldKeyValues: Slice[KeyValue],
            stats: MergeStats[Memory, Iterable],
            isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore): Unit =
    merge(
      headGap = Assignable.emptyIterable,
      tailGap = Assignable.emptyIterable,
      newKeyValues = DropIterator[Memory.Range, Assignable](newKeyValues),
      oldKeyValues = DropIterator[Memory.Range, KeyValue](oldKeyValues),
      builder = stats,
      isLastLevel = isLastLevel
    )

  def merge(headGap: Iterable[Assignable],
            tailGap: Iterable[Assignable],
            newKeyValues: Slice[KeyValue],
            oldKeyValues: Slice[KeyValue],
            stats: MergeStats[Memory, Iterable],
            isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore): Unit =
    merge(
      headGap = headGap,
      tailGap = tailGap,
      newKeyValues = DropIterator[Memory.Range, Assignable](newKeyValues),
      oldKeyValues = DropIterator[Memory.Range, KeyValue](oldKeyValues),
      builder = stats,
      isLastLevel = isLastLevel
    )

  def merge[T[_]](headGap: Iterable[Assignable],
                  tailGap: Iterable[Assignable],
                  newKeyValues: Slice[KeyValue],
                  oldKeyValuesCount: Int,
                  oldKeyValues: Iterator[KeyValue],
                  stats: MergeStats[Memory, T],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore): Unit =
    merge(
      headGap = headGap,
      tailGap = tailGap,
      newKeyValues = DropIterator[Memory.Range, Assignable](newKeyValues),
      oldKeyValues = DropIterator[Memory.Range, KeyValue](oldKeyValuesCount, oldKeyValues),
      builder = stats,
      isLastLevel = isLastLevel
    )

  def merge[T[_]](headGap: Iterable[Assignable],
                  tailGap: Iterable[Assignable],
                  mergeableCount: Int,
                  mergeable: Iterator[Assignable],
                  oldKeyValuesCount: Int,
                  oldKeyValues: Iterator[KeyValue],
                  stats: MergeStats[Memory, T],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore): Unit =
    merge(
      headGap = headGap,
      tailGap = tailGap,
      newKeyValues = DropIterator[Memory.Range, Assignable](mergeableCount, mergeable),
      oldKeyValues = DropIterator[Memory.Range, KeyValue](oldKeyValuesCount, oldKeyValues),
      builder = stats,
      isLastLevel = isLastLevel
    )

  def mergeAssignable[T[_]](mergeable: Slice[Assignable],
                            oldKeyValues: Slice[KeyValue],
                            stats: MergeStats[Memory, T],
                            isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                  functionStore: FunctionStore): Unit =
    merge(
      headGap = Assignable.emptyIterable,
      tailGap = Assignable.emptyIterable,
      newKeyValues = DropIterator[Memory.Range, Assignable](mergeable),
      oldKeyValues = DropIterator[Memory.Range, KeyValue](oldKeyValues),
      builder = stats,
      isLastLevel = isLastLevel
    )

  def merge[T[_]](mergeableCount: Int,
                  mergeable: Iterator[Assignable],
                  oldKeyValuesCount: Int,
                  oldKeyValues: Iterator[KeyValue],
                  stats: MergeStats[Memory, T],
                  isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore): Unit =
    merge(
      headGap = Assignable.emptyIterable,
      tailGap = Assignable.emptyIterable,
      newKeyValues = DropIterator[Memory.Range, Assignable](mergeableCount, mergeable),
      oldKeyValues = DropIterator[Memory.Range, KeyValue](oldKeyValuesCount, oldKeyValues),
      builder = stats,
      isLastLevel = isLastLevel
    )

  private def merge[T[_]](headGap: Iterable[Assignable], //head key-values that do not require merging
                          tailGap: Iterable[Assignable], //tail key-values that do not require merging
                          newKeyValues: DropIterator[Memory.Range, Assignable],
                          oldKeyValues: DropIterator[Memory.Range, KeyValue],
                          builder: MergeStats[Memory, T],
                          isLastLevel: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): Unit = {

    import keyOrder._

    @inline def add(nextKeyValue: KeyValue): Unit =
      SegmentGrouper.add(
        keyValue = nextKeyValue,
        builder = builder,
        isLastLevel = isLastLevel
      )

    @inline def addAll(toAdd: Iterator[Assignable]) =
      toAdd foreach {
        case collection: Assignable.Collection =>
          collection.iterator() foreach add

        case value: KeyValue =>
          add(value)
      }

    if (headGap.nonEmpty) {
      assert(headGap.last.key < oldKeyValues.headOrNull.key)
      addAll(headGap.iterator)
    }

    @tailrec
    def doMerge(newKeyValues: DropIterator[Memory.Range, Assignable],
                oldKeyValues: DropIterator[Memory.Range, KeyValue]): Unit =
      newKeyValues.headOrNull match {
        /**
         * FIXED onto OTHERS
         */

        case collection: Assignable.Collection =>
          val expanded = DropIterator[Memory.Range, Assignable](collection.getKeyValueCount(), collection.iterator())
          val newIterator = expanded append newKeyValues.dropHead()

          doMerge(newIterator, oldKeyValues)

        case newKeyValue: KeyValue.Fixed =>
          oldKeyValues.headOrNull match {
            case oldKeyValue: KeyValue.Fixed =>
              val compare = keyOrder.compare(oldKeyValue.key, newKeyValue.key)

              if (compare < 0) {
                add(oldKeyValue)
                doMerge(newKeyValues, oldKeyValues.dropHead())
              } else if (compare > 0) {
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

            case oldRangeKeyValue: KeyValue.Range =>
              val compare = keyOrder.compare(newKeyValue.key, oldRangeKeyValue.fromKey)

              if (compare < 0) {
                add(newKeyValue)
                doMerge(newKeyValues.dropHead(), oldKeyValues)
              } else if (newKeyValue.key >= oldRangeKeyValue.toKey) {
                add(oldRangeKeyValue)
                doMerge(newKeyValues, oldKeyValues.dropHead())
              } else { //is in-range key
                val (oldFromValue, oldRangeValue) = oldRangeKeyValue.fetchFromAndRangeValueUnsafe
                if (compare == 0) {
                  val newFromValue =
                    FixedMerger(
                      newKeyValue = newKeyValue,
                      oldKeyValue = oldFromValue.getOrElseS(oldRangeValue).toMemory(newKeyValue.key)
                    ).toFromValue()

                  val toPrepend =
                    Memory.Range(
                      fromKey = oldRangeKeyValue.fromKey,
                      toKey = oldRangeKeyValue.toKey,
                      fromValue = newFromValue,
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
                  val upperSplit = Memory.Range(newKeyValue.key, oldRangeKeyValue.toKey, newFromValue, oldRangeValue)
                  add(lowerSplit)
                  doMerge(newKeyValues.dropHead(), oldKeyValues.dropPrepend(upperSplit))
                }
              }

            case null =>
              addAll(newKeyValues.iterator)
          }

        /**
         * RANGE onto OTHERS
         */

        case newRangeKeyValue: KeyValue.Range =>
          oldKeyValues.headOrNull match {
            case oldKeyValue: KeyValue.Fixed =>
              if (oldKeyValue.key >= newRangeKeyValue.toKey) {
                add(newRangeKeyValue)
                doMerge(newKeyValues.dropHead(), oldKeyValues)
              } else if (oldKeyValue.key < newRangeKeyValue.fromKey) {
                add(oldKeyValue)
                doMerge(newKeyValues, oldKeyValues.dropHead())
              } else { //is in-range key
                val (newRangeFromValue, newRangeRangeValue) = newRangeKeyValue.fetchFromAndRangeValueUnsafe
                if (newRangeKeyValue.fromKey equiv oldKeyValue.key) {
                  val fromOrRange = newRangeFromValue.getOrElseS(newRangeRangeValue)
                  fromOrRange match {
                    //the range is remove or put simply drop old key-value. No need to merge! Important! do a time check.
                    case Value.Remove(None, _) | _: Value.Put if fromOrRange.time > oldKeyValue.time =>
                      doMerge(newKeyValues, oldKeyValues.dropHead())

                    case _ =>
                      //if not then do a merge.
                      val newFromValue: Value.FromValue =
                        FixedMerger(
                          newKeyValue = newRangeFromValue.getOrElseS(newRangeRangeValue).toMemory(oldKeyValue.key),
                          oldKeyValue = oldKeyValue
                        ).toFromValue()

                      val newKeyValue =
                        Memory.Range(
                          fromKey = newRangeKeyValue.fromKey,
                          toKey = newRangeKeyValue.toKey,
                          fromValue = newFromValue,
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
                      val upperSplit = Memory.Range(oldKeyValue.key, newRangeKeyValue.toKey, newFromValue, newRangeRangeValue)
                      add(lowerSplit)
                      doMerge(newKeyValues.dropPrepend(upperSplit), oldKeyValues.dropHead())
                  }
                }
              }

            case oldRangeKeyValue: KeyValue.Range =>
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
                        fromValue = oldRangeFromValue.flatMapS(ValueMerger(oldRangeFromKey, newRangeRangeValue, _)),
                        rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                      )
                    val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, Value.FromValue.Null, oldRangeRangeValue)

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
                        fromValue = oldRangeFromValue.flatMapS(ValueMerger(oldRangeFromKey, newRangeRangeValue, _)),
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
                        fromValue = oldRangeFromValue.flatMapS(ValueMerger(oldRangeFromKey, newRangeRangeValue, _)),
                        rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                      )

                    val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, Value.FromValue.Null, newRangeRangeValue)

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
                        oldRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, newRangeFromValue.getOrElseS(newRangeRangeValue), _)) orElseS {
                          newRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, _, oldRangeRangeValue))
                        },
                      rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                    )
                    val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, Value.FromValue.Null, oldRangeRangeValue)

                    add(upperSplit)
                    doMerge(newKeyValues.dropHead(), oldKeyValues.dropPrepend(lowerSplit))

                  } else if (newRangeToKey equiv oldRangeToKey) {
                    //      10   -  20
                    //      10   -  20
                    val update = Memory.Range(
                      fromKey = newRangeFromKey,
                      toKey = newRangeToKey,
                      fromValue =
                        oldRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, newRangeFromValue.getOrElseS(newRangeRangeValue), _)) orElseS {
                          newRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, _, oldRangeRangeValue))
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
                        oldRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, newRangeFromValue.getOrElseS(newRangeRangeValue), _)) orElseS {
                          newRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, _, oldRangeRangeValue))
                        },
                      rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                    )
                    val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, Value.FromValue.Null, newRangeRangeValue)

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
                        fromValue = newRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, _, oldRangeRangeValue)),
                        rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                      )

                    val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, Value.FromValue.Null, oldRangeRangeValue)

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
                      fromValue = newRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, _, oldRangeRangeValue)),
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
                        fromValue = newRangeFromValue.flatMapS(ValueMerger(newRangeFromKey, _, oldRangeRangeValue)),
                        rangeValue = ValueMerger(newRangeRangeValue, oldRangeRangeValue)
                      )

                    val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, Value.FromValue.Null, newRangeRangeValue)

                    add(upperSplit)
                    add(middleSplit)
                    doMerge(newKeyValues.dropPrepend(lowerSplit), oldKeyValues.dropHead())
                  }
                }
              }

            case null =>
              addAll(newKeyValues.iterator)
          }

        case null =>
          oldKeyValues.iterator foreach add
      }

    doMerge(newKeyValues, oldKeyValues)

    if (tailGap.nonEmpty) {
      if (isLastLevel) {
        builder.result.asInstanceOf[ListBuffer[Memory]].lastOption foreach {
          mergedLast =>
            assert(mergedLast.key < tailGap.head.key)
        }
      } else {
        val mergedLastKey = builder.result.asInstanceOf[ListBuffer[Memory]].last.key
        assert(mergedLastKey < tailGap.head.key)
      }
    }

    addAll(tailGap.iterator)
  }
}
