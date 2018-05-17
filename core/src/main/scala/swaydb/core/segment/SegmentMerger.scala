/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, Persistent, Value, _}
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.segment.KeyValueMerger._
import swaydb.core.util.SliceUtil._
import swaydb.core.util.TryUtil
import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import swaydb.data.util.StorageUnits._

private[core] object SegmentMerger extends LazyLogging {

  def mergeSmallerSegmentWithPrevious(segments: ListBuffer[ListBuffer[KeyValue.WriteOnly]],
                                      minSegmentSize: Long,
                                      forMemory: Boolean,
                                      bloomFilterFalsePositiveRate: Double): ListBuffer[ListBuffer[KeyValue.WriteOnly]] =
    if (segments.length >= 2 && ((forMemory && segments.last.memorySegmentSize < minSegmentSize) || segments.last.persistentSegmentSize < minSegmentSize)) {
      val newSegments = segments dropRight 1
      val newSegmentsLast = newSegments.last
      segments.last foreach {
        keyValue =>
          newSegmentsLast += keyValue.updateStats(bloomFilterFalsePositiveRate, newSegmentsLast.lastOption)
      }
      newSegments
    } else
      segments

  def addKeyValue(keyValueToAdd: KeyValue.ReadOnly,
                  segmentKeyValues: ListBuffer[ListBuffer[KeyValue.WriteOnly]],
                  minSegmentSize: Long,
                  forInMemory: Boolean,
                  isLastLevel: Boolean,
                  bloomFilterFalsePositiveRate: Double): Try[Unit] = {

    def doAdd(keyValueToAdd: Option[KeyValue.WriteOnly] => KeyValue.WriteOnly): Unit = {

      val currentSplitsLastKeyValue = segmentKeyValues.lastOption.flatMap(_.lastOption)

      val currentSegmentSize =
        if (forInMemory)
          currentSplitsLastKeyValue.map(_.stats.memorySegmentSize).getOrElse(0)
        else
          currentSplitsLastKeyValue.map(_.stats.segmentSize).getOrElse(0)

      val nextKeyValueWithUpdatedStats: KeyValue.WriteOnly = keyValueToAdd(currentSplitsLastKeyValue)

      val segmentSizeWithNextKeyValue =
        if (forInMemory)
          currentSegmentSize + nextKeyValueWithUpdatedStats.stats.thisKeyValueMemorySize
        else
          currentSegmentSize + nextKeyValueWithUpdatedStats.stats.thisKeyValuesSegmentSizeWithoutFooter

      def startNewSegment(): Unit =
        segmentKeyValues += ListBuffer[KeyValue.WriteOnly]()

      def addKeyValue(): Unit =
        segmentKeyValues.last += nextKeyValueWithUpdatedStats

      if (segmentSizeWithNextKeyValue >= minSegmentSize) {
        addKeyValue()
        startNewSegment()
      } else
        addKeyValue()
    }

    keyValueToAdd match {
      case fixed: KeyValue.ReadOnly.Fixed =>
        if (isLastLevel && fixed.isOverdue())
          TryUtil.successUnit
        else
          fixed match {
            case Memory.Put(key, value, deadline) =>
              doAdd(Transient.Put(key, value, bloomFilterFalsePositiveRate, _, deadline))
              TryUtil.successUnit

            case put: Persistent.Put =>
              put.getOrFetchValue map {
                value =>
                  doAdd(Transient.Put(put.key, value, bloomFilterFalsePositiveRate, _, put.deadline))
              }

            case remove @ (_: Memory.Remove | _: Persistent.Remove) =>
              if (!isLastLevel) doAdd(Transient.Remove(keyValueToAdd.key, bloomFilterFalsePositiveRate, _, remove.deadline))
              TryUtil.successUnit

            case Memory.Update(key, value, deadline) =>
              if (!isLastLevel) doAdd(Transient.Update(key, value, bloomFilterFalsePositiveRate, _, deadline))
              TryUtil.successUnit

            case update: Persistent.Update =>
              if (!isLastLevel)
                update.getOrFetchValue map {
                  value =>
                    doAdd(Transient.Update(update.key, value, bloomFilterFalsePositiveRate, _, update.deadline))
                }
              else
                TryUtil.successUnit

          }
      case range: KeyValue.ReadOnly.Range =>
        if (isLastLevel)
          range.fetchFromValue match {
            case Success(fromValue) =>
              fromValue match {
                case Some(fromValue) =>
                  fromValue match {
                    case put @ Value.Put(fromValue, deadline) =>
                      if (put.hasTimeLeft()) doAdd(Transient.Put(range.fromKey, fromValue, bloomFilterFalsePositiveRate, _, deadline))
                      TryUtil.successUnit
                    case _: Value.Remove | _: Value.Update =>
                      TryUtil.successUnit
                  }
                case None =>
                  TryUtil.successUnit
              }
            case Failure(exception) =>
              Failure(exception)
          }
        else
          range.fetchFromAndRangeValue map {
            case (fromValue, rangeValue) =>
              doAdd(Transient.Range(range.fromKey, range.toKey, fromValue, rangeValue, bloomFilterFalsePositiveRate, _))
          }
    }
  }

  def split(keyValues: Iterable[KeyValue.ReadOnly],
            minSegmentSize: Long,
            isLastLevel: Boolean,
            forInMemory: Boolean,
            bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]]): Iterable[Iterable[KeyValue.WriteOnly]] = {
    val splits = ListBuffer[ListBuffer[KeyValue.WriteOnly]](ListBuffer())
    keyValues foreach {
      keyValue =>
        addKeyValue(
          keyValueToAdd = keyValue,
          segmentKeyValues = splits,
          minSegmentSize = minSegmentSize,
          forInMemory = forInMemory,
          isLastLevel = isLastLevel,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
        )
    }
    mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate).filter(_.nonEmpty)
  }

  /**
    * TODO: Both inputs are Memory so temporarily it's OK to call .get because Memory key-values do not do IO. But this should be fixed and .get should be invoked.
    *
    * Need a type class implementation on executing side effects of merging key-values, one for Memory key-values and other for persistent.
    *
    */
  def merge(newKeyValues: Slice[Memory],
            oldKeyValues: Slice[Memory],
            hasTimeLeftAtLeast: FiniteDuration)(implicit ordering: Ordering[Slice[Byte]]): ListBuffer[KeyValue.WriteOnly] =
    merge(newKeyValues, oldKeyValues, 1000.gb, false, true, 0.1, hasTimeLeftAtLeast).get.flatten.asInstanceOf[ListBuffer[KeyValue.WriteOnly]]

  def merge(newKeyValue: Memory,
            oldKeyValue: Memory,
            hasTimeLeftAtLeast: FiniteDuration)(implicit ordering: Ordering[Slice[Byte]]): ListBuffer[KeyValue.WriteOnly] =
    merge(Slice(newKeyValue), Slice(oldKeyValue), 1000.gb, false, true, 0.1, hasTimeLeftAtLeast).get.flatten.asInstanceOf[ListBuffer[KeyValue.WriteOnly]]


  def merge(newKeyValues: Slice[KeyValue.ReadOnly],
            oldKeyValues: Slice[KeyValue.ReadOnly],
            minSegmentSize: Long,
            isLastLevel: Boolean,
            forInMemory: Boolean,
            bloomFilterFalsePositiveRate: Double,
            hasTimeLeftAtLeast: FiniteDuration)(implicit ordering: Ordering[Slice[Byte]]): Try[Iterable[Iterable[KeyValue.WriteOnly]]] = {
    import ordering._
    val splits = ListBuffer[ListBuffer[KeyValue.WriteOnly]](ListBuffer.empty)

    var newRangeKeyValueStash = Option.empty[Memory.Range]
    var oldRangeKeyValueStash = Option.empty[Memory.Range]

    @tailrec
    def doMerge(newKeyValues: Slice[KeyValue.ReadOnly],
                oldKeyValues: Slice[KeyValue.ReadOnly]): Try[ListBuffer[ListBuffer[KeyValue.WriteOnly]]] = {

      def dropOldKeyValue(stash: Option[Memory.Range] = None) =
        if (oldRangeKeyValueStash.isDefined) {
          oldRangeKeyValueStash = stash
          oldKeyValues
        } else {
          oldRangeKeyValueStash = stash
          oldKeyValues.drop(1)
        }

      def dropNewKeyValue(stash: Option[Memory.Range] = None) =
        if (newRangeKeyValueStash.isDefined) {
          newRangeKeyValueStash = stash
          newKeyValues
        } else {
          newRangeKeyValueStash = stash
          newKeyValues.drop(1)
        }

      def add(nextKeyValue: KeyValue.ReadOnly): Try[Unit] =
        addKeyValue(nextKeyValue, splits, minSegmentSize, forInMemory, isLastLevel, bloomFilterFalsePositiveRate)

      (newRangeKeyValueStash orElse newKeyValues.headOption, oldRangeKeyValueStash orElse oldKeyValues.headOption) match {

        case (Some(newKeyValue: KeyValue.ReadOnly.Fixed), Some(oldKeyValue: KeyValue.ReadOnly.Fixed)) if oldKeyValue.key < newKeyValue.key =>
          add(oldKeyValue) match {
            case Success(_) =>
              doMerge(newKeyValues, dropOldKeyValue())
            case Failure(exception) =>
              Failure(exception)
          }

        case (Some(newKeyValue: KeyValue.ReadOnly.Fixed), Some(oldKeyValue: KeyValue.ReadOnly.Fixed)) if newKeyValue.key < oldKeyValue.key =>
          add(newKeyValue) match {
            case Success(_) =>
              doMerge(dropNewKeyValue(), oldKeyValues)
            case Failure(exception) =>
              Failure(exception)
          }

        case (Some(newKeyValue: KeyValue.ReadOnly.Fixed), Some(oldKeyValue: KeyValue.ReadOnly.Fixed)) => //equals
          applyValue(newKeyValue, oldKeyValue, hasTimeLeftAtLeast) flatMap add match {
            case Success(_) =>
              doMerge(dropNewKeyValue(), dropOldKeyValue())
            case Failure(exception) =>
              Failure(exception)
          }

        /**
          * If the input is an overwrite key-value and the existing is a range key-value.
          */
        case (Some(newKeyValue: KeyValue.ReadOnly.Fixed), Some(oldRangeKeyValue: ReadOnly.Range)) =>
          if (newKeyValue.key < oldRangeKeyValue.fromKey) {
            add(newKeyValue) match {
              case Success(_) =>
                doMerge(dropNewKeyValue(), oldKeyValues)
              case Failure(exception) =>
                Failure(exception)
            }
          } else if (newKeyValue.key >= oldRangeKeyValue.toKey) {
            add(oldRangeKeyValue) match {
              case Success(_) =>
                doMerge(newKeyValues, dropOldKeyValue())
              case Failure(exception) =>
                Failure(exception)
            }
          } else { //is in-range key
            oldRangeKeyValue.fetchFromAndRangeValue match {
              case Success((oldFromValue, oldRangeRangeValue)) if newKeyValue.key equiv oldRangeKeyValue.fromKey =>
                applyValue(newKeyValue, oldFromValue.getOrElse(oldRangeRangeValue), hasTimeLeftAtLeast) match {
                  case Success(newFromValue) =>
                    val oldStash =
                      Memory.Range(
                        fromKey = oldRangeKeyValue.fromKey,
                        toKey = oldRangeKeyValue.toKey,
                        fromValue = Some(newFromValue),
                        rangeValue = oldRangeRangeValue
                      )
                    doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(oldStash)))

                  case Failure(exception) =>
                    Failure(exception)
                }

              case Success((oldFromValue, oldRangeValue)) => //else it's a mid range value - split required.
                applyValue(newKeyValue, oldRangeValue, hasTimeLeftAtLeast) match {
                  case Success(newFromValue) =>
                    val lowerSplit = Memory.Range(oldRangeKeyValue.fromKey, newKeyValue.key, oldFromValue, oldRangeValue)
                    val upperSplit = Memory.Range(newKeyValue.key, oldRangeKeyValue.toKey, Some(newFromValue), oldRangeValue)
                    add(lowerSplit) match {
                      case Success(_) =>
                        doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(upperSplit)))
                      case Failure(exception) =>
                        Failure(exception)
                    }
                  case Failure(exception) =>
                    Failure(exception)
                }
              case Failure(exception) =>
                Failure(exception)
            }
          }

        /**
          * If the input is a range and the existing is a fixed key-value.
          */
        case (Some(newRangeKeyValue: ReadOnly.Range), Some(oldKeyValue: KeyValue.ReadOnly.Fixed)) =>
          if (oldKeyValue.key >= newRangeKeyValue.toKey) {
            add(newRangeKeyValue) match {
              case Success(_) =>
                doMerge(dropNewKeyValue(), oldKeyValues)
              case Failure(exception) =>
                Failure(exception)
            }
          } else if (oldKeyValue.key < newRangeKeyValue.fromKey) {
            add(oldKeyValue) match {
              case Success(_) =>
                doMerge(newKeyValues, dropOldKeyValue())
              case Failure(exception) =>
                Failure(exception)
            }
          } else { //is in-range key
            newRangeKeyValue.fetchFromAndRangeValue match {
              case Success((None | Some(Value.Remove(None)) | Some(_: Value.Put), Value.Remove(None))) => //if input is remove range, drop old key-value
                doMerge(newKeyValues, dropOldKeyValue())

              case Success((_, Value.Remove(None))) if newRangeKeyValue.fromKey != oldKeyValue.key => //if input is remove range, drop old key-value
                doMerge(newKeyValues, dropOldKeyValue())

              case Success((newRangeFromValue, newRangeRangeValue)) if newRangeKeyValue.fromKey equiv oldKeyValue.key =>
                applyValue(newRangeFromValue.getOrElse(newRangeRangeValue), oldKeyValue, hasTimeLeftAtLeast = hasTimeLeftAtLeast) match {
                  case Success(newFromValue) =>
                    val newKeyValue =
                      Memory.Range(
                        fromKey = newRangeKeyValue.fromKey,
                        toKey = newRangeKeyValue.toKey,
                        fromValue = Some(newFromValue),
                        rangeValue = newRangeRangeValue
                      )
                    doMerge(dropNewKeyValue(Some(newKeyValue)), dropOldKeyValue())
                  case Failure(exception) =>
                    Failure(exception)
                }

              case Success((newRangeFromValue, newRangeRangeValue)) => //split required.
                applyValue(newRangeRangeValue, oldKeyValue, hasTimeLeftAtLeast) match {
                  case Success(newFromValue) =>
                    val lowerSplit = Memory.Range(newRangeKeyValue.fromKey, oldKeyValue.key, newRangeFromValue, newRangeRangeValue)
                    val upperSplit = Memory.Range(oldKeyValue.key, newRangeKeyValue.toKey, Some(newFromValue), newRangeRangeValue)
                    add(lowerSplit) match {
                      case Success(_) =>
                        doMerge(dropNewKeyValue(stash = Some(upperSplit)), dropOldKeyValue())
                      case Failure(exception) =>
                        Failure(exception)
                    }
                  case Failure(exception) =>
                    Failure(exception)

                }

              case Failure(exception) =>
                Failure(exception)
            }
          }

        /**
          * If both the key-values are ranges.
          */
        case (Some(newRangeKeyValue: ReadOnly.Range), Some(oldRangeKeyValue: ReadOnly.Range)) =>
          if (newRangeKeyValue.toKey <= oldRangeKeyValue.fromKey) {
            add(newRangeKeyValue) match {
              case Success(_) =>
                doMerge(dropNewKeyValue(), oldKeyValues)
              case Failure(exception) =>
                Failure(exception)
            }

          } else if (oldRangeKeyValue.toKey <= newRangeKeyValue.fromKey) {
            add(oldRangeKeyValue) match {
              case Success(_) =>
                doMerge(newKeyValues, dropOldKeyValue())
              case Failure(exception) =>
                Failure(exception)
            }
          } else {
            newRangeKeyValue.fetchFromAndRangeValue match {
              case Success((newRangeFromValue, newRangeRangeValue)) =>
                oldRangeKeyValue.fetchFromAndRangeValue match {
                  case Success((oldRangeFromValue, oldRangeRangeValue)) =>
                    val newRangeFromKey = newRangeKeyValue.fromKey
                    val newRangeToKey = newRangeKeyValue.toKey
                    val oldRangeFromKey = oldRangeKeyValue.fromKey
                    val oldRangeToKey = oldRangeKeyValue.toKey

                    if (newRangeFromKey < oldRangeFromKey) {
                      //1   -     15
                      //      10   -  20
                      if (newRangeToKey < oldRangeToKey) {
                        val upperSplit = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                        val middleSplit = Memory.Range(oldRangeFromKey, newRangeToKey, oldRangeFromValue.map(applyValue(newRangeRangeValue, _, hasTimeLeftAtLeast).get), applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get)
                        val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)

                        add(upperSplit).flatMap(_ => add(middleSplit)) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(lowerSplit)))

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      } else if (newRangeToKey equiv oldRangeToKey) {
                        //1      -      20
                        //      10   -  20
                        val upperSplit = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                        val lowerSplit = Memory.Range(oldRangeFromKey, oldRangeToKey, oldRangeFromValue.map(applyValue(newRangeRangeValue, _, hasTimeLeftAtLeast).get), applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get)

                        add(upperSplit).flatMap(_ => add(lowerSplit)) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(), dropOldKeyValue())

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      } else {
                        //1      -         21
                        //      10   -  20
                        val upperSplit = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                        val middleSplit = Memory.Range(oldRangeFromKey, oldRangeToKey, oldRangeFromValue.map(applyValue(newRangeRangeValue, _, hasTimeLeftAtLeast).get), applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get)
                        val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)

                        add(upperSplit).flatMap(_ => add(middleSplit)) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(stash = Some(lowerSplit)), dropOldKeyValue())

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      }
                    } else if (newRangeFromKey equiv oldRangeFromKey) {
                      //      10 - 15
                      //      10   -  20
                      if (newRangeToKey < oldRangeToKey) {
                        val upperSplit = Memory.Range(
                          fromKey = newRangeFromKey,
                          toKey = newRangeToKey,
                          fromValue = oldRangeFromValue.map(applyValue(newRangeFromValue.getOrElse(newRangeRangeValue), _, hasTimeLeftAtLeast).get) orElse {
                            newRangeFromValue.map(applyValue(_, oldRangeRangeValue, hasTimeLeftAtLeast).get)
                          },
                          rangeValue = applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get
                        )
                        val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)

                        add(upperSplit) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(lowerSplit)))

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      } else if (newRangeToKey equiv oldRangeToKey) {
                        //      10   -  20
                        //      10   -  20
                        val update = Memory.Range(
                          fromKey = newRangeFromKey,
                          toKey = newRangeToKey,
                          fromValue = oldRangeFromValue.map(applyValue(newRangeFromValue.getOrElse(newRangeRangeValue), _, hasTimeLeftAtLeast).get) orElse {
                            newRangeFromValue.map(applyValue(_, oldRangeRangeValue, hasTimeLeftAtLeast).get)
                          },
                          rangeValue = applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get
                        )

                        add(update) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(), dropOldKeyValue())

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      } else {
                        //      10   -     21
                        //      10   -  20
                        val upperSplit = Memory.Range(
                          fromKey = newRangeFromKey,
                          toKey = oldRangeToKey,
                          fromValue = oldRangeFromValue.map(applyValue(newRangeFromValue.getOrElse(newRangeRangeValue), _, hasTimeLeftAtLeast).get) orElse {
                            newRangeFromValue.map(applyValue(_, oldRangeRangeValue, hasTimeLeftAtLeast).get)
                          },
                          rangeValue = applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get
                        )
                        val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)

                        add(upperSplit) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(stash = Some(lowerSplit)), dropOldKeyValue())

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      }
                    } else {
                      //        11 - 15
                      //      10   -   20
                      if (newRangeToKey < oldRangeToKey) {
                        val upperSplit = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)
                        val middleSplit = Memory.Range(newRangeFromKey, newRangeToKey, newRangeFromValue.map(applyValue(_, oldRangeRangeValue, hasTimeLeftAtLeast).get), applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get)
                        val lowerSplit = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)

                        add(upperSplit).flatMap(_ => add(middleSplit)) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(lowerSplit)))

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      } else if (newRangeToKey equiv oldRangeToKey) {
                        //        11 -   20
                        //      10   -   20
                        val upperSplit = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)
                        val lowerSplit = Memory.Range(newRangeFromKey, newRangeToKey, newRangeFromValue.map(applyValue(_, oldRangeRangeValue, hasTimeLeftAtLeast).get), applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get)

                        add(upperSplit).flatMap(_ => add(lowerSplit)) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(), dropOldKeyValue())

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      } else {
                        //        11 -     21
                        //      10   -   20
                        val upperSplit = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)
                        val middleSplit = Memory.Range(newRangeFromKey, oldRangeToKey, newRangeFromValue.map(applyValue(_, oldRangeRangeValue, hasTimeLeftAtLeast).get), applyValue(newRangeRangeValue, oldRangeRangeValue, hasTimeLeftAtLeast).get)
                        val lowerSplit = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)

                        add(upperSplit).flatMap(_ => add(middleSplit)) match {
                          case Success(_) =>
                            doMerge(dropNewKeyValue(stash = Some(lowerSplit)), dropOldKeyValue())

                          case Failure(exception) =>
                            Failure(exception)
                        }
                      }
                    }

                  case Failure(exception) =>
                    Failure(exception)
                }

              case Failure(exception) =>
                Failure(exception)
            }
          }

        //there are no more oldKeyValues. Add all remaining newKeyValues
        case (Some(newKeyValue), None) =>
          add(newKeyValue) match {
            case Success(_) =>
              //if stash exists for newKeyValues, do not drop from the newKeyValue for the above add. Ignore head and continue adding remaining new keyValues.
              val remainingKeyValues = if (newRangeKeyValueStash.isDefined) newKeyValues else newKeyValues.drop(1)
              remainingKeyValues.tryForeach(add) match {
                case Some(Failure(exception)) =>
                  Failure(exception)
                case None =>
                  Success(mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate))
              }
            case Failure(exception) =>
              Failure(exception)
          }

        //there are no more newKeyValues. Add all remaining oldKeyValues
        case (None, Some(oldKeyValue)) =>
          add(oldKeyValue) match {
            case Success(_) =>
              //if stash exists for oldKeyValues, do not drop from the oldKeyValue from the above add. Ignore head and continue adding remaining old keyValues.
              val remainingKeyValues = if (oldRangeKeyValueStash.isDefined) oldKeyValues else oldKeyValues.drop(1)
              remainingKeyValues.tryForeach(add) match {
                case Some(Failure(exception)) =>
                  Failure(exception)
                case None =>
                  Success(mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate))
              }
            case Failure(exception) =>
              Failure(exception)
          }

        case (None, None) =>
          Success(mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate))
      }
    }

    try {
      doMerge(newKeyValues, oldKeyValues).map(_.filter(_.nonEmpty))
    } catch {
      case ex: Exception =>
        Failure(ex)
    }
  }

}
