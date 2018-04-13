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
import swaydb.core.util.SliceUtil._
import swaydb.core.util.TryUtil
import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

private[core] object SegmentMerge extends LazyLogging {

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
        fixed match {
          case Memory.Put(key, value) =>
            doAdd(Transient.Put(key, value, bloomFilterFalsePositiveRate, _))
            TryUtil.successUnit

          case put: Persistent.Put =>
            put.getOrFetchValue map {
              value =>
                doAdd(Transient.Put(put.key, value, bloomFilterFalsePositiveRate, _))
            }

          case _: Memory.Remove | _: Persistent.Remove =>
            if (isLastLevel)
              TryUtil.successUnit
            else {
              doAdd(Transient.Remove(keyValueToAdd.key, bloomFilterFalsePositiveRate, _))
              TryUtil.successUnit
            }
        }
      case range: KeyValue.ReadOnly.Range =>
        if (isLastLevel)
          range.fetchFromValue match {
            case Success(fromValue) =>
              fromValue match {
                case Some(fromValue) =>
                  fromValue match {
                    case Value.Put(fromValue) =>
                      doAdd(Transient.Put(range.fromKey, fromValue, bloomFilterFalsePositiveRate, _))
                      TryUtil.successUnit
                    case _: Value.Remove =>
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
    mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)
  }

  def merge(newKeyValues: Slice[KeyValue.ReadOnly],
            oldKeyValues: Slice[KeyValue.ReadOnly],
            minSegmentSize: Long,
            isLastLevel: Boolean,
            forInMemory: Boolean,
            bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]]): Try[Iterable[Iterable[KeyValue.WriteOnly]]] = {
    import ordering._
    val splits = ListBuffer[ListBuffer[KeyValue.WriteOnly]](ListBuffer())

    var newRangeKeyValueStash = Option.empty[Memory.Range]
    var oldRangeKeyValueStash = Option.empty[Memory.Range]

    @tailrec
    def doMerge(newKeyValues: Slice[KeyValue.ReadOnly],
                oldKeyValues: Slice[KeyValue.ReadOnly]): Try[ListBuffer[ListBuffer[KeyValue.WriteOnly]]] = {

      def dropOldKeyValue(stash: Option[Memory.Range] = None) = {
        if (oldRangeKeyValueStash.isDefined) {
          oldRangeKeyValueStash = stash
          oldKeyValues
        } else {
          oldRangeKeyValueStash = stash
          oldKeyValues.drop(1)
        }
      }

      def dropNewKeyValue(stash: Option[Memory.Range] = None) = {
        if (newRangeKeyValueStash.isDefined) {
          newRangeKeyValueStash = stash
          newKeyValues
        } else {
          newRangeKeyValueStash = stash
          newKeyValues.drop(1)
        }
      }

      def add(nextKeyValue: KeyValue.ReadOnly): Try[Unit] = {
        addKeyValue(nextKeyValue, splits, minSegmentSize, forInMemory, isLastLevel, bloomFilterFalsePositiveRate)
      }

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

        case (Some(newKeyValue: KeyValue.ReadOnly.Fixed), Some(_: KeyValue.ReadOnly.Fixed)) => //equals
          add(newKeyValue) match {
            case Success(_) =>
              doMerge(dropNewKeyValue(), dropOldKeyValue())
            case Failure(exception) =>
              Failure(exception)
          }

        /**
          * If the input is a fixed key-value and the existing is a range key-value.
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
            newKeyValue.toValue match {
              case Success(newFromValue) =>
                oldRangeKeyValue.fetchRangeValue match {
                  case Success(rangeValue) if newKeyValue.key equiv oldRangeKeyValue.fromKey =>
                    val oldStash = Some(Memory.Range(oldRangeKeyValue.fromKey, oldRangeKeyValue.toKey, Some(newFromValue), rangeValue))
                    doMerge(dropNewKeyValue(), dropOldKeyValue(oldStash))

                  case Success(rangeValue) => //else it's a mid range value - split required.
                    oldRangeKeyValue.fetchFromValue match {
                      case Success(oldFromValue) =>
                        val lowerSplit = Memory.Range(oldRangeKeyValue.fromKey, newKeyValue.key, oldFromValue, rangeValue)
                        val upperSplit = Memory.Range(newKeyValue.key, oldRangeKeyValue.toKey, Some(newFromValue), rangeValue)
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
              case Success((_, Value.Remove)) => //if input range is remove, drop old key-value
                doMerge(newKeyValues, dropOldKeyValue())

              case Success((Some(_), _)) if newRangeKeyValue.fromKey equiv oldKeyValue.key => //if fromValue is already set, ignore old.
                doMerge(newKeyValues, dropOldKeyValue())

              case Success((None, newRangeRangeValue: Value.Put)) if newRangeKeyValue.fromKey equiv oldKeyValue.key => //if fromValue is not set.
                val stash =
                  oldKeyValue match {
                    case _: Memory.Remove | _: Persistent.Remove =>
                      Some(Memory.Range(newRangeKeyValue.fromKey, newRangeKeyValue.toKey, Some(Value.Remove), newRangeRangeValue))

                    case _: Memory.Put | _: Persistent.Put =>
                      Some(Memory.Range(newRangeKeyValue.fromKey, newRangeKeyValue.toKey, Some(newRangeRangeValue), newRangeRangeValue))
                  }
                doMerge(dropNewKeyValue(stash = stash), dropOldKeyValue())

              case Success((fromValue, newRangeRangeValue: Value.Put)) => //split required.
                val lowerSplit = Memory.Range(newRangeKeyValue.fromKey, oldKeyValue.key, fromValue, newRangeRangeValue)
                val upperSplit =
                  oldKeyValue match {
                    case _: Memory.Remove | _: Persistent.Remove =>
                      Memory.Range(oldKeyValue.key, newRangeKeyValue.toKey, Some(Value.Remove), newRangeRangeValue)
                    case _: Memory.Put | _: Persistent.Put =>
                      Memory.Range(oldKeyValue.key, newRangeKeyValue.toKey, Some(newRangeRangeValue), newRangeRangeValue)
                  }
                add(lowerSplit) match {
                  case Success(_) =>
                    doMerge(dropNewKeyValue(stash = Some(upperSplit)), dropOldKeyValue())
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

                    oldRangeRangeValue match {
                      case Value.Remove if newRangeFromKey equiv oldRangeFromKey =>
                        val newFromValue = newRangeFromValue orElse {
                          oldRangeFromValue map {
                            case Value.Remove =>
                              Value.Remove
                            case Value.Put(_) =>
                              newRangeRangeValue
                          }
                        }
                        if (newRangeToKey <= oldRangeToKey) {
                          val newOldRange = Memory.Range(oldRangeFromKey, oldRangeToKey, newFromValue, oldRangeRangeValue)
                          doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(newOldRange)))
                        } else { // newRangeToKey > oldRangeToKey
                          val left = Memory.Range(oldRangeFromKey, oldRangeToKey, newFromValue, oldRangeRangeValue)
                          val right = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case Value.Remove if newRangeFromKey > oldRangeFromKey =>
                        if (newRangeToKey <= oldRangeToKey) {
                          newRangeFromValue match {
                            case Some(_) =>
                              val left = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)
                              val right = Memory.Range(newRangeFromKey, oldRangeToKey, newRangeFromValue, oldRangeRangeValue)
                              add(left) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                                case Failure(exception) =>
                                  Failure(exception)
                              }
                            case None =>
                              doMerge(dropNewKeyValue(), oldKeyValues)
                          }
                        } else { //newRangeToKey > oldRangeToKey
                          newRangeFromValue match {
                            case Some(_) =>
                              val left = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)
                              val mid = Memory.Range(newRangeFromKey, oldRangeToKey, newRangeFromValue, oldRangeRangeValue)
                              val right = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)
                              add(left).flatMap(_ => add(mid)) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                                case Failure(exception) =>
                                  Failure(exception)
                              }
                            case None =>
                              val left = Memory.Range(oldRangeFromKey, oldRangeToKey, oldRangeFromValue, oldRangeRangeValue)
                              val right = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)
                              add(left) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                                case Failure(exception) =>
                                  Failure(exception)
                              }
                          }
                        }

                      case Value.Remove if newRangeFromKey < oldRangeFromKey =>
                        if (newRangeToKey <= oldRangeToKey) {
                          val left = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                          val right = Memory.Range(oldRangeFromKey, oldRangeToKey, oldRangeFromValue, oldRangeRangeValue)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        } else { //newRangeToKey > oldRangeToKey
                          val left = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                          val mid = Memory.Range(oldRangeFromKey, oldRangeToKey, oldRangeFromValue, oldRangeRangeValue)
                          val right = Memory.Range(oldRangeToKey, newRangeToKey, None, newRangeRangeValue)
                          add(left).flatMap(_ => add(mid)) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case _: Value.Put if newRangeFromKey equiv oldRangeFromKey =>
                        //if fromValue is set in newRange, use it! else check if the fromValue is set in oldRange and replace it with newRange's rangeValue.
                        val newFromValue = newRangeFromValue orElse {
                          oldRangeFromValue map {
                            case Value.Remove =>
                              Value.Remove
                            case Value.Put(_) =>
                              newRangeRangeValue
                          }
                        }
                        if (newRangeToKey >= oldRangeToKey) { //new range completely overlaps old range.
                          val adjustedNewRange = Memory.Range(newRangeFromKey, newRangeToKey, newFromValue, newRangeRangeValue)
                          doMerge(dropNewKeyValue(stash = Some(adjustedNewRange)), dropOldKeyValue())
                        } else { // newRangeToKey < oldRangeToKey
                          val left = Memory.Range(newRangeFromKey, newRangeToKey, newFromValue, newRangeRangeValue)
                          val right = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case _: Value.Put if newRangeFromKey > oldRangeFromKey =>
                        //if fromValue is set in newRange, use it! else check if the fromValue is set in oldRange and replace it with newRange's rangeValue.
                        if (newRangeToKey >= oldRangeToKey) { //new range completely overlaps old range's right half.
                          val left = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)
                          val right = Memory.Range(newRangeFromKey, newRangeToKey, newRangeFromValue, newRangeRangeValue)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        } else { // newRangeToKey < oldRangeToKey
                          val left = Memory.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeRangeValue)
                          val mid = Memory.Range(newRangeFromKey, newRangeToKey, newRangeFromValue, newRangeRangeValue)
                          val right = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)
                          add(left).flatMap(_ => add(mid)) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case _: Value.Put if newRangeFromKey < oldRangeFromKey =>
                        //if fromValue is set in newRange, use it! else check if the fromValue is set in oldRange and replace it with newRange's rangeValue.
                        if (newRangeToKey >= oldRangeToKey) {
                          oldRangeFromValue match {
                            case Some(oldRangeFromValue) =>
                              val left = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                              val right = Memory.Range(oldRangeFromKey, newRangeToKey, if (oldRangeFromValue.isRemove) Some(Value.Remove) else Some(newRangeRangeValue), newRangeRangeValue)
                              add(left) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                                case Failure(exception) =>
                                  Failure(exception)
                              }

                            case None =>
                              doMerge(newKeyValues, dropOldKeyValue())
                          }
                        } else { // newRangeToKey < oldRangeToKey
                          oldRangeFromValue match {
                            case Some(oldRangeFromValue) =>
                              val left = Memory.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeRangeValue)
                              val mid = Memory.Range(oldRangeFromKey, newRangeToKey, if (oldRangeFromValue.isRemove) Some(Value.Remove) else Some(newRangeRangeValue), newRangeRangeValue)
                              val right = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)
                              add(left).flatMap(_ => add(mid)) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                                case Failure(exception) =>
                                  Failure(exception)
                              }

                            case None =>
                              val left = Memory.Range(newRangeFromKey, newRangeToKey, newRangeFromValue, newRangeRangeValue)
                              val right = Memory.Range(newRangeToKey, oldRangeToKey, None, oldRangeRangeValue)
                              add(left) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                                case Failure(exception) =>
                                  Failure(exception)
                              }
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

    doMerge(newKeyValues, oldKeyValues).map(_.filter(_.nonEmpty))
  }

}
