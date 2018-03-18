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

import swaydb.core.data.KeyValue.{FixedWriteOnly, RangeWriteOnly}
import swaydb.core.data.SegmentEntry.Put
import swaydb.core.data.Transient.Remove
import swaydb.core.data.{KeyValue, SegmentEntry, Transient, Value}
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.util.SliceUtil._
import swaydb.core.util.TryUtil
import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

private[core] object SegmentMerge {

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

  def addKeyValue(keyValueToAdd: KeyValue.WriteOnly,
                  segmentKeyValues: ListBuffer[ListBuffer[KeyValue.WriteOnly]],
                  minSegmentSize: Long,
                  forInMemory: Boolean,
                  isLastLevel: Boolean,
                  bloomFilterFalsePositiveRate: Double): Try[Unit] = {

    def doAdd(keyValueToAdd: KeyValue.WriteOnly): Unit = {
      val currentSplitsLastKeyValue = segmentKeyValues.lastOption.flatMap(_.lastOption)

      val currentSegmentSize =
        if (forInMemory)
          currentSplitsLastKeyValue.map(_.stats.memorySegmentSize).getOrElse(0)
        else
          currentSplitsLastKeyValue.map(_.stats.segmentSize).getOrElse(0)

      val nextKeyValueWithUpdatedStats = keyValueToAdd.updateStats(bloomFilterFalsePositiveRate, currentSplitsLastKeyValue)

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

    if (isLastLevel)
      keyValueToAdd match {
        case fixed: KeyValue.FixedWriteOnly =>
          fixed match {
            case _: Put => doAdd(keyValueToAdd)
            case _: Transient.Put => doAdd(keyValueToAdd)
            case _: Remove =>
            case _: SegmentEntry.Remove =>
          }
          TryUtil.successUnit
        case range: KeyValue.RangeWriteOnly =>
          range.fetchFromValue match {
            case Success(fromValue) =>
              fromValue match {
                case Some(fromValue) =>
                  fromValue match {
                    case Value.Put(fromValue) =>
                      doAdd(Transient.Put(range.fromKey, fromValue, bloomFilterFalsePositiveRate, None))
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
      }
    else {
      doAdd(keyValueToAdd)
      TryUtil.successUnit
    }
  }

  def split(keyValues: Slice[KeyValue.WriteOnly],
            minSegmentSize: Long,
            removeDeletes: Boolean,
            forInMemory: Boolean,
            bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]]): Try[Iterable[Iterable[KeyValue.WriteOnly]]] =
    merge(keyValues, Slice.create[KeyValue.WriteOnly](0), minSegmentSize, isLastLevel = removeDeletes, forInMemory = forInMemory, bloomFilterFalsePositiveRate)

  def merge(newKeyValues: Slice[KeyValue.WriteOnly],
            oldKeyValues: Slice[KeyValue.WriteOnly],
            minSegmentSize: Long,
            isLastLevel: Boolean,
            forInMemory: Boolean,
            bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]]): Try[Iterable[Iterable[KeyValue.WriteOnly]]] = {
    import ordering._
    val splits = ListBuffer[ListBuffer[KeyValue.WriteOnly]](ListBuffer())

    var newRangeKeyValueStash = Option.empty[KeyValue.RangeWriteOnly]
    var oldRangeKeyValueStash = Option.empty[KeyValue.RangeWriteOnly]

    @tailrec
    def doMerge(newKeyValues: Slice[KeyValue.WriteOnly],
                oldKeyValues: Slice[KeyValue.WriteOnly]): Try[ListBuffer[ListBuffer[KeyValue.WriteOnly]]] = {

      def dropOldKeyValue(stash: Option[KeyValue.RangeWriteOnly] = None) =
        if (oldRangeKeyValueStash.isDefined) {
          oldRangeKeyValueStash = stash
          oldKeyValues
        } else {
          oldRangeKeyValueStash = stash
          oldKeyValues.drop(1)
        }

      def dropNewKeyValue(stash: Option[KeyValue.RangeWriteOnly] = None) = {
        if (newRangeKeyValueStash.isDefined) {
          newRangeKeyValueStash = stash
          newKeyValues
        } else {
          newRangeKeyValueStash = stash
          newKeyValues.drop(1)
        }
      }

      def add(nextKeyValue: KeyValue.WriteOnly): Try[Unit] =
        addKeyValue(nextKeyValue, splits, minSegmentSize, forInMemory, isLastLevel, bloomFilterFalsePositiveRate)

      (newRangeKeyValueStash orElse newKeyValues.headOption, oldRangeKeyValueStash orElse oldKeyValues.headOption) match {

        case (Some(newKeyValue: FixedWriteOnly), Some(oldKeyValue: FixedWriteOnly)) if oldKeyValue.key < newKeyValue.key =>
          add(oldKeyValue) match {
            case Success(_) =>
              doMerge(newKeyValues, dropOldKeyValue())
            case Failure(exception) =>
              Failure(exception)
          }

        case (Some(newKeyValue: FixedWriteOnly), Some(oldKeyValue: FixedWriteOnly)) if newKeyValue.key < oldKeyValue.key =>
          add(newKeyValue) match {
            case Success(_) =>
              doMerge(dropNewKeyValue(), oldKeyValues)
            case Failure(exception) =>
              Failure(exception)
          }

        case (Some(newKeyValue: FixedWriteOnly), Some(_: FixedWriteOnly)) => //equals
          add(newKeyValue) match {
            case Success(_) =>
              doMerge(dropNewKeyValue(), dropOldKeyValue())
            case Failure(exception) =>
              Failure(exception)
          }

        /**
          * If the input is a fixed key-value and the existing is a range key-value.
          */
        case (Some(newKeyValue: FixedWriteOnly), Some(oldRangeKeyValue: RangeWriteOnly)) =>
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
                    val oldStash = Some(Transient.Range(oldRangeKeyValue.fromKey, oldRangeKeyValue.toKey, Some(newFromValue), rangeValue, bloomFilterFalsePositiveRate, None))
                    doMerge(dropNewKeyValue(), dropOldKeyValue(oldStash))

                  case Success(rangeValue) => //else it's a mid range value - split required.
                    oldRangeKeyValue.fetchFromValue match {
                      case Success(oldFromValue) =>
                        val lowerSplit = Transient.Range(oldRangeKeyValue.fromKey, newKeyValue.key, oldFromValue, rangeValue, bloomFilterFalsePositiveRate, None)
                        val upperSplit = Transient.Range(newKeyValue.key, oldRangeKeyValue.toKey, Some(newFromValue), rangeValue, bloomFilterFalsePositiveRate, None)
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
        case (Some(newRangeKeyValue: RangeWriteOnly), Some(oldKeyValue: FixedWriteOnly)) =>
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
                    case _: Remove | _: SegmentEntry.Remove =>
                      Some(Transient.Range[Value.Remove, Value.Put](newRangeKeyValue.fromKey, newRangeKeyValue.toKey, Some(Value.Remove), newRangeRangeValue, bloomFilterFalsePositiveRate, None))
                    case _ =>
                      Some(Transient.Range(newRangeKeyValue.fromKey, newRangeKeyValue.toKey, Some(newRangeRangeValue), newRangeRangeValue, bloomFilterFalsePositiveRate, None))
                  }
                doMerge(dropNewKeyValue(stash = stash), dropOldKeyValue())

              case Success((fromValue, newRangeRangeValue: Value.Put)) => //split required.
                val lowerSplit = Transient.Range[Value.Fixed, Value.Fixed](newRangeKeyValue.fromKey, oldKeyValue.key, fromValue, newRangeRangeValue, bloomFilterFalsePositiveRate, None)
                val upperSplit =
                  oldKeyValue match {
                    case _: Remove | _: SegmentEntry.Remove =>
                      Transient.Range[Value.Remove, Value.Put](oldKeyValue.key, newRangeKeyValue.toKey, Some(Value.Remove), newRangeRangeValue, bloomFilterFalsePositiveRate, None)
                    case _ =>
                      Transient.Range(oldKeyValue.key, newRangeKeyValue.toKey, Some(newRangeRangeValue), newRangeRangeValue, bloomFilterFalsePositiveRate, None)
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
        case (Some(newRangeKeyValue: RangeWriteOnly), Some(oldRangeKeyValue: RangeWriteOnly)) =>
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
            newRangeKeyValue.toValue match {
              case Success((newRangeFromKey, newRangeValue)) =>
                oldRangeKeyValue.toValue match {
                  case Success((oldRangeFromKey, oldRangeValue)) =>

                    (newRangeValue.fromValue, newRangeValue.rangeValue, oldRangeValue.fromValue, oldRangeValue.rangeValue) match {
                      case (_, _, _, Value.Remove) if newRangeFromKey equiv oldRangeFromKey =>
                        val newFromValue = newRangeValue.fromValue orElse (if (oldRangeValue.fromValue.isDefined) Some(newRangeValue.rangeValue) else None)
                        if (newRangeValue.toKey <= oldRangeValue.toKey) {
                          val newOldRange = Transient.Range(oldRangeFromKey, oldRangeValue.toKey, newFromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(newOldRange)))
                        } else { // newRangeValue.toKey > oldRangeValue.toKey
                          val left = Transient.Range(oldRangeFromKey, oldRangeValue.toKey, newFromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val right = Transient.Range(oldRangeValue.toKey, newRangeValue.toKey, None, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case (newRangeFromValue, _, _, Value.Remove) if newRangeFromKey > oldRangeFromKey =>
                        if (newRangeValue.toKey <= oldRangeValue.toKey) {
                          newRangeFromValue match {
                            case Some(_) =>
                              val left = Transient.Range(oldRangeFromKey, newRangeFromKey, oldRangeValue.fromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val right = Transient.Range(newRangeFromKey, oldRangeValue.toKey, newRangeValue.fromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              add(left) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                                case Failure(exception) =>
                                  Failure(exception)
                              }
                            case None =>
                              doMerge(dropNewKeyValue(), oldKeyValues)
                          }
                        } else { //newRangeValue.toKey > oldRangeValue.toKey
                          newRangeFromValue match {
                            case Some(_) =>
                              val left = Transient.Range(oldRangeFromKey, newRangeFromKey, oldRangeValue.fromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val mid = Transient.Range(newRangeFromKey, oldRangeValue.toKey, newRangeValue.fromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val right = Transient.Range(oldRangeValue.toKey, newRangeValue.toKey, None, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              add(left).flatMap(_ => add(mid)) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                                case Failure(exception) =>
                                  Failure(exception)
                              }
                            case None =>
                              val left = Transient.Range(oldRangeFromKey, oldRangeValue.toKey, oldRangeValue.fromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val right = Transient.Range(oldRangeValue.toKey, newRangeValue.toKey, None, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              add(left) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                                case Failure(exception) =>
                                  Failure(exception)
                              }
                          }
                        }

                      case (_, _, _, Value.Remove) if newRangeFromKey < oldRangeFromKey =>
                        if (newRangeValue.toKey <= oldRangeValue.toKey) {
                          val left = Transient.Range(newRangeFromKey, oldRangeFromKey, newRangeValue.fromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val right = Transient.Range(oldRangeFromKey, oldRangeValue.toKey, oldRangeValue.fromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        } else { //newRangeValue.toKey > oldRangeValue.toKey
                          val left = Transient.Range(newRangeFromKey, oldRangeFromKey, newRangeValue.fromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val mid = Transient.Range(oldRangeFromKey, oldRangeValue.toKey, oldRangeValue.fromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val right = Transient.Range(oldRangeValue.toKey, newRangeValue.toKey, None, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          add(left).flatMap(_ => add(mid)) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case (newRangeFromValue, _, oldRangeFromValue, _) if newRangeFromKey equiv oldRangeFromKey =>
                        //if fromValue is set in newRange, use it! else check if the fromValue is set in oldRange and replace it with newRange's rangeValue.
                        val fromValue = newRangeFromValue orElse (if (oldRangeFromValue.isDefined) Some(newRangeValue.rangeValue) else None)
                        if (newRangeValue.toKey >= oldRangeValue.toKey) { //new range completely overlaps old range.
                          val adjustedNewRange = Transient.Range(newRangeFromKey, newRangeValue.toKey, fromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          doMerge(dropNewKeyValue(stash = Some(adjustedNewRange)), dropOldKeyValue())
                        } else { // newRangeValue.toKey < oldRangeValue.toKey
                          val left = Transient.Range(newRangeFromKey, newRangeValue.toKey, fromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val right = Transient.Range(newRangeValue.toKey, oldRangeValue.toKey, None, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case (newRangeFromValue, _, oldRangeFromValue, _) if newRangeFromKey > oldRangeFromKey =>
                        //if fromValue is set in newRange, use it! else check if the fromValue is set in oldRange and replace it with newRange's rangeValue.
                        if (newRangeValue.toKey >= oldRangeValue.toKey) { //new range completely overlaps old range's right half.
                          val left = Transient.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val right = Transient.Range(newRangeFromKey, newRangeValue.toKey, newRangeFromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          add(left) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        } else { // newRangeValue.toKey < oldRangeValue.toKey
                          val left = Transient.Range(oldRangeFromKey, newRangeFromKey, oldRangeFromValue, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val mid = Transient.Range(newRangeFromKey, newRangeValue.toKey, newRangeFromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          val right = Transient.Range(newRangeValue.toKey, oldRangeValue.toKey, None, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                          add(left).flatMap(_ => add(mid)) match {
                            case Success(_) =>
                              doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                            case Failure(exception) =>
                              Failure(exception)
                          }
                        }

                      case (newRangeFromValue, _, oldRangeFromValue, _) if newRangeFromKey < oldRangeFromKey =>
                        //if fromValue is set in newRange, use it! else check if the fromValue is set in oldRange and replace it with newRange's rangeValue.
                        if (newRangeValue.toKey >= oldRangeValue.toKey) {
                          oldRangeFromValue match {
                            case Some(_) =>
                              val left = Transient.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val right = Transient.Range(oldRangeFromKey, newRangeValue.toKey, Some(newRangeValue.rangeValue), newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              add(left) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(stash = Some(right)), dropOldKeyValue())
                                case Failure(exception) =>
                                  Failure(exception)
                              }

                            case None =>
                              doMerge(newKeyValues, dropOldKeyValue())
                          }
                        } else { // newRangeValue.toKey < oldRangeValue.toKey
                          oldRangeFromValue match {
                            case Some(_) =>
                              val left = Transient.Range(newRangeFromKey, oldRangeFromKey, newRangeFromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val mid = Transient.Range(oldRangeFromKey, newRangeValue.toKey, Some(newRangeValue.rangeValue), newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val right = Transient.Range(newRangeValue.toKey, oldRangeValue.toKey, None, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              add(left).flatMap(_ => add(mid)) match {
                                case Success(_) =>
                                  doMerge(dropNewKeyValue(), dropOldKeyValue(stash = Some(right)))
                                case Failure(exception) =>
                                  Failure(exception)
                              }

                            case None =>
                              val left = Transient.Range(newRangeFromKey, newRangeValue.toKey, newRangeFromValue, newRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
                              val right = Transient.Range(newRangeValue.toKey, oldRangeValue.toKey, None, oldRangeValue.rangeValue, bloomFilterFalsePositiveRate, None)
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
              newKeyValues.drop(1).tryForeach(add) match {
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
              oldKeyValues.drop(1).tryForeach(add) match {
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
