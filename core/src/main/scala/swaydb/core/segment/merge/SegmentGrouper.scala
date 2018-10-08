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

package swaydb.core.segment.merge

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Transient.Group
import swaydb.core.data.{Memory, Persistent, Value, _}
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, GroupingStrategy, KeyValueGroupingStrategyInternal}
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.util.{Benchmark, TryUtil}
import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * SegmentGroups will always group key-values with Groups at the head of key-value List. Groups cannot be randomly
  * added in the middle.
  */
private[merge] object SegmentGrouper extends LazyLogging {
  //Read key-values during merge are always cleared by GC after the merge therefore in-memory key-value
  //management of these key-values is not required.
  implicit val keyValueLimiter = KeyValueLimiter.none

  private def shouldGroupGroups(segmentKeyValues: Iterable[KeyValue.WriteOnly],
                                groupingStrategy: GroupGroupingStrategyInternal,
                                force: Boolean): Boolean =
    if (segmentKeyValues.isEmpty || segmentKeyValues.last.stats.groupsCount <= 1)
      false
    else if (force)
      true
    else
      groupingStrategy match {
        case GroupGroupingStrategyInternal.Size(size, _, _) =>
          segmentKeyValues.last.stats.segmentSizeWithoutFooter >= size

        case GroupGroupingStrategyInternal.Count(count, _, _) =>
          segmentKeyValues.last.stats.groupsCount >= count
      }

  private def shouldGroupKeyValues(segmentKeyValues: Iterable[KeyValue.WriteOnly],
                                   groupingStrategy: KeyValueGroupingStrategyInternal,
                                   force: Boolean): Boolean =
    if (segmentKeyValues.isEmpty)
      false
    else if (force)
      true
    else
      groupingStrategy match {
        case KeyValueGroupingStrategyInternal.Size(size, _, _, _) =>
          segmentKeyValues.last.stats.segmentSizeWithoutFooterForNextGroup >= size

        case KeyValueGroupingStrategyInternal.Count(count, _, _, _) =>
          //use segmentKeyValues.last.stats.position instead of segmentKeyValues.size because position is pre-calculated.
          segmentKeyValues.last.stats.position - segmentKeyValues.last.stats.groupsCount >= count
      }

  private def groupsToGroup(keyValues: Iterable[KeyValue.WriteOnly],
                            bloomFilterFalsePositiveRate: Double,
                            groupingStrategy: GroupGroupingStrategyInternal,
                            force: Boolean): Option[Slice[KeyValue.WriteOnly]] =
    if (shouldGroupGroups(segmentKeyValues = keyValues, groupingStrategy = groupingStrategy, force = force)) {
      //use segmentKeyValues.last.stats.position instead of keyValues.size because position is pre-calculated.
      val keyValuesToGroup = Slice.create[KeyValue.WriteOnly](keyValues.last.stats.position)
      //do not need to recalculate stats since all key-values are being groups.
      //      keyValues foreach (keyValuesToGroup add _.updateStats(bloomFilterFalsePositiveRate, keyValuesToGroup.lastOption))
      keyValues foreach (keyValuesToGroup add _)
      Some(keyValuesToGroup)
    } else {
      None
    }

  /**
    * All groups should be in the head of the List. If the key-value list contains a random key-value in between
    * the groups which is not a Group, this functions will return failure.
    *
    * @return Success key-values to Group and the last Group. Failure if the head of the List does not contain all the Group.
    */
  private def keyValuesToGroup(segmentKeyValues: Iterable[KeyValue.WriteOnly],
                               bloomFilterFalsePositiveRate: Double,
                               groupingStrategy: KeyValueGroupingStrategyInternal,
                               force: Boolean): Try[Option[(Iterable[KeyValue.WriteOnly], Option[Transient.Group])]] =
    if (shouldGroupKeyValues(segmentKeyValues = segmentKeyValues, groupingStrategy = groupingStrategy, force = force)) {
      //create a new list of key-values with stats updated.
      val expectedGroupsKeyValueCount = segmentKeyValues.last.stats.position - segmentKeyValues.last.stats.groupsCount
      if (expectedGroupsKeyValueCount == 0)
        TryUtil.successNone
      else {
        val keyValuesToGroup = Slice.create[KeyValue.WriteOnly](expectedGroupsKeyValueCount)
        segmentKeyValues.tryFoldLeft((1, Option.empty[Transient.Group])) {
          case ((count, lastGroup), keyValue) =>
            keyValue match {
              case keyValue if count > segmentKeyValues.last.stats.groupsCount =>
                //this validation is not mandatory. It's expected that groupsCount is accurate and no other groups are following the head groups after the groupCount.
                //                if (keyValue.isGroup) {
                //                  val exception = new Exception(s"Post group key-value is a Group. ${keyValue.getClass.getSimpleName} found, expected a Fixed or Range.")
                //                  logger.error(exception.getMessage, exception)
                //                  Failure(exception)
                //                } else {
                Try {
                  keyValuesToGroup add keyValue.updateStats(bloomFilterFalsePositiveRate, keyValuesToGroup.lastOption)
                  (count + 1, lastGroup)
                }
              //                }
              case group: Transient.Group =>
                Success((count + 1, Some(group)))
              case other =>
                val exception = new Exception(s"Head key-values are not Groups. ${other.getClass.getSimpleName} found, expected a Group.")
                logger.error(exception.getMessage, exception)
                Failure(exception)
            }
        } flatMap {
          case (_, lastGroup) =>
            if (!keyValuesToGroup.isFull)
              Failure(new Exception(s"keyValuesToGroup is not full! actual: ${keyValuesToGroup.written} - expected: ${keyValuesToGroup.size}"))
            else
              Success(Some(keyValuesToGroup, lastGroup))
        }
      }
    } else {
      TryUtil.successNone
    }

  private def createGroup(keyValuesToGroup: Iterable[KeyValue.WriteOnly],
                          lastGroup: Option[Transient.Group],
                          segmentKeyValues: ListBuffer[KeyValue.WriteOnly],
                          bloomFilterFalsePositiveRate: Double,
                          groupingStrategy: GroupingStrategy): Try[Option[Group]] =
    Group(
      keyValues = keyValuesToGroup,
      indexCompressions = groupingStrategy.indexCompressions,
      valueCompressions = groupingStrategy.valueCompressions,
      falsePositiveRate = bloomFilterFalsePositiveRate,
      previous = lastGroup
    ) map {
      newGroup =>
        newGroup map {
          newGroup =>
            if (lastGroup.isEmpty) { //if there was no last group, simply clear and add group.
              segmentKeyValues.clear()
            } else {
              //remove all key-values that were added to the new Group.
              val removeFromIndex = lastGroup.map(_.stats.position).getOrElse(0)
              segmentKeyValues.remove(removeFromIndex, newGroup.keyValues.last.stats.position)
            }
            segmentKeyValues += newGroup
            newGroup
        }
    }

  private[segment] def groupKeyValues(segmentKeyValues: ListBuffer[KeyValue.WriteOnly],
                                      bloomFilterFalsePositiveRate: Double,
                                      groupingStrategy: KeyValueGroupingStrategyInternal,
                                      force: Boolean): Try[Option[Group]] =
    keyValuesToGroup(
      segmentKeyValues = segmentKeyValues,
      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
      groupingStrategy = groupingStrategy,
      force = force
    ) flatMap {
      case Some((keyValuesToGroup, lastGroup)) =>
        createGroup(
          keyValuesToGroup = keyValuesToGroup,
          lastGroup = lastGroup,
          segmentKeyValues = segmentKeyValues,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          groupingStrategy = groupingStrategy
        )
      case None =>
        TryUtil.successNone
    }

  private[segment] def groupGroups(groupKeyValues: ListBuffer[KeyValue.WriteOnly],
                                   bloomFilterFalsePositiveRate: Double,
                                   groupingStrategy: GroupGroupingStrategyInternal,
                                   force: Boolean): Try[Option[Group]] =
    groupsToGroup(
      keyValues = groupKeyValues,
      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
      groupingStrategy = groupingStrategy,
      force = force
    ) map {
      groupsToGroup =>
        createGroup(
          keyValuesToGroup = groupsToGroup,
          lastGroup = None,
          segmentKeyValues = groupKeyValues,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          groupingStrategy = groupingStrategy
        )
    } getOrElse TryUtil.successNone

  /**
    * Mutates the input key-values by grouping them. Should not be accessed outside this class.
    *
    * @return returns the last group in the List if grouping was successful else None.
    */
  private[segment] def group(segmentKeyValues: ListBuffer[KeyValue.WriteOnly],
                             bloomFilterFalsePositiveRate: Double,
                             groupingStrategy: KeyValueGroupingStrategyInternal,
                             force: Boolean): Try[Option[Group]] =
    for {
      keyValuesGroup <-
        groupKeyValues(
          segmentKeyValues = segmentKeyValues,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          groupingStrategy = groupingStrategy,
          force = force
        )
      groupsGroup <-
        if (keyValuesGroup.isDefined) //grouping of groups should only run if key-value grouping was successful.
          groupingStrategy.groupCompression map {
            groupingStrategy =>
              groupGroups(
                groupKeyValues = segmentKeyValues,
                bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                groupingStrategy = groupingStrategy,
                force = force
              )
          } getOrElse TryUtil.successNone
        else
          TryUtil.successNone

    } yield {
      groupsGroup orElse keyValuesGroup
    }

  @tailrec
  def addKeyValues(keyValues: MergeList,
                   splits: ListBuffer[ListBuffer[KeyValue.WriteOnly]],
                   minSegmentSize: Long,
                   forInMemory: Boolean,
                   isLastLevel: Boolean,
                   bloomFilterFalsePositiveRate: Double,
                   compressDuplicateValues: Boolean)(implicit groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                     ordering: Ordering[Slice[Byte]]): Try[Unit] =
    keyValues.headOption match {
      case Some(keyValue) =>
        keyValue match {
          case keyValue: KeyValue.ReadOnly.Group =>
            keyValue.segmentCache.getAll() match {
              case Success(groupKeyValues) =>
                addKeyValues(
                  keyValues = MergeList(groupKeyValues) append keyValues.dropHead(),
                  splits = splits,
                  minSegmentSize = minSegmentSize,
                  forInMemory = forInMemory,
                  isLastLevel = isLastLevel,
                  bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                  compressDuplicateValues = compressDuplicateValues
                )
              case Failure(exception) =>
                Failure(exception)
            }

          case keyValue =>
            addKeyValue(
              keyValueToAdd = keyValue,
              splits = splits,
              minSegmentSize = minSegmentSize,
              forInMemory = forInMemory,
              isLastLevel = isLastLevel,
              bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
              compressDuplicateValues = compressDuplicateValues
            ) match {
              case Success(_) =>
                addKeyValues(
                  keyValues = keyValues.dropHead(),
                  splits = splits,
                  minSegmentSize = minSegmentSize,
                  forInMemory = forInMemory,
                  isLastLevel = isLastLevel,
                  bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                  compressDuplicateValues = compressDuplicateValues
                )
              case Failure(exception) =>
                Failure(exception)
            }

        }
      case None =>
        TryUtil.successUnit
    }

  def addKeyValue(keyValueToAdd: KeyValue.ReadOnly,
                  splits: ListBuffer[ListBuffer[KeyValue.WriteOnly]],
                  minSegmentSize: Long,
                  forInMemory: Boolean,
                  isLastLevel: Boolean,
                  bloomFilterFalsePositiveRate: Double,
                  compressDuplicateValues: Boolean)(implicit groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                    ordering: Ordering[Slice[Byte]]): Try[Unit] = {

    def doAdd(keyValueToAdd: Option[KeyValue.WriteOnly] => KeyValue.WriteOnly): Try[Unit] = {

      /**
        * Tries adding key-value to the current split/Segment. If force is true then the key-value will get added to
        * current split regardless of size limitation.
        *
        * @return Returns false if force is false and the key-value does not fit in the current Segment else true is returned on successful insert.
        */
      def addToCurrentSplit(force: Boolean): Boolean = {
        val currentSplitsLastKeyValue = splits.lastOption.flatMap(_.lastOption)

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

        //if there are no key-values in the current Segment or if the current Segment size with new key-value fits, do add else return false.
        if (force || currentSegmentSize == 0 || segmentSizeWithNextKeyValue <= minSegmentSize) {
          splits.last += nextKeyValueWithUpdatedStats
          true
        } else {
          false
        }
      }

      def tryGrouping(force: Boolean): Try[Unit] =
        groupingStrategy flatMap {
          groupingStrategy =>
            splits.lastOption map {
              last =>
                group(
                  segmentKeyValues = last,
                  bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                  groupingStrategy = groupingStrategy,
                  force = force
                ) map (_ => ())
            }
        } getOrElse TryUtil.successUnit

      def startNewSegment(): Unit =
        splits += ListBuffer[KeyValue.WriteOnly]()

      //try adding to current split
      if (addToCurrentSplit(force = false))
        tryGrouping(force = false) //on successful add, try grouping if possible.
      else //if unable to add to current split, try force grouping and then try add again.
        tryGrouping(force = true) flatMap {
          _ =>
            if (addToCurrentSplit(force = false)) {
              TryUtil.successUnit //add successful after force grouping!
            } else {
              //if still unable to add to current split after force grouping, start a new Segment!
              //And then do force add just in-case the new key-value is larger than the minimum segmentSize
              //because a Segment should contain at least one key-value.
              startNewSegment()
              if (addToCurrentSplit(force = true))
                TryUtil.successUnit
              else
                Failure(new Exception(s"Failed to add key-value to new Segment split. minSegmentSize: $minSegmentSize, splits: ${splits.size}, lastSplit: ${splits.lastOption.map(_.size)}"))
            }
        }
    }

    try {
      keyValueToAdd match {
        case fixed: KeyValue.ReadOnly.Fixed =>
          if (isLastLevel && fixed.isOverdue())
            TryUtil.successUnit
          else
            fixed match {
              case Memory.Put(key, value, deadline) =>
                doAdd(Transient.Put(key, value, bloomFilterFalsePositiveRate, _, deadline, compressDuplicateValues))

              case put: Persistent.Put =>
                put.getOrFetchValue flatMap {
                  value =>
                    doAdd(Transient.Put(put.key, value, bloomFilterFalsePositiveRate, _, put.deadline, compressDuplicateValues))
                }

              case remove @ (_: Memory.Remove | _: Persistent.Remove) =>
                if (!isLastLevel)
                  doAdd(Transient.Remove(keyValueToAdd.key, bloomFilterFalsePositiveRate, _, remove.deadline))
                else
                  TryUtil.successUnit

              case Memory.Update(key, value, deadline) =>
                if (!isLastLevel)
                  doAdd(Transient.Update(key, value, bloomFilterFalsePositiveRate, _, deadline, compressDuplicateValues))
                else
                  TryUtil.successUnit

              case update: Persistent.Update =>
                if (!isLastLevel)
                  update.getOrFetchValue flatMap {
                    value =>
                      doAdd(Transient.Update(update.key, value, bloomFilterFalsePositiveRate, _, update.deadline, compressDuplicateValues))
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
                        if (put.hasTimeLeft())
                          doAdd(Transient.Put(range.fromKey, fromValue, bloomFilterFalsePositiveRate, _, deadline, compressDuplicateValues))
                        else
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
            range.fetchFromAndRangeValue flatMap {
              case (fromValue, rangeValue) =>
                doAdd(Transient.Range(range.fromKey, range.toKey, fromValue, rangeValue, bloomFilterFalsePositiveRate, _))
            }

        case group: KeyValue.ReadOnly.Group =>
          group.segmentCache.getAll() flatMap {
            keyValues =>
              addKeyValues(
                keyValues = MergeList(keyValues),
                splits = splits,
                minSegmentSize = minSegmentSize,
                forInMemory = forInMemory,
                isLastLevel = isLastLevel,
                bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                compressDuplicateValues = compressDuplicateValues
              )
          }
      }
    } catch {
      case ex: Exception =>
        println(s"debug: $keyValueToAdd")
        throw ex
    }
  }
}
