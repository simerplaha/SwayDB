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

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import swaydb.core.data.Transient.Group
import swaydb.core.data.{Memory, Persistent, Value, _}
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, GroupingStrategy, KeyValueGroupingStrategyInternal}
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.block.{BinarySearchIndex, BloomFilter, HashIndex, SortedIndex, Values}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.config.HashIndex.HashIndexMeter
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

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
        case size: GroupGroupingStrategyInternal.Size =>
          segmentKeyValues.last.stats.segmentSizeWithoutFooter >= size.size

        case count: GroupGroupingStrategyInternal.Count =>
          segmentKeyValues.last.stats.groupsCount >= count.count
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
        case size: KeyValueGroupingStrategyInternal.Size =>
          segmentKeyValues.last.stats.segmentSizeWithoutFooterForNextGroup >= size.size

        case count: KeyValueGroupingStrategyInternal.Count =>
          //use segmentKeyValues.last.stats.position instead of segmentKeyValues.size because position is pre-calculated.
          segmentKeyValues.last.stats.chainPosition - segmentKeyValues.last.stats.groupsCount >= count.count
      }

  private def groupsToGroup(keyValues: Iterable[KeyValue.WriteOnly],
                            groupingStrategy: GroupGroupingStrategyInternal,
                            force: Boolean): Option[Slice[KeyValue.WriteOnly]] =
    if (shouldGroupGroups(segmentKeyValues = keyValues, groupingStrategy = groupingStrategy, force = force)) {
      //use segmentKeyValues.last.stats.position instead of keyValues.size because position is pre-calculated.
      val keyValuesToGroup = Slice.create[KeyValue.WriteOnly](keyValues.last.stats.chainPosition)
      //do not need to recalculate stats since all key-values are being grouped.
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
    * @return IO.Success key-values to Group and the last Group. IO.Failure if the head of the List does not contain all the Group.
    */
  private def keyValuesToGroup(segmentKeyValues: Iterable[KeyValue.WriteOnly],
                               groupingStrategy: KeyValueGroupingStrategyInternal,
                               force: Boolean): IO[Option[(Slice[KeyValue.WriteOnly], Option[Transient.Group])]] =
    if (shouldGroupKeyValues(segmentKeyValues = segmentKeyValues, groupingStrategy = groupingStrategy, force = force)) {
      //create a new list of key-values with stats updated.
      val expectedGroupsKeyValueCount = segmentKeyValues.last.stats.chainPosition - segmentKeyValues.last.stats.groupsCount
      if (expectedGroupsKeyValueCount == 0)
        IO.none
      else {
        val keyValuesToGroup = Slice.create[KeyValue.WriteOnly](expectedGroupsKeyValueCount)
        segmentKeyValues.foldLeftIO((1, Option.empty[Transient.Group])) {
          case ((count, lastGroup), keyValue) =>
            keyValue match {
              case keyValue if count > segmentKeyValues.last.stats.groupsCount =>
                //this validation is not mandatory. It's expected that groupsCount is accurate and no other groups are following the head groups after the groupCount.
                //                if (keyValue.isGroup) {
                //                  val exception = new Exception(s"Post group key-value is a Group. ${keyValue.getClass.getSimpleName} found, expected a Fixed or Range.")
                //                  logger.error(exception.getMessage, exception)
                //                  IO.Failure(exception)
                //                } else {
                IO {
                  keyValuesToGroup add
                    keyValue.updatePrevious(
                      valuesConfig = groupingStrategy.valuesConfig,
                      sortedIndexConfig = groupingStrategy.sortedIndexConfig,
                      binarySearchIndexConfig = groupingStrategy.binarySearchIndexConfig,
                      hashIndexConfig = groupingStrategy.hashIndexConfig,
                      bloomFilterConfig = groupingStrategy.bloomFilterConfig,
                      previous = keyValuesToGroup.lastOption
                    )
                  (count + 1, lastGroup)
                }
              //                }
              case group: Transient.Group =>
                IO.Success((count + 1, Some(group)))
              case other =>
                val exception = new Exception(s"Head key-values are not Groups. ${other.getClass.getSimpleName} found, expected a Group.")
                logger.error(exception.getMessage, exception)
                IO.Failure(exception)
            }
        } flatMap {
          case (_, lastGroup) =>
            if (!keyValuesToGroup.isFull)
              IO.Failure(new Exception(s"keyValuesToGroup is not full! actual: ${keyValuesToGroup.written} - expected: ${keyValuesToGroup.size}"))
            else
              IO.Success(Some(keyValuesToGroup, lastGroup))
        }
      }
    } else {
      IO.none
    }

  private def createGroup(keyValuesToGroup: Slice[KeyValue.WriteOnly],
                          lastGroup: Option[Transient.Group],
                          segmentKeyValues: ListBuffer[KeyValue.WriteOnly],
                          groupingStrategy: GroupingStrategy): IO[Option[Group]] =
    Transient.Group(
      keyValues = keyValuesToGroup,
      previous = lastGroup,
      groupingStrategy = groupingStrategy
    ) map {
      newGroup =>
        newGroup map {
          newGroup =>
            if (lastGroup.isEmpty) { //if there was no last group, simply clear and add group.
              segmentKeyValues.clear()
            } else {
              //remove all key-values that were added to the new Group.
              val removeFromIndex = lastGroup.map(_.stats.chainPosition).getOrElse(0)
              segmentKeyValues.remove(removeFromIndex, newGroup.keyValues.last.stats.chainPosition)
            }
            segmentKeyValues += newGroup
            newGroup
        }
    }

  private[segment] def groupKeyValues(segmentKeyValues: ListBuffer[KeyValue.WriteOnly],
                                      groupingStrategy: KeyValueGroupingStrategyInternal,
                                      force: Boolean): IO[Option[Group]] =
    keyValuesToGroup(
      segmentKeyValues = segmentKeyValues,
      groupingStrategy = groupingStrategy,
      force = force
    ) flatMap {
      case Some((keyValuesToGroup, lastGroup)) =>
        createGroup(
          keyValuesToGroup = keyValuesToGroup,
          lastGroup = lastGroup,
          segmentKeyValues = segmentKeyValues,
          groupingStrategy = groupingStrategy
        )
      case None =>
        IO.none
    }

  private[segment] def groupGroups(groupKeyValues: ListBuffer[KeyValue.WriteOnly],
                                   groupingStrategy: GroupGroupingStrategyInternal,
                                   force: Boolean): IO[Option[Group]] =
    groupsToGroup(
      keyValues = groupKeyValues,
      groupingStrategy = groupingStrategy,
      force = force
    ) map {
      groupsToGroup =>
        createGroup(
          keyValuesToGroup = groupsToGroup,
          lastGroup = None,
          segmentKeyValues = groupKeyValues,
          groupingStrategy = groupingStrategy
        )
    } getOrElse IO.none

  /**
    * Mutates the input key-values by grouping them. Should not be accessed outside this class.
    *
    * @return returns the last group in the List if grouping was successful else None.
    */
  private[segment] def group(segmentKeyValues: ListBuffer[KeyValue.WriteOnly],
                             groupingStrategy: KeyValueGroupingStrategyInternal,
                             force: Boolean): IO[Option[Group]] =
    for {
      keyValuesGroup <- {
        groupKeyValues(
          segmentKeyValues = segmentKeyValues,
          groupingStrategy = groupingStrategy,
          force = force
        )
      }
      groupsGroup <- {
        if (keyValuesGroup.isDefined) //grouping of groups should only run if key-value grouping was successful.
          groupingStrategy.groupCompression map {
            groupingStrategy =>
              groupGroups(
                groupKeyValues = segmentKeyValues,
                groupingStrategy = groupingStrategy,
                force = force
              )
          } getOrElse IO.none
        else
          IO.none
      }
    } yield {
      groupsGroup orElse keyValuesGroup
    }

  @tailrec
  def addKeyValues(keyValues: MergeList[Memory.Range, KeyValue.ReadOnly],
                   splits: ListBuffer[ListBuffer[KeyValue.WriteOnly]],
                   minSegmentSize: Long,
                   forInMemory: Boolean,
                   isLastLevel: Boolean,
                   valuesConfig: Values.Config,
                   sortedIndexConfig: SortedIndex.Config,
                   binarySearchIndexConfig: BinarySearchIndex.Config,
                   hashIndexConfig: HashIndex.Config,
                   bloomFilterConfig: BloomFilter.Config)(implicit groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                          keyOrder: KeyOrder[Slice[Byte]]): IO[Unit] =
    keyValues.headOption match {
      case Some(keyValue) =>
        keyValue match {
          case keyValue: KeyValue.ReadOnly.Group =>
            keyValue.segment.getAll() match {
              case IO.Success(groupKeyValues) =>
                addKeyValues(
                  keyValues = MergeList[Memory.Range, KeyValue.ReadOnly](groupKeyValues) append keyValues.dropHead(),
                  splits = splits,
                  minSegmentSize = minSegmentSize,
                  forInMemory = forInMemory,
                  isLastLevel = isLastLevel,
                  valuesConfig = valuesConfig,
                  sortedIndexConfig = sortedIndexConfig,
                  binarySearchIndexConfig = binarySearchIndexConfig,
                  hashIndexConfig = hashIndexConfig,
                  bloomFilterConfig = bloomFilterConfig
                )
              case IO.Failure(exception) =>
                IO.Failure(exception)
            }

          case keyValue =>
            addKeyValue(
              keyValueToAdd = keyValue,
              splits = splits,
              minSegmentSize = minSegmentSize,
              forInMemory = forInMemory,
              isLastLevel = isLastLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig
            ) match {
              case IO.Success(_) =>
                addKeyValues(
                  keyValues = keyValues.dropHead(),
                  splits = splits,
                  minSegmentSize = minSegmentSize,
                  forInMemory = forInMemory,
                  isLastLevel = isLastLevel,
                  valuesConfig = valuesConfig,
                  sortedIndexConfig = sortedIndexConfig,
                  binarySearchIndexConfig = binarySearchIndexConfig,
                  hashIndexConfig = hashIndexConfig,
                  bloomFilterConfig = bloomFilterConfig
                )
              case IO.Failure(exception) =>
                IO.Failure(exception)
            }
        }
      case None =>
        IO.unit
    }

  def addKeyValue(keyValueToAdd: KeyValue.ReadOnly,
                  splits: ListBuffer[ListBuffer[KeyValue.WriteOnly]],
                  minSegmentSize: Long,
                  forInMemory: Boolean,
                  isLastLevel: Boolean,
                  valuesConfig: Values.Config,
                  sortedIndexConfig: SortedIndex.Config,
                  binarySearchIndexConfig: BinarySearchIndex.Config,
                  hashIndexConfig: HashIndex.Config,
                  bloomFilterConfig: BloomFilter.Config)(implicit groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                         keyOrder: KeyOrder[Slice[Byte]]): IO[Unit] = {

    def doAdd(keyValueToAdd: Option[KeyValue.WriteOnly] => KeyValue.WriteOnly): IO[Unit] = {

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
            currentSegmentSize + nextKeyValueWithUpdatedStats.stats.thisKeyValuesSegmentKeyAndValueSize

        //if there are no key-values in the current Segment or if the current Segment size with new key-value fits, do add else return false.
        if (force || currentSegmentSize == 0 || segmentSizeWithNextKeyValue <= minSegmentSize) {
          splits.last += nextKeyValueWithUpdatedStats
          true
        } else {
          false
        }
      }

      def tryGrouping(force: Boolean): IO[Unit] =
        groupingStrategy flatMap {
          groupingStrategy =>
            splits.lastOption map {
              last =>
                group(
                  segmentKeyValues = last,
                  groupingStrategy = groupingStrategy,
                  force = force
                ) map (_ => ())
            }
        } getOrElse IO.unit

      def startNewSegment(): Unit =
        splits += ListBuffer[KeyValue.WriteOnly]()

      //try adding to current split
      if (addToCurrentSplit(force = false))
        tryGrouping(force = false) //on successful add, try grouping if possible.
      else //if unable to add to current split, try force grouping and then try add again.
        tryGrouping(force = true) flatMap {
          _ =>
            if (addToCurrentSplit(force = false)) {
              IO.unit //add successful after force grouping!
            } else {
              //if still unable to add to current split after force grouping, start a new Segment!
              //And then do force add just in-case the new key-value is larger than the minimum segmentSize
              //because a Segment should contain at least one key-value.
              startNewSegment()
              if (addToCurrentSplit(force = true))
                IO.unit
              else
                IO.Failure(new Exception(s"Failed to add key-value to new Segment split. minSegmentSize: $minSegmentSize, splits: ${splits.size}, lastSplit: ${splits.lastOption.map(_.size)}"))
            }
        }
    }

    IO.CatchLeak {
      keyValueToAdd match {
        case fixed: KeyValue.ReadOnly.Fixed =>
          fixed match {
            case put @ Memory.Put(key, value, deadline, time) =>
              if (isLastLevel && !put.hasTimeLeft())
                IO.unit
              else
                doAdd(
                  Transient.Put(
                    key = key,
                    value = value,
                    deadline = deadline,
                    time = time,
                    valuesConfig = valuesConfig,
                    sortedIndexConfig = sortedIndexConfig,
                    binarySearchIndexConfig = binarySearchIndexConfig,
                    hashIndexConfig = hashIndexConfig,
                    bloomFilterConfig = bloomFilterConfig,
                    _
                  )
                )

            case put: Persistent.Put =>
              if (isLastLevel && !put.hasTimeLeft())
                IO.unit
              else
                put.getOrFetchValue flatMap {
                  value =>
                    doAdd(
                      Transient.Put(
                        key = put.key,
                        value = value,
                        deadline = put.deadline,
                        time = put.time,
                        valuesConfig = valuesConfig,
                        sortedIndexConfig = sortedIndexConfig,
                        binarySearchIndexConfig = binarySearchIndexConfig,
                        hashIndexConfig = hashIndexConfig,
                        bloomFilterConfig = bloomFilterConfig,
                        _
                      )
                    )
                }

            case remove: Memory.Remove =>
              if (isLastLevel)
                IO.unit
              else
                doAdd(
                  Transient.Remove(
                    key = keyValueToAdd.key,
                    deadline = remove.deadline,
                    time = remove.time,
                    valuesConfig = valuesConfig,
                    sortedIndexConfig = sortedIndexConfig,
                    binarySearchIndexConfig = binarySearchIndexConfig,
                    hashIndexConfig = hashIndexConfig,
                    bloomFilterConfig = bloomFilterConfig,
                    _
                  )
                )

            case remove: Persistent.Remove =>
              if (isLastLevel)
                IO.unit
              else
                doAdd(
                  Transient.Remove(
                    key = keyValueToAdd.key,
                    deadline = remove.deadline,
                    time = remove.time,
                    valuesConfig = valuesConfig,
                    sortedIndexConfig = sortedIndexConfig,
                    binarySearchIndexConfig = binarySearchIndexConfig,
                    hashIndexConfig = hashIndexConfig,
                    bloomFilterConfig = bloomFilterConfig,
                    _
                  )
                )

            case Memory.Update(key, value, deadline, time) =>
              if (isLastLevel)
                IO.unit
              else
                doAdd(
                  Transient.Update(
                    key = key,
                    value = value,
                    deadline = deadline,
                    time = time,
                    valuesConfig = valuesConfig,
                    sortedIndexConfig = sortedIndexConfig,
                    binarySearchIndexConfig = binarySearchIndexConfig,
                    hashIndexConfig = hashIndexConfig,
                    bloomFilterConfig = bloomFilterConfig,
                    _
                  )
                )

            case update: Persistent.Update =>
              if (isLastLevel)
                IO.unit
              else
                update.getOrFetchValue flatMap {
                  value =>
                    doAdd(
                      Transient.Update(
                        key = update.key,
                        value = value,
                        deadline = update.deadline,
                        time = update.time,
                        valuesConfig = valuesConfig,
                        sortedIndexConfig = sortedIndexConfig,
                        binarySearchIndexConfig = binarySearchIndexConfig,
                        hashIndexConfig = hashIndexConfig,
                        bloomFilterConfig = bloomFilterConfig,
                        _
                      )
                    )
                }

            case Memory.Function(key, function, time) =>
              if (isLastLevel)
                IO.unit
              else
                doAdd(
                  Transient.Function(
                    key = key,
                    function = function,
                    deadline = None,
                    time = time,
                    valuesConfig = valuesConfig,
                    sortedIndexConfig = sortedIndexConfig,
                    binarySearchIndexConfig = binarySearchIndexConfig,
                    hashIndexConfig = hashIndexConfig,
                    bloomFilterConfig = bloomFilterConfig,
                    _
                  )
                )

            case function: Persistent.Function =>
              if (isLastLevel)
                IO.unit
              else
                function.getOrFetchFunction flatMap {
                  functionId =>
                    doAdd(
                      Transient.Function(
                        key = function.key,
                        function = functionId,
                        deadline = None,
                        time = function.time,
                        valuesConfig = valuesConfig,
                        sortedIndexConfig = sortedIndexConfig,
                        binarySearchIndexConfig = binarySearchIndexConfig,
                        hashIndexConfig = hashIndexConfig,
                        bloomFilterConfig = bloomFilterConfig,
                        _
                      )
                    )
                }

            case Memory.PendingApply(key, applies) =>
              if (isLastLevel)
                IO.unit
              else
                doAdd(
                  Transient.PendingApply(
                    key = key,
                    applies = applies,
                    valuesConfig = valuesConfig,
                    sortedIndexConfig = sortedIndexConfig,
                    binarySearchIndexConfig = binarySearchIndexConfig,
                    hashIndexConfig = hashIndexConfig,
                    bloomFilterConfig = bloomFilterConfig,
                    _
                  )
                )

            case pendingApply: Persistent.PendingApply =>
              if (isLastLevel)
                IO.unit
              else
                pendingApply.getOrFetchApplies flatMap {
                  applies =>
                    doAdd(
                      Transient.PendingApply(
                        key = pendingApply.key,
                        applies = applies,
                        valuesConfig = valuesConfig,
                        sortedIndexConfig = sortedIndexConfig,
                        binarySearchIndexConfig = binarySearchIndexConfig,
                        hashIndexConfig = hashIndexConfig,
                        bloomFilterConfig = bloomFilterConfig,
                        _
                      )
                    )
                }
          }

        case range: KeyValue.ReadOnly.Range =>
          if (isLastLevel)
            range.fetchFromValue match {
              case IO.Success(fromValue) =>
                fromValue match {
                  case Some(fromValue) =>
                    fromValue match {
                      case put @ Value.Put(fromValue, deadline, time) =>
                        if (put.hasTimeLeft())
                          doAdd(
                            Transient.Put(
                              key = range.fromKey,
                              value = fromValue,
                              deadline = deadline,
                              time = time,
                              valuesConfig = valuesConfig,
                              sortedIndexConfig = sortedIndexConfig,
                              binarySearchIndexConfig = binarySearchIndexConfig,
                              hashIndexConfig = hashIndexConfig,
                              bloomFilterConfig = bloomFilterConfig,
                              _
                            )
                          )
                        else
                          IO.unit

                      case _: Value.Remove | _: Value.Update | _: Value.Function | _: Value.PendingApply =>
                        IO.unit
                    }
                  case None =>
                    IO.unit
                }
              case IO.Failure(exception) =>
                IO.Failure(exception)
            }
          else
            range.fetchFromAndRangeValue flatMap {
              case (fromValue, rangeValue) =>
                doAdd(
                  Transient.Range(
                    fromKey = range.fromKey,
                    toKey = range.toKey,
                    fromValue = fromValue,
                    rangeValue = rangeValue,
                    valuesConfig = valuesConfig,
                    sortedIndexConfig = sortedIndexConfig,
                    binarySearchIndexConfig = binarySearchIndexConfig,
                    hashIndexConfig = hashIndexConfig,
                    bloomFilterConfig = bloomFilterConfig,
                    _
                  )
                )
            }

        case group: KeyValue.ReadOnly.Group =>
          group.segment.getAll() flatMap {
            keyValues =>
              addKeyValues(
                keyValues = MergeList(keyValues),
                splits = splits,
                minSegmentSize = minSegmentSize,
                forInMemory = forInMemory,
                isLastLevel = isLastLevel,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig
              )
          }
      }
    }
  }
}
