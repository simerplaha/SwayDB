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
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Memory, Persistent, Value, _}
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.segment.format.a.block._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * SegmentGroups will always group key-values with Groups at the head of key-value List. Groups cannot be randomly
 * added in the middle.
 */
private[merge] object SegmentGrouper extends LazyLogging {
  //Read key-values during merge are always cleared by GC after the merge therefore in-memory key-value
  //management of these key-values is not required.
  implicit val memorySweeper = Option.empty[MemorySweeper.KeyValue]

  /**
   * Mutates the input key-values by grouping them. Should not be accessed outside this class.
   *
   * @return returns the last group in the List if grouping was successful else None.
   */
  private[merge] def group(buffer: SegmentBuffer.Grouped,
                           createdInLevel: Int,
                           segmentValuesConfig: ValuesBlock.Config,
                           segmentSortedIndexConfig: SortedIndexBlock.Config,
                           segmentBinarySearchIndexConfig: BinarySearchIndexBlock.Config,
                           segmentHashIndexConfig: HashIndexBlock.Config,
                           segmentBloomFilterConfig: BloomFilterBlock.Config,
                           force: Boolean,
                           skipQuotaCheck: Boolean): IO[swaydb.Error.Segment, Unit] =
    if (skipQuotaCheck || buffer.shouldGroupKeyValues(force))
      Transient.Group(
        keyValues = buffer.unGrouped,
        previous = buffer.lastGroup,
        groupConfig = buffer.groupBy.groupConfig,
        createdInLevel = createdInLevel,
        valuesConfig = segmentValuesConfig,
        sortedIndexConfig = segmentSortedIndexConfig,
        binarySearchIndexConfig = segmentBinarySearchIndexConfig,
        hashIndexConfig = segmentHashIndexConfig,
        bloomFilterConfig = segmentBloomFilterConfig
      ) flatMap {
        newGroup =>
          buffer replaceGroupedKeyValues newGroup
          buffer.groupBy.groupByGroups map {
            groupByGroups =>
              if (buffer shouldGroupGroups groupByGroups)
                Transient.Group(
                  keyValues = buffer getGroupsToGroup groupByGroups,
                  previous = None,
                  groupConfig = groupByGroups.groupConfig,
                  createdInLevel = createdInLevel,
                  valuesConfig = segmentValuesConfig,
                  sortedIndexConfig = segmentSortedIndexConfig,
                  binarySearchIndexConfig = segmentBinarySearchIndexConfig,
                  hashIndexConfig = segmentHashIndexConfig,
                  bloomFilterConfig = segmentBloomFilterConfig
                ) map {
                  newGroup =>
                    buffer replaceGroupedGroups newGroup
                }
              else
                IO.unit
          } getOrElse IO.unit
      }
    else
      IO.unit

  @tailrec
  def addKeyValues(keyValues: MergeList[Memory.Range, KeyValue.ReadOnly],
                   splits: ListBuffer[SegmentBuffer],
                   minSegmentSize: Long,
                   forInMemory: Boolean,
                   isLastLevel: Boolean,
                   createdInLevel: Int,
                   segmentMergeConfigs: SegmentMergeConfigs,
                   segmentIO: SegmentIO)(implicit groupBy: Option[GroupByInternal.KeyValues],
                                         keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Unit] =
    keyValues.headOption match {
      case Some(keyValue) =>
        keyValue match {
          case keyValue: KeyValue.ReadOnly.Group =>
            implicit val groupIO = groupBy.map(_.groupIO) getOrElse segmentIO
            keyValue.segment.getAll() match {
              case IO.Success(groupKeyValues) =>
                addKeyValues(
                  keyValues = MergeList[Memory.Range, KeyValue.ReadOnly](groupKeyValues) append keyValues.dropHead(),
                  splits = splits,
                  minSegmentSize = minSegmentSize,
                  forInMemory = forInMemory,
                  isLastLevel = isLastLevel,
                  createdInLevel = createdInLevel,
                  segmentMergeConfigs = segmentMergeConfigs,
                  segmentIO = segmentIO
                )
              case IO.Failure(error) =>
                IO.Failure(error)
            }

          case keyValue =>
            addKeyValue(
              keyValueToAdd = keyValue,
              splits = splits,
              minSegmentSize = minSegmentSize,
              forInMemory = forInMemory,
              isLastLevel = isLastLevel,
              createdInLevel = createdInLevel,
              segmentMergeConfigs = segmentMergeConfigs,
              segmentIO = segmentIO
            ) match {
              case IO.Success(_) =>
                addKeyValues(
                  keyValues = keyValues.dropHead(),
                  splits = splits,
                  minSegmentSize = minSegmentSize,
                  forInMemory = forInMemory,
                  isLastLevel = isLastLevel,
                  createdInLevel = createdInLevel,
                  segmentMergeConfigs = segmentMergeConfigs,
                  segmentIO = segmentIO
                )
              case IO.Failure(error) =>
                IO.Failure(error)
            }
        }
      case None =>
        IO.unit
    }

  def addKeyValue(keyValueToAdd: KeyValue.ReadOnly,
                  splits: ListBuffer[SegmentBuffer],
                  minSegmentSize: Long,
                  forInMemory: Boolean,
                  isLastLevel: Boolean,
                  createdInLevel: Int,
                  segmentMergeConfigs: SegmentMergeConfigs,
                  segmentIO: SegmentIO)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Unit] = {

    implicit val groupBy: Option[GroupByInternal.KeyValues] =
      splits.head match {
        case _: SegmentBuffer.Flattened =>
          None

        case grouped: SegmentBuffer.Grouped =>
          Some(grouped.groupBy)
      }

    def doAdd(keyValueToAdd: Option[Transient.SegmentResponse] => Transient.SegmentResponse): IO[swaydb.Error.Segment, Unit] = {

      /**
       * Tries adding key-value to the current split/Segment. If force is true then the key-value will value added to
       * current split regardless of size limitation.
       *
       * @return Returns false if force is false and the key-value does not fit in the current Segment else true is returned on successful insert.
       */
      def addToCurrentSplit(force: Boolean): Boolean =
        splits.lastOption exists {
          lastBuffer =>
            if (lastBuffer.isReadyForGrouping) {
              false
            } else {
              val currentGroupsLastKeyValues = lastBuffer.lastNonGroupOption
              val currentSegmentSize =
                if (forInMemory)
                  currentGroupsLastKeyValues.map(_.stats.memorySegmentSize).getOrElse(0)
                else
                  currentGroupsLastKeyValues.map(_.stats.segmentSize).getOrElse(0)

              val nextKeyValueWithUpdatedStats: Transient.SegmentResponse = keyValueToAdd(currentGroupsLastKeyValues)

              val segmentSizeWithNextKeyValue =
                if (forInMemory)
                  currentSegmentSize + nextKeyValueWithUpdatedStats.stats.thisKeyValueMemorySize
                else
                  currentSegmentSize + nextKeyValueWithUpdatedStats.stats.thisKeyValuesSegmentKeyAndValueSize

              //if there are no key-values in the current Segment or if the current Segment size with new key-value fits, do add else return false.
              if (force || currentSegmentSize == 0 || segmentSizeWithNextKeyValue <= minSegmentSize) {
                splits.last add nextKeyValueWithUpdatedStats
                true
              } else {
                false
              }
            }
        }

      def tryGrouping(force: Boolean): IO[swaydb.Error.Segment, Unit] =
        splits.lastOption map {
          case _: SegmentBuffer.Flattened =>
            IO.unit

          case buffer: SegmentBuffer.Grouped =>
            if (buffer shouldGroupKeyValues force) {
              group(
                buffer = buffer,
                segmentValuesConfig = segmentMergeConfigs.segmentValuesConfig,
                createdInLevel = createdInLevel,
                segmentSortedIndexConfig = segmentMergeConfigs.segmentSortedIndexConfig,
                segmentBinarySearchIndexConfig = segmentMergeConfigs.segmentBinarySearchIndexConfig,
                segmentHashIndexConfig = segmentMergeConfigs.segmentHashIndexConfig,
                segmentBloomFilterConfig = segmentMergeConfigs.segmentBloomFilterConfig,
                force = force,
                skipQuotaCheck = true
              ) map (_ => ())
            }
            else
              IO.unit
        } getOrElse IO.unit



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
              splits += SegmentBuffer(groupBy)
              if (addToCurrentSplit(force = true))
                IO.unit
              else
                IO.failed(s"Failed to add key-value to new Segment split. minSegmentSize: $minSegmentSize, splits: ${splits.size}, lastSplit: ${splits.lastOption.map(_.size)}")
            }
        }
    }

    IO.Catch {
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
                    normaliseToSize = None,
                    value = value,
                    deadline = deadline,
                    time = time,
                    valuesConfig = segmentMergeConfigs.groupValuesConfig,
                    sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                    binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                    hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                    bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                        normaliseToSize = None,
                        value = value,
                        deadline = put.deadline,
                        time = put.time,
                        valuesConfig = segmentMergeConfigs.groupValuesConfig,
                        sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                        binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                        hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                        bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                    normaliseToSize = None,
                    deadline = remove.deadline,
                    time = remove.time,
                    valuesConfig = segmentMergeConfigs.groupValuesConfig,
                    sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                    binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                    hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                    bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                    normaliseToSize = None,
                    deadline = remove.deadline,
                    time = remove.time,
                    valuesConfig = segmentMergeConfigs.groupValuesConfig,
                    sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                    binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                    hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                    bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                    normaliseToSize = None,
                    value = value,
                    deadline = deadline,
                    time = time,
                    valuesConfig = segmentMergeConfigs.groupValuesConfig,
                    sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                    binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                    hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                    bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                        normaliseToSize = None,
                        value = value,
                        deadline = update.deadline,
                        time = update.time,
                        valuesConfig = segmentMergeConfigs.groupValuesConfig,
                        sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                        binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                        hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                        bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                    normaliseToSize = None,
                    function = function,
                    time = time,
                    valuesConfig = segmentMergeConfigs.groupValuesConfig,
                    sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                    binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                    hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                    bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                        normaliseToSize = None,
                        function = functionId,
                        time = function.time,
                        valuesConfig = segmentMergeConfigs.groupValuesConfig,
                        sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                        binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                        hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                        bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                    normaliseToSize = None,
                    applies = applies,
                    valuesConfig = segmentMergeConfigs.groupValuesConfig,
                    sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                    binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                    hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                    bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                        normaliseToSize = None,
                        applies = applies,
                        valuesConfig = segmentMergeConfigs.groupValuesConfig,
                        sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                        binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                        hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                        bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
                              normaliseToSize = None,
                              value = fromValue,
                              deadline = deadline,
                              time = time,
                              valuesConfig = segmentMergeConfigs.groupValuesConfig,
                              sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                              binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                              hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                              bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
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
              case IO.Failure(error) =>
                IO.Failure(error)
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
                    valuesConfig = segmentMergeConfigs.groupValuesConfig,
                    sortedIndexConfig = segmentMergeConfigs.groupSortedIndexConfig,
                    binarySearchIndexConfig = segmentMergeConfigs.groupBinarySearchIndexConfig,
                    hashIndexConfig = segmentMergeConfigs.groupHashIndexConfig,
                    bloomFilterConfig = segmentMergeConfigs.groupBloomFilterConfig,
                    _
                  )
                )
            }

        case group: KeyValue.ReadOnly.Group =>
          implicit val groupIO = groupBy.map(_.groupIO) getOrElse segmentIO
          group.segment.getAll() flatMap {
            keyValues =>
              addKeyValues(
                keyValues = MergeList(keyValues),
                splits = splits,
                minSegmentSize = minSegmentSize,
                forInMemory = forInMemory,
                isLastLevel = isLastLevel,
                createdInLevel = createdInLevel,
                //segment configs here because this a segment level adds.
                segmentMergeConfigs = segmentMergeConfigs,
                segmentIO = segmentIO
              )
          }
      }
    }
  }
}
