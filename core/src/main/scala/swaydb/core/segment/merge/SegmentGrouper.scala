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
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Memory, Persistent, Value, _}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
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

  def addKeyValue(keyValueToAdd: KeyValue.ReadOnly,
                  splits: ListBuffer[SegmentBuffer],
                  minSegmentSize: Long,
                  forInMemory: Boolean,
                  isLastLevel: Boolean,
                  createdInLevel: Int,
                  valuesConfig: ValuesBlock.Config,
                  sortedIndexConfig: SortedIndexBlock.Config,
                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                  hashIndexConfig: HashIndexBlock.Config,
                  bloomFilterConfig: BloomFilterBlock.Config,
                  segmentIO: SegmentIO)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Unit] = {

    def doAdd(keyValueToAdd: Option[Transient] => Transient): IO[swaydb.Error.Segment, Unit] = {

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

              val nextKeyValueWithUpdatedStats: Transient = keyValueToAdd(currentGroupsLastKeyValues)

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

      if (addToCurrentSplit(force = false)) {
        IO.unit //add successful after force grouping!
      } else {
        //if still unable to add to current split after force grouping, start a new Segment!
        //And then do force add just in-case the new key-value is larger than the minimum segmentSize
        //because a Segment should contain at least one key-value.
        splits += SegmentBuffer()
        if (addToCurrentSplit(force = true))
          IO.unit
        else
          IO.failed(s"Failed to add key-value to new Segment split. minSegmentSize: $minSegmentSize, splits: ${splits.size}, lastSplit: ${splits.lastOption.map(_.size)}")
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
                        normaliseToSize = None,
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
                    normaliseToSize = None,
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
                    normaliseToSize = None,
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
                    normaliseToSize = None,
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
                        normaliseToSize = None,
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
                    normaliseToSize = None,
                    function = function,
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
                        normaliseToSize = None,
                        function = functionId,
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
                    normaliseToSize = None,
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
                        normaliseToSize = None,
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
              case IO.Right(fromValue) =>
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
              case IO.Left(error) =>
                IO.Left(error)
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
      }
    }
  }
}
