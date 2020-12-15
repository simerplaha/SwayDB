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

package swaydb.core.segment.ref

import com.typesafe.scalalogging.LazyLogging
import swaydb.Aggregator
import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.merge.{KeyValueGrouper, KeyValueMerger, MergeStats}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.{PersistentSegmentMany, PersistentSegmentOne, Segment}
import swaydb.core.util.IDGenerator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

private[segment] object SegmentRefWriter extends LazyLogging {

  def mergeRef(ref: SegmentRef,
               headGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
               tailGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
               mergeableCount: Int,
               mergeable: Iterator[Assignable],
               removeDeletes: Boolean,
               createdInLevel: Int,
               valuesConfig: ValuesBlock.Config,
               sortedIndexConfig: SortedIndexBlock.Config,
               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
               hashIndexConfig: HashIndexBlock.Config,
               bloomFilterConfig: BloomFilterBlock.Config,
               segmentConfig: SegmentBlock.Config)(implicit executionContext: ExecutionContext,
                                                   keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   functionStore: FunctionStore): Future[SegmentMergeResult[Slice[TransientSegment.Persistent]]] =
    defragGaps(
      gap = headGap,
      resultBuffer = ListBuffer.empty[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]],
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    ) flatMap {
      resultBuffer =>
        defragMerge(
          ref = ref,
          mergeableCount = mergeableCount,
          mergeable = mergeable,
          removeDeletes = removeDeletes,
          resultBuffer = resultBuffer,
          //forceExpand if the ref is too small and there are gaps.
          forceExpand = (removeDeletes && ref.hasNonPut) || (ref.segmentSize < segmentConfig.minSize && (headGap.nonEmpty || tailGap.nonEmpty))
        ) map {
          replaced =>
            (resultBuffer, replaced)
        }
    } flatMap {
      case (resultBuffer, replaced) =>
        defragGaps(
          gap = tailGap,
          resultBuffer = resultBuffer,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        ) map {
          buffer =>
            (buffer, replaced)
        }
    } flatMap {
      case (resultBuffer, replaced) =>
        Future {
          val newBuffer =
            defragLast(
              sortedIndexConfig = sortedIndexConfig,
              segmentConfig = segmentConfig,
              removeDeletes = removeDeletes,
              resultBuffer = resultBuffer
            )

          (newBuffer, replaced)
        }
    } flatMap {
      case (resultBuffer, replaced) =>
        writeTransient(
          resultBuffer = resultBuffer,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        ) map {
          transientSegments =>
            SegmentMergeResult(
              result = transientSegments,
              replaced = replaced
            )
        }
    }

  def mergeMultiRef(headGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
                    tailGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
                    segmentRefs: Iterator[SegmentRef],
                    assignableCount: Int,
                    assignables: Iterator[Assignable],
                    removeDeletes: Boolean,
                    createdInLevel: Int,
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator,
                                                        executionContext: ExecutionContext,
                                                        keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore): Future[SegmentMergeResult[Slice[TransientSegment.Persistent]]] = {
    //    if (assignableCount == 0) {
    //      mergeNoMid(
    //        headGap = headGap,
    //        tailGap = tailGap,
    //        segmentRefs = segmentRefs,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      )
    //    } else {
    //      val segments = ListBuffer.empty[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]]
    //
    //      defragGaps(
    //        gap = headGap,
    //        segments = segments,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      ) flatMapUnit {
    //        val (assignmentRefs, untouchedRefs) = segmentRefs.duplicate
    //
    //        //assign key-values to Segment and then perform merge.
    //        val assignments =
    //          SegmentAssigner.assignUnsafeGapsSegmentRef[ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]]](
    //            assignablesCount = assignableCount,
    //            assignables = assignables,
    //            segments = assignmentRefs
    //          )
    //
    //        if (assignments.isEmpty) {
    //          val exception = swaydb.Exception.MergeKeyValuesWithoutTargetSegment(assignableCount)
    //          val error = "Assigned segments are empty."
    //          logger.error(error, exception)
    //          Future.failed(exception)
    //        } else {
    //          //keep oldRefs that are not assign and make sure they are added in order.
    //
    //          def nextOldOrNull() = if (untouchedRefs.hasNext) untouchedRefs.next() else null
    //
    //          val singles = ListBuffer.empty[TransientSegment.Persistent]
    //
    //          val assignmentsResult: Future[ListBuffer[Any]] =
    //            Future.traverse(assignments) {
    //              assignment =>
    //
    //                var oldRef: SegmentRef = nextOldOrNull()
    //
    //                //insert SegmentRefs directly that are not assigned or do not require merging making
    //                //sure they are inserted in order.
    //                while (oldRef != null && oldRef != assignment.segment) {
    //                  singles += TransientSegment.RemoteRef(fileHeader = PersistentSegmentOne.formatIdSlice, ref = oldRef)
    //
    //                  oldRef = nextOldOrNull()
    //                }
    //
    //                SegmentRefWriter.mergeRef(
    //                  ref = assignment.segment,
    //                  headGap = assignment.headGap.result,
    //                  tailGap = assignment.tailGap.result,
    //                  mergeableCount = assignment.midOverlap.size,
    //                  mergeable = assignment.midOverlap.iterator,
    //                  removeDeletes = removeDeletes,
    //                  createdInLevel = createdInLevel,
    //                  valuesConfig = valuesConfig,
    //                  sortedIndexConfig = sortedIndexConfig,
    //                  binarySearchIndexConfig = binarySearchIndexConfig,
    //                  hashIndexConfig = hashIndexConfig,
    //                  bloomFilterConfig = bloomFilterConfig,
    //                  segmentConfig = segmentConfig
    //                ) map {
    //                  result =>
    //                    if (result.replaced) {
    //                      singles ++= result.result
    //                    } else {
    //                      //                      val merge = result.result :+ TransientSegment.RemoteRef(fileHeader = PersistentSegmentOne.formatIdSlice, ref = assignment.segment)
    //                      //
    //                      //                      singles ++= merge.flatten.sortBy(_.minKey)(keyOrder)
    //                      ???
    //                    }
    //                }
    //            }
    //
    //          assignmentsResult map {
    //            _ =>
    //              untouchedRefs foreach {
    //                oldRef =>
    //                  singles += TransientSegment.RemoteRef(fileHeader = PersistentSegmentOne.formatIdSlice, ref = oldRef)
    //              }
    //          } flatMapUnit {
    //            defragGaps(
    //              gap = tailGap,
    //              segments = segments,
    //              removeDeletes = removeDeletes,
    //              createdInLevel = createdInLevel,
    //              valuesConfig = valuesConfig,
    //              sortedIndexConfig = sortedIndexConfig,
    //              binarySearchIndexConfig = binarySearchIndexConfig,
    //              hashIndexConfig = hashIndexConfig,
    //              bloomFilterConfig = bloomFilterConfig,
    //              segmentConfig = segmentConfig
    //            )
    //          } flatMapUnit {
    //            Future {
    //              defragLast(
    //                sortedIndexConfig = sortedIndexConfig,
    //                segmentConfig = segmentConfig,
    //                removeDeletes = removeDeletes,
    //                segments = segments
    //              )
    //            }
    //          } flatMapUnit {
    //            writeTransient(
    //              segments = segments,
    //              removeDeletes = removeDeletes,
    //              createdInLevel = createdInLevel,
    //              valuesConfig = valuesConfig,
    //              sortedIndexConfig = sortedIndexConfig,
    //              binarySearchIndexConfig = binarySearchIndexConfig,
    //              hashIndexConfig = hashIndexConfig,
    //              bloomFilterConfig = bloomFilterConfig,
    //              segmentConfig = segmentConfig
    //            ) map {
    //              transientSegments =>
    //                SegmentMergeResult(result = transientSegments, replaced = true)
    //            }
    //          }
    //        }
    //      }
    //    }
    ???
  }


  def refresh(ref: SegmentRef,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment.OneOrRemoteRefOrMany] = {
    //    val footer = ref.getFooter()
    val iterator = ref.iterator()
    //if it's created in the same level the required spaces for sortedIndex and values
    //will be the same as existing or less than the current sizes so there is no need to create a
    //MergeState builder.

    //NOTE - IGNORE created in same Level as configurations can change on boot-up.
    //    if (footer.createdInLevel == createdInLevel)
    //      Segment.refreshForSameLevel(
    //        sortedIndexBlock = ref.segmentBlockCache.getSortedIndex(),
    //        valuesBlock = ref.segmentBlockCache.getValues(),
    //        iterator = iterator,
    //        keyValuesCount = footer.keyValueCount,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      )
    //    else
    Segment.refreshForNewLevel(
      keyValues = iterator,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    )
  }

  /** **************************************************
   * ***************************************************
   * ********************* PRIVATE *********************
   * ***************************************************
   * ************************************************* */

  private[ref] def isStatsSmall(stats: MergeStats.Persistent.Builder[Memory, ListBuffer],
                                sortedIndexConfig: SortedIndexBlock.Config,
                                segmentConfig: SegmentBlock.Config): Boolean = {
    val mergeStats =
      stats.close(
        hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
      )

    mergeStats.keyValuesCount < segmentConfig.maxCount && mergeStats.maxSortedIndexSize + stats.totalValuesSize < segmentConfig.minSize / 2
  }

  private[ref] def createMergeStats(removeDeletes: Boolean): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if (removeDeletes)
      MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.addLastLevel)
    else
      MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

  private[ref] def defragGaps(gap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
                              resultBuffer: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]],
                              removeDeletes: Boolean,
                              createdInLevel: Int,
                              valuesConfig: ValuesBlock.Config,
                              sortedIndexConfig: SortedIndexBlock.Config,
                              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                              hashIndexConfig: HashIndexBlock.Config,
                              bloomFilterConfig: BloomFilterBlock.Config,
                              segmentConfig: SegmentBlock.Config)(implicit ec: ExecutionContext): Future[ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]]] =
    if (gap.isEmpty)
      Future.successful(resultBuffer)
    else
      Future {
        @inline def addRemoteSegment(segment: Segment): Unit = {
          val remote =
            TransientSegment.RemoteSegment(
              segment = segment,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

          resultBuffer += Right(remote)
        }

        @inline def appendRemoteSegmentRef(ref: SegmentRef): Unit = {
          val remote =
            TransientSegment.RemoteRef(
              fileHeader = PersistentSegmentOne.formatIdSlice,
              ref = ref
            )

          resultBuffer += Right(remote)
        }

        @inline def lastMergeStatsOrNull(): MergeStats.Persistent.Builder[Memory, ListBuffer] =
          resultBuffer.lastOption match {
            case Some(Left(stats)) =>
              stats

            case Some(Right(_)) | None =>
              null
          }

        @inline def processSegment(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                                   segment: Segment) =
          if (statsOrNull == null) {
            //does matter if this Segment is small. Add it because there are currently no known opened stats.
            addRemoteSegment(segment)
            null
          } else if (segment.segmentSize < segmentConfig.minSize || isStatsSmall(statsOrNull, sortedIndexConfig, segmentConfig)) {
            segment match {
              case many: PersistentSegmentMany =>
                many.segmentRefsIterator().foldLeft(statsOrNull) {
                  case (statsOrNull, segmentRef) =>
                    processSegmentRef(
                      statsOrNull = statsOrNull,
                      segmentRef = segmentRef
                    )
                }

              case _ =>
                segment.iterator() foreach (keyValue => statsOrNull.add(keyValue.toMemory()))
                statsOrNull
            }

          } else {
            addRemoteSegment(segment)
            statsOrNull
          }

        @inline def processSegmentRef(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer],
                                      segmentRef: SegmentRef) =
          if (statsOrNull == null) {
            //does matter if this Segment is small. Add it because there are currently no known opened stats.
            appendRemoteSegmentRef(segmentRef)
            null
          } else if (segmentRef.getKeyValueCount() < segmentConfig.maxCount || isStatsSmall(statsOrNull, sortedIndexConfig, segmentConfig)) {
            segmentRef.iterator() foreach (keyValue => statsOrNull.add(keyValue.toMemory()))
            statsOrNull
          } else {
            appendRemoteSegmentRef(segmentRef)
            statsOrNull
          }

        gap.foldLeft(lastMergeStatsOrNull()) {
          case (statsOrNull, Right(segment: Segment)) =>
            processSegment(
              statsOrNull = statsOrNull,
              segment = segment
            )

          case (statsOrNull, Right(segmentRef: SegmentRef)) =>
            processSegmentRef(
              statsOrNull = statsOrNull,
              segmentRef = segmentRef
            )

          case (statsOrNull, Right(collection: Assignable.Collection)) =>
            val stats =
              if (statsOrNull == null) {
                val newStats = createMergeStats(removeDeletes = removeDeletes)
                resultBuffer += Left(newStats)
                newStats
              } else {
                statsOrNull
              }

            collection.iterator() foreach (keyValue => stats.add(keyValue.toMemory()))

            stats

          case (statsOrNull, Left(stats: MergeStats.Persistent.Builder[Memory, ListBuffer])) =>
            if (statsOrNull == null) {
              resultBuffer += Left(stats)
              stats
            } else {
              stats.keyValues foreach statsOrNull.add
              statsOrNull
            }
        }

        resultBuffer
      }

  private[ref] def defragMerge(ref: SegmentRef,
                               mergeableCount: Int,
                               mergeable: Iterator[Assignable],
                               removeDeletes: Boolean,
                               forceExpand: Boolean,
                               resultBuffer: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                             functionStore: FunctionStore,
                                                                                                                                             ec: ExecutionContext): Future[Boolean] = {
    @inline def doMerge(stats: MergeStats.Persistent.Builder[Memory, ListBuffer]): Unit =
      KeyValueMerger.merge(
        headGap = Assignable.emptyIterable,
        tailGap = Assignable.emptyIterable,
        mergeableCount = mergeableCount,
        mergeable = mergeable,
        oldKeyValuesCount = ref.getKeyValueCount(),
        oldKeyValues = ref.iterator(),
        stats = stats,
        isLastLevel = removeDeletes
      )

    if (mergeableCount > 0)
      Future {
        resultBuffer.lastOption match {
          case Some(Left(existingStats)) =>
            doMerge(existingStats)
            true

          case Some(Right(_)) | None =>
            val newStats = createMergeStats(removeDeletes = removeDeletes)
            doMerge(newStats)
            resultBuffer += Left(newStats)
            true
        }
      }
    else if (forceExpand)
      Future {
        resultBuffer.lastOption match {
          case Some(Left(lastStats)) =>
            ref.iterator() foreach (keyValue => lastStats.add(keyValue.toMemory()))
            true

          case Some(Right(_)) | None =>
            val newStats = createMergeStats(removeDeletes = removeDeletes)
            doMerge(newStats)
            resultBuffer += Left(newStats)
            true
        }
      }
    else
      Futures.`false`
  }

  private[ref] def defragLast(sortedIndexConfig: SortedIndexBlock.Config,
                              segmentConfig: SegmentBlock.Config,
                              removeDeletes: Boolean,
                              resultBuffer: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]])(implicit ec: ExecutionContext): ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]] =
    resultBuffer.last match {
      case Left(lastStats) =>
        if (isStatsSmall(lastStats, sortedIndexConfig, segmentConfig))
          resultBuffer.dropRight(1).lastOption match {
            case Some(Left(_)) =>
              throw new Exception(s"Invalid merge. There not have never been two consecutive ${MergeStats.productPrefix}")

            case Some(Right(secondLastSegment: Segment)) =>
              resultBuffer.dropRight(2).lastOption match {
                case Some(Left(thirdLastStats)) =>
                  secondLastSegment.iterator() foreach (keyValue => thirdLastStats.add(keyValue.toMemory()))
                  lastStats.keyValues foreach thirdLastStats.add
                  resultBuffer.remove(resultBuffer.size - 2, 2)
                  resultBuffer

                case Some(Right(thirdLastSegment: Segment)) =>
                  val newStats = createMergeStats(removeDeletes = removeDeletes)
                  secondLastSegment.iterator() foreach (keyValue => newStats.add(keyValue.toMemory()))
                  lastStats.keyValues foreach newStats.add
                  resultBuffer.remove(resultBuffer.size - 2, 2)
                  resultBuffer += Left(newStats)

                case None =>
                  val newStats = createMergeStats(removeDeletes = removeDeletes)
                  secondLastSegment.iterator() foreach (keyValue => newStats.add(keyValue.toMemory()))
                  lastStats.keyValues foreach newStats.add
                  resultBuffer.clear()
                  resultBuffer += Left(newStats)
              }

            case None =>
              //cant do much here there is only one item in the result.
              resultBuffer
          }
        else
          resultBuffer

      case Right(lastSegment) =>
        if (lastSegment.segmentSize < segmentConfig.minSize) {
          val droppedLastSegment = resultBuffer.dropRight(1)

          droppedLastSegment.lastOption match {
            case Some(Left(secondLastStats)) =>
              lastSegment.iterator() foreach (keyValue => secondLastStats.add(keyValue.toMemory()))
              resultBuffer

            case Some(Right(secondLastSegment)) =>
              val newStats = createMergeStats(removeDeletes = removeDeletes)
              secondLastSegment.iterator() foreach (keyValue => newStats.add(keyValue.toMemory()))
              lastSegment.iterator() foreach (keyValue => newStats.add(keyValue.toMemory()))
              resultBuffer.remove(resultBuffer.size - 2, 2)
              resultBuffer += Left(newStats)

            case None =>
              //Nothing to do
              resultBuffer
          }
        } else {
          resultBuffer
        }
    }

  private[ref] def writeTransient(resultBuffer: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]],
                                  removeDeletes: Boolean,
                                  createdInLevel: Int,
                                  valuesConfig: ValuesBlock.Config,
                                  sortedIndexConfig: SortedIndexBlock.Config,
                                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                  hashIndexConfig: HashIndexBlock.Config,
                                  bloomFilterConfig: BloomFilterBlock.Config,
                                  segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                      ec: ExecutionContext): Future[Slice[TransientSegment.Persistent]] =
    Future.traverse(resultBuffer) {
      case Left(stats) =>
        Future {
          val mergeStats =
            stats.close(
              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
            )

          SegmentBlock.writeOneOrMany(
            mergeStats = mergeStats,
            createdInLevel = createdInLevel,
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )
        }

      case Right(segment) =>
        Future.successful(Slice(segment))
    } map {
      buffer =>
        //TODO - group SegmentRefs and write them as PersistentSegmentMany if needed.
        val slice = Slice.of[TransientSegment.Persistent](buffer.foldLeft(0)(_ + _.size))
        buffer foreach slice.addAll
        slice
    }

  private[ref] def mergeNoMid(headGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
                              tailGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
                              segmentRefs: Iterator[SegmentRef],
                              removeDeletes: Boolean,
                              createdInLevel: Int,
                              valuesConfig: ValuesBlock.Config,
                              sortedIndexConfig: SortedIndexBlock.Config,
                              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                              hashIndexConfig: HashIndexBlock.Config,
                              bloomFilterConfig: BloomFilterBlock.Config,
                              segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator,
                                                                  executionContext: ExecutionContext,
                                                                  keyOrder: KeyOrder[Slice[Byte]],
                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                  functionStore: FunctionStore): Future[SegmentMergeResult[Slice[TransientSegment.Persistent]]] = {
    //    val segments = ListBuffer.empty[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]]
    //
    //    defragGaps(
    //      gap = headGap,
    //      segments = segments,
    //      removeDeletes = removeDeletes,
    //      createdInLevel = createdInLevel,
    //      valuesConfig = valuesConfig,
    //      sortedIndexConfig = sortedIndexConfig,
    //      binarySearchIndexConfig = binarySearchIndexConfig,
    //      hashIndexConfig = hashIndexConfig,
    //      bloomFilterConfig = bloomFilterConfig,
    //      segmentConfig = segmentConfig
    //    ) flatMapUnit {
    //      defragGaps(
    //        gap = tailGap,
    //        segments = segments,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      )
    //    } flatMapUnit {
    //      Future {
    //        defragLast(
    //          sortedIndexConfig = sortedIndexConfig,
    //          segmentConfig = segmentConfig,
    //          segments = segments
    //        )
    //      }
    //    } flatMapUnit {
    //      writeTransient(
    //        segments = segments,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      ) map {
    //        transientSegments =>
    //          SegmentMergeResult(
    //            result = transientSegments,
    //            replaced = false
    //          )
    //      }
    //    }
    ???
  }
}
