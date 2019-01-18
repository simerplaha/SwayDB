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

package swaydb.core.level

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Path, StandardOpenOption}

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data._
import swaydb.core.finders.{Get, Higher, Lower}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.level.LevelException.ReceivedKeyValuesToMergeWithoutTargetSegment
import swaydb.core.level.actor.LevelCommand.ClearExpiredKeyValues
import swaydb.core.level.actor.{LevelAPI, LevelActor, LevelActorAPI, LevelCommand}
import swaydb.core.map.serializer._
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.SegmentException.SegmentFileMissing
import swaydb.core.segment.{Segment, SegmentAssigner}
import swaydb.core.util.CollectionUtil._
import swaydb.core.util.ExceptionUtil._
import swaydb.core.util.FileUtil._
import swaydb.core.util.TryUtil._
import swaydb.core.util.{MinMax, _}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.Dir
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._
import swaydb.data.storage.{AppendixStorage, LevelStorage}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import FiniteDurationUtil._
import swaydb.core.function.FunctionStore
import swaydb.data.order.{KeyOrder, TimeOrder}

private[core] object Level extends LazyLogging {

  def acquireLock(levelStorage: LevelStorage): Try[Option[FileLock]] =
    if (levelStorage.persistent)
      Try {
        IO createDirectoriesIfAbsent levelStorage.dir
        val lockFile = levelStorage.dir.resolve("LOCK")
        logger.info("{}: Acquiring lock.", lockFile)
        IO createFileIfAbsent lockFile
        val lock = FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()
        levelStorage.dirs foreach {
          dir =>
            IO createDirectoriesIfAbsent dir.path
        }
        Some(lock)
      }
    else
      TryUtil.successNone

  def apply(segmentSize: Long,
            bloomFilterFalsePositiveRate: Double,
            levelStorage: LevelStorage,
            appendixStorage: AppendixStorage,
            nextLevel: Option[LevelRef],
            pushForward: Boolean = false,
            throttle: LevelMeter => Throttle,
            compressDuplicateValues: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore,
                                              ec: ExecutionContext,
                                              keyValueLimiter: KeyValueLimiter,
                                              fileOpenLimiter: DBFile => Unit,
                                              groupingStrategy: Option[KeyValueGroupingStrategyInternal]): Try[LevelRef] = {
    //acquire lock on folder
    acquireLock(levelStorage) flatMap {
      lock =>
        //lock acquired.
        //initialise readers & writers
        import AppendixMapEntryWriter.{AppendixPutWriter, AppendixRemoveWriter}

        val appendixReader =
          new AppendixMapEntryReader(
            removeDeletes = removeDeletes(nextLevel),
            mmapSegmentsOnRead = levelStorage.mmapSegmentsOnWrite,
            mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnRead
          )

        import appendixReader._

        //default merger
        implicit val merger = AppendixSkipListMerger

        //initialise appendix
        val appendix: Try[Map[Slice[Byte], Segment]] =
          appendixStorage match {
            case AppendixStorage.Persistent(mmap, appendixFlushCheckpointSize) =>
              logger.info("{}: Initialising appendix.", levelStorage.dir)
              val appendixFolder = levelStorage.dir.resolve("appendix")
              //check if appendix folder/file was deleted.
              if ((!IO.exists(appendixFolder) || appendixFolder.files(Extension.Log).isEmpty) && FileUtil.segmentFilesOnDisk(levelStorage.dirs.pathsSet.toSeq).nonEmpty) {
                logger.info("{}: Failed to start Level. Appendix file is missing", appendixFolder)
                Failure(new IllegalStateException(s"Failed to start Level. Appendix file is missing '$appendixFolder'."))
              } else {

                IO createDirectoriesIfAbsent appendixFolder
                Map.persistent[Slice[Byte], Segment](
                  folder = appendixFolder,
                  mmap = mmap,
                  flushOnOverflow = true,
                  fileSize = appendixFlushCheckpointSize,
                  dropCorruptedTailEntries = false
                ).map(_.item)
              }

            case AppendixStorage.Memory =>
              logger.info("{}: Initialising appendix for in-memory Level", levelStorage.dir)
              Try(Map.memory[Slice[Byte], Segment]())
          }

        //initialise Level
        appendix flatMap {
          appendix =>
            logger.debug("{}: Checking Segments exist.", levelStorage.dir)
            //check that all existing Segments in appendix also exists on disk or else return error message.
            appendix.asScala tryForeach {
              case (_, segment) =>
                if (segment.existsOnDisk)
                  TryUtil.successUnit
                else
                  Failure(SegmentFileMissing(segment.path))
            } match {
              case Some(Failure(exception)) =>
                Failure(exception)

              case None =>
                logger.info("{}: Starting level.", levelStorage.dir)

                Success(
                  new Level(
                    dirs = levelStorage.dirs,
                    bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                    pushForward = pushForward,
                    mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnWrite,
                    mmapSegmentsOnRead = levelStorage.mmapSegmentsOnRead,
                    inMemory = levelStorage.memory,
                    segmentSize = segmentSize,
                    appendix = appendix,
                    throttle = throttle,
                    nextLevel = nextLevel,
                    lock = lock,
                    compressDuplicateValues = compressDuplicateValues
                  ).init
                )
            }

        }
    }
  }

  def removeDeletes(nextLevel: Option[LevelRef]): Boolean =
    nextLevel.isEmpty || nextLevel.exists(_.isTrash)

}

private[core] class Level(val dirs: Seq[Dir],
                          bloomFilterFalsePositiveRate: Double,
                          val mmapSegmentsOnWrite: Boolean,
                          val mmapSegmentsOnRead: Boolean,
                          val inMemory: Boolean,
                          val segmentSize: Long,
                          val pushForward: Boolean,
                          val throttle: LevelMeter => Throttle,
                          val nextLevel: Option[LevelRef],
                          appendix: Map[Slice[Byte], Segment],
                          lock: Option[FileLock],
                          val compressDuplicateValues: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                functionStore: FunctionStore,
                                                                ec: ExecutionContext,
                                                                removeWriter: MapEntryWriter[MapEntry.Remove[Slice[Byte]]],
                                                                addWriter: MapEntryWriter[MapEntry.Put[Slice[Byte], Segment]],
                                                                keyValueLimiter: KeyValueLimiter,
                                                                fileOpenLimiter: DBFile => Unit,
                                                                groupingStrategy: Option[KeyValueGroupingStrategyInternal]) extends LevelRef with LevelActorAPI with LazyLogging {

  val paths: PathsDistributor = PathsDistributor(dirs, () => appendix.values().asScala)

  logger.info(s"{}: Level started.", paths)

  val removeDeletedRecords = Level.removeDeletes(nextLevel)

  def rootPath: Path =
    dirs.head.path

  def appendixPath: Path =
    rootPath.resolve("appendix")

  def releaseLocks: Try[Unit] =
    IO.release(lock) flatMap {
      _ =>
        nextLevel.map(_.releaseLocks) getOrElse TryUtil.successUnit
    }

  private def deleteUncommittedSegments(): Unit =
    dirs.flatMap(_.path.files(Extension.Seg)) foreach {
      segmentToDelete =>
        val toDelete =
          appendix.foldLeft(true) {
            case (toDelete, (_, appendixSegment)) =>
              if (appendixSegment.path == segmentToDelete)
                false
              else
                toDelete
          }
        if (toDelete) {
          logger.info("SEGMENT {} not in appendix. Deleting uncommitted segment.", segmentToDelete)
          IO.delete(segmentToDelete)
        }
    }

  private def largestSegmentId: Long =
    appendix.foldLeft(0L) {
      case (initialId, (_, segment)) =>
        val segmentId = segment.path.fileId.get._1
        if (initialId > segmentId)
          initialId
        else
          segmentId
    }

  private[level] implicit val segmentIDGenerator = IDGenerator(initial = largestSegmentId)

  def init: Level = {
    if (existsOnDisk) deleteUncommittedSegments()
    //If this is the last level, dispatch initial message to start the process of clearing expired key-values.
    if (nextLevel.isEmpty) actor ! ClearExpiredKeyValues(0.nanosecond.fromNow)
    this
  }

  private val actor =
    LevelActor(ec, this, keyOrder)

  override def !(request: LevelAPI): Unit =
    actor ! request

  override def forward(command: LevelAPI): Try[Unit] =
    nextLevel map {
      nextLevel =>
        if (pushForward && isEmpty && nextLevel.isEmpty) {
          logger.debug("{}: Push forwarded.", paths)
          nextLevel ! command
          TryUtil.successUnit
        }
        else
          Failure(LevelException.NotSentToNextLevel)
    } getOrElse Failure(LevelException.NotSentToNextLevel)

  override def push(command: LevelAPI): Unit =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel ! command
      case None =>
        logger.error("{}: Push submitted. But there is no lower level", paths.head)
    }

  def nextPushDelay: FiniteDuration =
    throttle(meter).pushDelay

  def nextBatchSize: Int =
    throttle(meter).segmentsToPush

  def nextPushDelayAndBatchSize =
    throttle(meter)

  def nextPushDelayAndSegmentsCount: (FiniteDuration, Int) = {
    val stats = meter
    (throttle(stats).pushDelay, stats.segmentsCount)
  }

  def nextBatchSizeAndSegmentsCount: (Int, Int) = {
    val stats = meter
    (throttle(stats).segmentsToPush, stats.segmentsCount)
  }

  def put(segment: Segment): Try[Unit] =
    put(Seq(segment))

  def put(segments: Iterable[Segment]): Try[Unit] = {
    logger.trace(s"{}: Putting segments '{}' segments.", paths.head, segments.map(_.path.toString).toList)
    //check to ensure that the Segments do not overlap with busy Segments
    val busySegments = getBusySegments()
    val appendixValues = appendix.values().asScala
    Segment.overlapsWithBusySegments(segments, busySegments, appendixValues) flatMap {
      overlaps =>
        if (overlaps) {
          logger.debug("{}: Segments '{}' intersect with current busy segments: {}", paths.head, segments.map(_.path.toString), busySegments.map(_.path.toString))
          Failure(LevelException.ContainsOverlappingBusySegments)
        } else { //only copy Segments if the both this Level and the Segments are persistent.
          val (segmentToMerge, segmentToCopy) = Segment.partitionOverlapping(segments, appendixValues)
          put(segmentToMerge, segmentToCopy, appendixValues).map(_ => alertActorForSegmentManagement())
        }
    }
  }

  private def deleteCopiedSegments(copiedSegments: Iterable[Segment]) =
    copiedSegments foreach {
      segmentToDelete =>
        segmentToDelete.delete.failed foreach {
          exception =>
            logger.error(s"{}: Failed to delete copied Segment '{}'", paths.head, segmentToDelete.path, exception)
        }
    }

  private[level] def put(segmentsToMerge: Iterable[Segment],
                         segmentsToCopy: Iterable[Segment],
                         targetSegments: Iterable[Segment]): Try[Unit] =
    if (segmentsToCopy.nonEmpty)
      copy(segmentsToCopy) flatMap {
        copiedSegments =>
          buildNewMapEntry(copiedSegments, None, None) flatMap {
            copiedSegmentsEntry =>
              val putResult: Try[Unit] =
                if (segmentsToMerge.nonEmpty)
                  merge(segmentsToMerge, targetSegments, Some(copiedSegmentsEntry))
                else
                  appendix.write(copiedSegmentsEntry) map (_ => ())

              putResult recoverWith {
                case exception =>
                  logFailure(s"${paths.head}: Failed to create a log entry. Deleting ${copiedSegments.size} copied segments", exception)
                  deleteCopiedSegments(copiedSegments)
                  Failure(exception)
              }
          }
      }
    else
      merge(segmentsToMerge, targetSegments, None)

  def putMap(map: Map[Slice[Byte], Memory.SegmentResponse]): Try[Unit] = {
    logger.trace("{}: PutMap '{}' Maps.", paths.head, map.count())
    //do an initial check to ensure that the Segments do not overlap with busy Segments
    val busySegs = getBusySegments()
    val appendixValues = appendix.values().asScala
    Segment.overlapsWithBusySegments(map, busySegs, appendixValues) flatMap {
      overlapsWithBusySegments =>
        if (overlapsWithBusySegments) {
          logger.debug("{}: Map '{}' contains key-values intersect with current busy segments: {}", paths.head, map.pathOption.map(_.toString), busySegs.map(_.path.toString))
          Failure(LevelException.ContainsOverlappingBusySegments)
        } else {
          if (!Segment.overlaps(map, appendixValues))
            copy(map) flatMap {
              newSegments =>
                //maps can be submitted directly to last Level (if all levels have pushForward == true or if there are only two Levels (0 and 1)).
                //If this Level is the last Level then copy can return empty List[Segments]. If this is not the last Level then continue execution regardless because
                //upper Levels should ALWAYS return non-empty Segments on merge as key-values NEVER get removed/deleted from upper Levels.
                //If the Segments are still empty, buildNewMapEntry will return a failure which is expected.
                //Note: Logs can get spammed due to buildNewMapEntry's failure because LevelZeroActor will dispatch a PullRequest (for ANY failure),
                //to which this Level will respond with a Push message to LevelZeroActor and the same failure will occur repeatedly since the error is not due to busy Segments.
                //but this should not occur during runtime and if it's does occur the spam is OK because it's a crucial error and should be fixed immediately as this error would result
                //to compaction coming to a halt.
                if (nextLevel.isDefined || newSegments.nonEmpty)
                  buildNewMapEntry(newSegments, None, None) flatMap {
                    entry =>
                      appendix
                        .write(entry)
                        .map {
                          _ =>
                            //Execute alertActorForSegmentManagement only if this is the last Level.
                            //alertActorForSegmentManagement is not always required here because Map generally are submitted to higher Levels and higher Level Segments
                            //always get merged to lower Level which will eventually collapse and expire key-values.
                            if (nextLevel.isEmpty) alertActorForSegmentManagement()
                        }
                  } recoverWith {
                    case exception =>
                      logFailure(s"${paths.head}: Failed to create a log entry.", exception)
                      deleteCopiedSegments(newSegments)
                      Failure(exception)
                  }
                else
                  TryUtil.successUnit
            }
          else
            putKeyValues(Slice(map.values().toArray(new Array[Memory](map.skipList.size()))), appendixValues, None)
        }
    }
  }

  private[level] def copy(map: Map[Slice[Byte], Memory.SegmentResponse]): Try[Iterable[Segment]] = {
    logger.trace(s"{}: Copying {} Map", paths.head, map.pathOption)

    def targetSegmentPath = paths.next.resolve(IDGenerator.segmentId(segmentIDGenerator.nextID))

    val keyValues = Slice(map.skipList.values().asScala.toArray)
    if (inMemory)
      Segment.copyToMemory(
        keyValues = keyValues,
        fetchNextPath = targetSegmentPath,
        minSegmentSize = segmentSize,
        removeDeletes = removeDeletedRecords,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues
      )
    else
      Segment.copyToPersist(
        keyValues = keyValues,
        fetchNextPath = targetSegmentPath,
        mmapSegmentsOnRead = mmapSegmentsOnRead,
        mmapSegmentsOnWrite = mmapSegmentsOnWrite,
        minSegmentSize = segmentSize,
        removeDeletes = removeDeletedRecords,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues
      )
  }

  private[level] def copy(segmentsToCopy: Iterable[Segment]): Try[Iterable[Segment]] = {
    logger.trace(s"{}: Copying {} Segments", paths.head, segmentsToCopy.map(_.path.toString))
    segmentsToCopy.tryFlattenIterable[Segment](
      tryBlock =
        segment => {
          def targetSegmentPath = paths.next.resolve(IDGenerator.segmentId(segmentIDGenerator.nextID))

          if (inMemory)
            Segment.copyToMemory(
              segment = segment,
              fetchNextPath = targetSegmentPath,
              minSegmentSize = segmentSize,
              removeDeletes = removeDeletedRecords,
              bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
              compressDuplicateValues = compressDuplicateValues
            )
          else
            Segment.copyToPersist(
              segment = segment,
              fetchNextPath = targetSegmentPath,
              mmapSegmentsOnRead = mmapSegmentsOnRead,
              mmapSegmentsOnWrite = mmapSegmentsOnWrite,
              minSegmentSize = segmentSize,
              removeDeletes = removeDeletedRecords,
              bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
              compressDuplicateValues = compressDuplicateValues
            )
        },
      recover =
        (segments, failure) => {
          logFailure(s"${paths.head}: Failed to copy Segments. Deleting partially copied Segments ${segments.size} Segments", failure.exception)
          segments foreach {
            segment =>
              segment.delete.failed foreach {
                exception =>
                  logger.error(s"{}: Failed to delete copied Segment '{}'", paths.head, segment.path, exception)
              }
          }
        }
    )
  }

  override def clearExpiredKeyValues(): Try[Unit] = {
    logger.debug("{}: Running clearExpiredKeyValues.", paths.head)
    if (nextLevel.nonEmpty) { //only run this if it's the last Level.
      logger.error("{}: clearExpiredKeyValues ran a Level that is not the last Level.", paths.head)
      TryUtil.successUnit
    } else {
      Segment.getNearestDeadlineSegment(appendix.values().asScala) map {
        segmentToClear =>
          logger.debug("{}: Executing clearExpiredKeyValues on Segment: '{}'.", paths.head, segmentToClear.path)
          val busySegments = getBusySegments()
          if (Segment.intersects(Seq(segmentToClear), busySegments)) {
            logger.debug(s"{}: Clearing segments {} intersect with current busy segments: {}.", paths.head, segmentToClear.path, busySegments.map(_.path.toString))
            Failure(LevelException.ContainsOverlappingBusySegments)
          } else {
            segmentToClear.refresh(
              minSegmentSize = segmentSize,
              bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
              compressDuplicateValues = compressDuplicateValues,
              targetPaths = paths
            ) flatMap {
              newSegments =>
                logger.debug(s"{}: Segment {} successfully cleared of expired key-values. New Segments: {}.", paths.head, segmentToClear.path, newSegments.map(_.path).mkString(", "))
                buildNewMapEntry(newSegments, Some(segmentToClear), None) flatMap {
                  entry =>
                    appendix.write(entry) flatMap {
                      _ =>
                        alertActorForSegmentManagement()
                        segmentToClear.delete recoverWith {
                          case exception =>
                            logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segmentToClear.path, exception)
                            TryUtil.successUnit
                        }
                    }
                } recoverWith {
                  case exception =>
                    newSegments foreach {
                      segment =>
                        segment.delete.failed foreach {
                          exception =>
                            logger.error(s"{}: Failed to delete Segment {}", paths.head, segment.path, exception)
                        }
                    }
                    Failure(exception)
                }
            }
          }
      } getOrElse {
        logger.debug("{}: No expired key-values to clear.", paths.head)
        alertActorForSegmentManagement()
        TryUtil.successUnit
      }
    }
  }

  /**
    * A Segment is considered small if it's size is less than 60% of the default [[segmentSize]]
    */
  def isSmallSegment(segment: Segment): Boolean =
    segment.segmentSize < segmentSize * 0.60

  def collapseAllSmallSegments(batch: Int): Try[Int] = {
    logger.trace("{}: Running collapseAllSmallSegments batch: '{}'.", paths.head, batch)
    if (batch <= 0)
      TryUtil.successZero
    else
      collapseSegments(batch, isSmallSegment)
  }

  /**
    * @return remaining small Segment that were not collapsed.
    */
  private def collapseSegments(count: Int, condition: Segment => Boolean): Try[Int] = {
    @tailrec
    def run(timesToRun: Int): Try[Int] = {
      val segmentsToCollapse = takeSegments(count, condition)
      if (segmentsToCollapse.isEmpty) {
        TryUtil.successZero
      } else {
        val busySegments = getBusySegments()
        if (Segment.intersects(segmentsToCollapse, busySegments)) {
          logger.debug(s"{}: Collapsing segments {} intersect with current busy segments: {}.", paths.head, segmentsToCollapse.map(_.path.toString), busySegments.map(_.path.toString))
          Failure(LevelException.ContainsOverlappingBusySegments)
        } else if (timesToRun == 0) {
          logger.debug(s"{}: Too many small Segments to collapse {}.", paths.head, segmentsToCollapse.map(_.path.toString))
          //there are too many small Segments to collapse, return the remaining small Segments and let the Actor decide when
          //to continue collapsing.
          Success(segmentsToCollapse.size)
        } else {
          collapse(segmentsToCollapse, appendix.values().asScala) match {
            case success @ Success(value) if value == 0 =>
              success

            case Success(_) =>
              run(timesToRun - 1)

            case Failure(exception) =>
              Failure(exception)
          }
        }
      }
    }

    run(timesToRun = 2)
  }

  def collapse(segments: Iterable[Segment],
               appendix: Iterable[Segment]): Try[Int] = {
    logger.trace(s"{}: Collapsing '{}' segments", paths.head, segments.size)
    if (segments.isEmpty) {
      Success(0)
    } else if (appendix.size == 1) { //if there is only one Segment in this Level which is a small segment. No collapse required
      Success(0)
    } else {
      //other segments in the appendix that are not the input segments (segments to collapse).
      val targetAppendixSegments = appendix.filterNot(map => segments.exists(_.path == map.path))
      val (segmentsToMerge, targetSegments) =
        if (targetAppendixSegments.nonEmpty) {
          logger.trace(s"{}: Target appendix segments {}", paths.head, targetAppendixSegments.size)
          (segments, targetAppendixSegments)
        }
        else {
          //If appendix without the small Segments is empty.
          // Then pick the first segment from the smallest segments and merge other small segments into it.
          val firstSmallSegment = Iterable(segments.head)
          logger.trace(s"{}: Target segments {}", paths.head, firstSmallSegment.size)
          (segments.drop(1), firstSmallSegment)
        }

      //create an appendEntry that will remove smaller segments from the appendix.
      //this entry will get applied only if the putSegments is successful orElse this entry will be discarded.
      val appendEntry =
      segmentsToMerge.foldLeft(Option.empty[MapEntry[Slice[Byte], Segment]]) {
        case (mapEntry, smallSegment) =>
          val entry = MapEntry.Remove(smallSegment.minKey)
          mapEntry.map(_ ++ entry) orElse Some(entry)
      }
      merge(segmentsToMerge, targetSegments, appendEntry) map {
        _ =>
          //delete the segments merged with self.
          segmentsToMerge foreach {
            segment =>
              segment.delete.failed map {
                exception =>
                  logger.warn(s"{}: Failed to delete Segment {} after successful collapse", paths.head, segment.path, exception)
              }
          }
          segmentsToMerge.size
      }
    }
  }

  private def merge(segments: Iterable[Segment],
                    targetSegments: Iterable[Segment],
                    appendEntry: Option[MapEntry[Slice[Byte], Segment]]): Try[Unit] = {
    logger.trace(s"{}: Merging segments {}", paths.head, segments.map(_.path.toString))
    Segment.getAllKeyValues(bloomFilterFalsePositiveRate, segments) flatMap {
      putKeyValues(_, targetSegments, appendEntry)
    }
  }

  private def putKeyValues(keyValues: Slice[KeyValue.ReadOnly],
                           targetSegments: Iterable[Segment],
                           appendEntry: Option[MapEntry[Slice[Byte], Segment]]): Try[Unit] = {
    logger.trace(s"{}: Merging {} KeyValues.", paths.head, keyValues.size)
    SegmentAssigner.assign(keyValues, targetSegments) flatMap {
      assignments =>

        val busySegments: Seq[Segment] = getBusySegments()
        //check to ensure that assigned Segments do not overlap with busy Segments.
        if (Segment.intersects(assignments.keys, busySegments)) {
          logger.trace(s"{}: Assigned segments {} intersect with current busy segments: {}.", paths.head, assignments.map(_._1.path.toString), busySegments.map(_.path.toString))
          Failure(LevelException.ContainsOverlappingBusySegments)
        } else {
          logger.trace(s"{}: Assigned segments {} for {} KeyValues.", paths.head, assignments.map(_._1.path.toString), keyValues.size)
          if (assignments.isEmpty) {
            logger.error(s"{}: Assigned segments are empty. Cannot merge Segments to empty target Segments: {}.", paths.head, keyValues.size)
            Failure(ReceivedKeyValuesToMergeWithoutTargetSegment(keyValues.size))
          } else {
            logger.debug(s"{}: Assigned segments {}. Merging {} KeyValues.", paths.head, assignments.map(_._1.path.toString), keyValues.size)
            putAssignedKeyValues(assignments) flatMap {
              targetSegmentAndNewSegments =>
                targetSegmentAndNewSegments.tryFoldLeft(Option.empty[MapEntry[Slice[Byte], Segment]]) {
                  case (mapEntry, (targetSegment, newSegments)) =>
                    buildNewMapEntry(newSegments, Some(targetSegment), mapEntry).map(Some(_))
                } flatMap {
                  case Some(mapEntry) =>
                    //also write appendEntry to this mapEntry before commiting entries to appendix.
                    //Note: appendEntry should not overwrite new Segment's entries with same keys so perform distinct
                    //which will remove oldEntries with duplicates with newer keys.
                    val mapEntryToWrite = appendEntry.map(appendEntry => MapEntry.distinct(mapEntry, appendEntry)) getOrElse mapEntry
                    (appendix write mapEntryToWrite) map {
                      _ =>
                        logger.debug(s"{}: putKeyValues successful. Deleting assigned Segments. {}.", paths.head, assignments.map(_._1.path.toString))
                        //delete assigned segments as they are replaced with new segments.
                        assignments foreach {
                          case (segment, _) =>
                            segment.delete.failed map {
                              exception =>
                                logger.error(s"{}: Failed to delete Segment {}", paths.head, segment.path, exception)
                            }
                        }
                    }

                  case None =>
                    Failure(new Exception(s"${paths.head}: Failed to create map entry"))

                } recoverWith {
                  case exception =>
                    logFailure(s"${paths.head}: Failed to write key-values. Reverting", exception)
                    targetSegmentAndNewSegments foreach {
                      case (_, newSegments) =>
                        newSegments foreach {
                          segment =>
                            segment.delete.failed map {
                              exception =>
                                logger.error(s"{}: Failed to delete Segment {}", paths.head, segment.path, exception)
                            }
                        }
                    }
                    Failure(exception)
                }
            }
          }
        }
    }
  }

  private def putAssignedKeyValues(assignedSegments: mutable.Map[Segment, Slice[KeyValue.ReadOnly]]): Try[Slice[(Segment, Slice[Segment])]] =
    assignedSegments.tryMap[(Segment, Slice[Segment])](
      tryBlock = {
        case (targetSegment, assignedKeyValues) =>
          targetSegment.put(
            newKeyValues = assignedKeyValues,
            minSegmentSize = segmentSize,
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
            compressDuplicateValues = compressDuplicateValues,
            targetPaths = paths.addPriorityPath(targetSegment.path.getParent)
          ) map {
            newSegments =>
              (targetSegment, newSegments)
          }
      },
      recover =
        (targetSegmentAndNewSegments, failure) => {
          logFailure(s"${paths.head}: Failed to do putAssignedKeyValues, Reverting and deleting written ${targetSegmentAndNewSegments.size} segments", failure.exception)
          targetSegmentAndNewSegments foreach {
            case (_, newSegments) =>
              newSegments foreach {
                segment =>
                  segment.delete.failed foreach {
                    exception =>
                      logger.error(s"{}: Failed to delete Segment '{}' in recovery for putAssignedKeyValues", paths.head, segment.path, exception)
                  }
              }

          }
        }
    )

  //if there is a small segment in the new segments, alert the Actor to collapse the small segments before next Push
  //this function can be achieved in single iteration.
  def alertActorForSegmentManagement() = {
    logger.debug(s"{}: Executing check for small Segments.", paths.head)
    //if there is no next Level do management check for all Segments in the Level.
    val segments = this.segmentsInLevel()
    //only collapse if there are more than 1 Segment in the Level.
    var collapseSegmentAlertSent = false
    if (segments.size > 1)
      segments.iterator foreachBreak {
        segment =>
          if (!collapseSegmentAlertSent && isSmallSegment(segment)) {
            logger.debug(s"{}: Found small Segment: '{}' of size: {}. Alerting Level actor to collapse Segment.", paths.head, segment.path, segment.segmentSize)
            actor ! LevelCommand.CollapseSmallSegments
            collapseSegmentAlertSent = true
          }
          collapseSegmentAlertSent
      }

    //expiration happens only in the last Level, if this is the last Level, only then check for expired key-values.
    if (nextLevel.isEmpty) {
      logger.debug(s"{}: Executing check for expired key-values.", paths.head)
      Segment.getNearestDeadlineSegment(segments) foreach {
        segment =>
          logger.debug(s"{}: Next nearest expired key-value deadline {} within {}.", paths.head, segment.nearestExpiryDeadline, segment.nearestExpiryDeadline.map(_.timeLeft.asString))
          segment.nearestExpiryDeadline foreach {
            deadline =>
              logger.debug(s"{}: Alerting actor for next nearest expired deadline: {} within {}.", paths.head, segment.nearestExpiryDeadline, segment.nearestExpiryDeadline.map(_.timeLeft.asString))
              actor ! LevelCommand.ClearExpiredKeyValues(deadline)
          }
      }
    }
  }

  def buildNewMapEntry(newSegments: Iterable[Segment],
                       originalSegmentMayBe: Option[Segment] = None,
                       initialMapEntry: Option[MapEntry[Slice[Byte], Segment]]): Try[MapEntry[Slice[Byte], Segment]] = {

    import keyOrder._

    var removeOriginalSegments = true
    val nextLogEntry =
      newSegments.foldLeft(initialMapEntry) {
        case (logEntry, newSegment) =>
          //if one of the new segments have the same minKey as the original segment, remove is not required as 'put' will replace old key.
          if (removeOriginalSegments && originalSegmentMayBe.exists(_.minKey equiv newSegment.minKey))
            removeOriginalSegments = false

          val nextLogEntry = MapEntry.Put(newSegment.minKey, newSegment)
          logEntry.map(_ ++ nextLogEntry) orElse Some(nextLogEntry)
      }
    (originalSegmentMayBe match {
      case Some(originalMap) if removeOriginalSegments =>
        val removeLogEntry = MapEntry.Remove[Slice[Byte]](originalMap.minKey)
        nextLogEntry.map(_ ++ removeLogEntry) orElse Some(removeLogEntry)
      case _ =>
        nextLogEntry
    }) match {
      case Some(value) =>
        Success(value)
      case None =>
        Failure(new Exception("Failed to build map entry"))
    }
  }

  def removeSegments(segments: Iterable[Segment]): Try[Int] = {
    //create this list which is a copy of segments. Segments can be iterable only once if it's a Java iterable.
    //this copy is for second read to delete the segments after the MapEntry is successfully created.
    logger.trace(s"{}: Removing Segments {}", paths.head, segments.map(_.path.toString))
    val segmentsToRemove = Slice.create[Segment](segments.size)

    segments.foldLeft(Option.empty[MapEntry[Slice[Byte], Segment]]) {
      case (previousEntry, segmentToRemove) =>
        segmentsToRemove add segmentToRemove
        val nextEntry = MapEntry.Remove[Slice[Byte]](segmentToRemove.minKey)
        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
    } map {
      mapEntry =>
        //        logger.info(s"$id. Build map entry: ${mapEntry.string(_.asInt().toString, _.id.toString)}")
        logger.trace(s"{}: Built map entry to remove Segments {}", paths.head, segments.map(_.path.toString))
        (appendix write mapEntry) flatMap {
          _ =>
            logger.debug(s"{}: MapEntry delete Segments successfully written. Deleting physical Segments: {}", paths.head, segments.map(_.path.toString))
            // If a delete fails that would be due OS permission issue.
            // But it's OK if it fails as long as appendix is updated with new segments. An error message will be logged
            // asking to delete the uncommitted segments manually or do a database restart which will delete the uncommitted
            // Segments on reboot.
            Segment.deleteSegments(segmentsToRemove) recoverWith {
              case exception =>
                logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segmentsToRemove.map(_.path.toString).mkString(", "), exception)
                Success(0)
            }
        }
    } getOrElse Failure(LevelException.NoSegmentsRemoved)
  }

  def getFromThisLevel(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.SegmentResponse]] =
    appendix.floor(key) match {
      case Some(segment) =>
        segment get key

      case None =>
        TryUtil.successNone
    }

  def getFromNextLevel(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    nextLevel.map(_.get(key)) getOrElse TryUtil.successNone

  override def get(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    Get(key, getFromThisLevel, getFromNextLevel)

  def mightContainInThisLevel(key: Slice[Byte]): Try[Boolean] =
    appendix.floor(key) match {
      case Some(segment) =>
        segment mightContain key

      case None =>
        Success(false)
    }

  override def mightContain(key: Slice[Byte]): Try[Boolean] =
    mightContainInThisLevel(key) flatMap {
      yes =>
        if (yes)
          Success(yes)
        else
          nextLevel.map(_.mightContain(key)) getOrElse Success(yes)
    }

  def lowerInThisLevel(key: Slice[Byte]) =
    appendix.lowerValue(key).map(_.lower(key)) getOrElse TryUtil.successNone

  private def lowerFromNextLevel(key: Slice[Byte]) =
    nextLevel.map(_.lower(key)) getOrElse TryUtil.successNone

  override def floor(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    get(key) match {
      case Success(Some(floor)) =>
        if (floor.hasTimeLeft())
          Success(Some(floor))
        else
          lower(key)

      case Success(None) =>
        lower(key)

      case failed @ Failure(_) =>
        failed
    }

  override def lower(key: Slice[Byte]): Try[Option[ReadOnly.Put]] =
    Lower(key, lowerInThisLevel, lowerFromNextLevel)

  private def higherFromFloorSegment(key: Slice[Byte]) =
    appendix.floor(key).map(_.higher(key)) getOrElse TryUtil.successNone

  private def higherFromHigherSegment(key: Slice[Byte]) =
    appendix.higherValue(key).map(_.higher(key)) getOrElse TryUtil.successNone

  private def higherInThisLevel(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly]] =
    higherFromFloorSegment(key) flatMap {
      fromFloor =>
        if (fromFloor.isDefined)
          Success(fromFloor)
        else
          higherFromHigherSegment(key)
    }

  private def higherInNextLevel(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    nextLevel.map(_.higher(key)) getOrElse TryUtil.successNone

  def ceiling(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    get(key) match {
      case Success(Some(ceiling)) =>
        Success(Some(ceiling))

      case Success(None) =>
        higher(key)

      case failed @ Failure(_) =>
        failed
    }

  override def higher(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly.Put]] =
    Higher(key, higherInThisLevel, get, higherInNextLevel)

  /**
    * Does a quick appendix lookup.
    * It does not check if the returned key is removed. Use [[Level.head]] instead.
    */
  override def headKey: Try[Option[Slice[Byte]]] =
    nextLevel.map(_.headKey) getOrElse TryUtil.successNone map {
      nextLevelFirstKey =>
        MinMax.min(appendix.firstKey, nextLevelFirstKey)(keyOrder)
    }

  /**
    * Does a quick appendix lookup.
    * It does not check if the returned key is removed. Use [[Level.last]] instead.
    */
  override def lastKey: Try[Option[Slice[Byte]]] =
    nextLevel.map(_.lastKey) getOrElse TryUtil.successNone map {
      nextLevelLastKey =>
        MinMax.max(appendix.lastValue().map(_.maxKey.maxKey), nextLevelLastKey)(keyOrder)
    }

  override def head =
    headKey flatMap {
      firstKey =>
        firstKey.map(ceiling) getOrElse TryUtil.successNone
    }

  override def last =
    lastKey flatMap {
      lastKey =>
        lastKey.map(floor) getOrElse TryUtil.successNone
    }

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    appendix contains minKey

  override def bloomFilterKeyValueCount: Try[Int] =
    appendix.foldLeft(Try(0)) {
      case (currentTotal, (_, segment)) =>
        segment.getBloomFilterKeyValueCount() flatMap {
          segmentSize =>
            currentTotal.map(_ + segmentSize)
        }
    } flatMap {
      thisLevelCount =>
        nextLevel.map(_.bloomFilterKeyValueCount).getOrElse(Success(0)) map (_ + thisLevelCount)
    }

  def getSegment(minKey: Slice[Byte]): Option[Segment] =
    appendix.get(minKey)(keyOrder)

  def lowerSegment(key: Slice[Byte]): Option[Segment] =
    (appendix lower key).map(_._2)

  def lowerSegmentMinKey(key: Slice[Byte]): Option[Slice[Byte]] =
    lowerSegment(key).map(_.minKey)

  def higherSegment(key: Slice[Byte]): Option[Segment] =
    (appendix higher key).map(_._2)

  override def getBusySegments(): List[Segment] =
    actor.getBusySegments

  override def segmentsCount(): Int =
    appendix.count()

  override def take(count: Int): Slice[Segment] =
    appendix take count

  override def pickSegmentsToPush(count: Int): Iterable[Segment] = {
    if (count == 0)
      Iterable.empty
    else
      nextLevel.map(_.getBusySegments()) match {
        case Some(nextLevelsBusySegments) if nextLevelsBusySegments.nonEmpty =>
          Segment.nonOverlapping(
            segments1 = segmentsInLevel(),
            segments2 = nextLevelsBusySegments,
            count = count
          )

        case _ =>
          take(count)
      }
  }

  def isEmpty: Boolean =
    appendix.isEmpty

  def isSleeping: Boolean =
    actor.isSleeping

  def isPushing: Boolean =
    actor.isPushing

  def segmentFilesOnDisk: Seq[Path] =
    FileUtil.segmentFilesOnDisk(dirs.map(_.path))

  def segmentFilesInAppendix: Int =
    appendix.count()

  def foreach[T](f: (Slice[Byte], Segment) => T): Unit =
    appendix.foreach(f)

  def segmentsInLevel(): Iterable[Segment] =
    appendix.values().asScala

  def hasNextLevel: Boolean =
    nextLevel.isDefined

  override def existsOnDisk =
    dirs.forall(_.path.exists)

  override def levelSize: Long =
    appendix.foldLeft(0)(_ + _._2.segmentSize)

  override def sizeOfSegments: Long =
    levelSize + nextLevel.map(_.levelSize).getOrElse(0L)

  def segmentCountAndLevelSize: (Int, Long) =
    appendix.foldLeft(0, 0L) {
      case ((segments, size), (_, segment)) =>
        (segments + 1, size + segment.segmentSize)
    }

  def meter: LevelMeter = {
    val (segmentsCount, levelSize) = segmentCountAndLevelSize
    LevelMeter(segmentsCount, levelSize)
  }

  def levelNumber: Long =
    paths.head.path.folderId

  def meterFor(levelNumber: Int): Option[LevelMeter] =
    if (levelNumber == paths.head.path.folderId)
      Some(meter)
    else
      nextLevel.flatMap(_.meterFor(levelNumber))

  override def takeSegments(size: Int, condition: Segment => Boolean): Iterable[Segment] =
    appendix.values().asScala filter {
      segment =>
        condition(segment)
    } take size

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    appendix.values().asScala.filter(_.segmentSize > segmentSize) take size

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    appendix.values().asScala.filter(_.segmentSize < segmentSize) take size

  def close: Try[Unit] =
    (nextLevel.map(_.close) getOrElse TryUtil.successUnit) flatMap {
      _ =>
        actor.terminate()
        appendix.close().failed foreach {
          exception =>
            logger.error("{}: Failed to close appendix", paths.head, exception)
        }
        closeSegments().failed foreach {
          exception =>
            logger.error("{}: Failed to close segments", paths.head, exception)
        }
        releaseLocks.failed foreach {
          exception =>
            logger.error("{}: Failed to release locks", paths.head, exception)
        }

        TryUtil.successUnit
    }

  def closeSegments(): Try[Unit] = {
    segmentsInLevel().tryForeach(_.close, failFast = false) foreach {
      case Failure(exception) =>
        logger.error("{}: Failed to close Segment file.", paths.head, exception)
    }

    nextLevel.map(_.closeSegments()) getOrElse TryUtil.successUnit
  }

  override val isTrash: Boolean =
    false

}
