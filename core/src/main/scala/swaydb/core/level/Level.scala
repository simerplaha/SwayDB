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
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.IOEffect
import swaydb.core.io.file.IOEffect._
import swaydb.core.map.serializer._
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.seek.{NextWalker, _}
import swaydb.core.segment.SegmentException.SegmentFileMissing
import swaydb.core.segment.{Segment, SegmentAssigner}
import swaydb.core.util.ExceptionUtil._
import swaydb.core.util.{MinMax, _}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._
import swaydb.data.storage.{AppendixStorage, LevelStorage}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

private[core] object Level extends LazyLogging {

  def acquireLock(levelStorage: LevelStorage): IO[Option[FileLock]] =
    if (levelStorage.persistent)
      IO {
        IOEffect createDirectoriesIfAbsent levelStorage.dir
        val lockFile = levelStorage.dir.resolve("LOCK")
        logger.info("{}: Acquiring lock.", lockFile)
        IOEffect createFileIfAbsent lockFile
        val lock = FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()
        levelStorage.dirs foreach {
          dir =>
            IOEffect createDirectoriesIfAbsent dir.path
        }
        Some(lock)
      }
    else
      IO.none

  def apply(segmentSize: Long,
            bloomFilterFalsePositiveRate: Double,
            levelStorage: LevelStorage,
            appendixStorage: AppendixStorage,
            nextLevel: Option[LevelRef],
            pushForward: Boolean = false,
            throttle: LevelMeter => Throttle,
            compressDuplicateValues: Boolean,
            deleteSegmentsEventually: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore,
                                               ec: ExecutionContext,
                                               keyValueLimiter: KeyValueLimiter,
                                               fileOpenLimiter: FileLimiter,
                                               groupingStrategy: Option[KeyValueGroupingStrategyInternal]): IO[LevelRef] = {
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
        val appendix: IO[Map[Slice[Byte], Segment]] =
          appendixStorage match {
            case AppendixStorage.Persistent(mmap, appendixFlushCheckpointSize) =>
              logger.info("{}: Initialising appendix.", levelStorage.dir)
              val appendixFolder = levelStorage.dir.resolve("appendix")
              //check if appendix folder/file was deleted.
              if ((!IOEffect.exists(appendixFolder) || appendixFolder.files(Extension.Log).isEmpty) && IOEffect.segmentFilesOnDisk(levelStorage.dirs.pathsSet.toSeq).nonEmpty) {
                logger.info("{}: Failed to start Level. Appendix file is missing", appendixFolder)
                IO.Failure(new IllegalStateException(s"Failed to start Level. Appendix file is missing '$appendixFolder'."))
              } else {

                IOEffect createDirectoriesIfAbsent appendixFolder
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
              IO(Map.memory[Slice[Byte], Segment]())
          }

        //initialise Level
        appendix flatMap {
          appendix =>
            logger.debug("{}: Checking Segments exist.", levelStorage.dir)
            //check that all existing Segments in appendix also exists on disk or else return error message.
            appendix.asScala foreachIO {
              case (_, segment) =>
                if (segment.existsOnDisk)
                  IO.unit
                else
                  IO.Failure(IO.Error.Fatal(SegmentFileMissing(segment.path)))
            } match {
              case Some(IO.Failure(error)) =>
                IO.Failure(error)

              case None =>
                logger.info("{}: Starting level.", levelStorage.dir)

                val allSegments = appendix.values().asScala
                implicit val segmentIDGenerator = IDGenerator(initial = largestSegmentId(allSegments))
                val paths: PathsDistributor = PathsDistributor(levelStorage.dirs, () => allSegments)

                val deletedUnCommittedSegments =
                  if (appendixStorage.persistent)
                    deleteUncommittedSegments(levelStorage.dirs, appendix.values().asScala)
                  else
                    IO.unit

                deletedUnCommittedSegments map {
                  _ =>
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
                      compressDuplicateValues = compressDuplicateValues,
                      deleteSegmentsEventually = deleteSegmentsEventually,
                      paths = paths,
                      removeDeletedRecords = Level.removeDeletes(nextLevel)
                    )
                }
            }
        }
    }
  }

  def removeDeletes(nextLevel: Option[LevelRef]): Boolean =
    nextLevel.isEmpty || nextLevel.exists(_.isTrash)

  def largestSegmentId(appendix: Iterable[Segment]): Long =
    appendix.foldLeft(0L) {
      case (initialId, segment) =>
        val segmentId = segment.path.fileId.get._1
        if (initialId > segmentId)
          initialId
        else
          segmentId
    }

  def deleteUncommittedSegments(dirs: Seq[Dir], appendixSegments: Iterable[Segment]): IO[Unit] =
    dirs.flatMap(_.path.files(Extension.Seg)) foreachIO {
      segmentToDelete =>
        val toDelete =
          appendixSegments.foldLeft(true) {
            case (toDelete, appendixSegment) =>
              if (appendixSegment.path == segmentToDelete)
                false
              else
                toDelete
          }
        if (toDelete) {
          logger.info("SEGMENT {} not in appendix. Deleting uncommitted segment.", segmentToDelete)
          IOEffect.delete(segmentToDelete)
        } else {
          IO.unit
        }
    } getOrElse IO.unit

  def getLevels(level: LevelRef): Seq[LevelRef] = {
    @tailrec
    def getLevels(level: Option[LevelRef], levels: Seq[LevelRef]): Seq[LevelRef] =
      level match {
        case Some(level) =>
          getLevels(level.nextLevel, levels :+ level)

        case None =>
          levels
      }

    getLevels(Some(level), Seq.empty)
  }

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
                          val compressDuplicateValues: Boolean,
                          val deleteSegmentsEventually: Boolean,
                          val paths: PathsDistributor,
                          val removeDeletedRecords: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                             functionStore: FunctionStore,
                                                             ec: ExecutionContext,
                                                             removeWriter: MapEntryWriter[MapEntry.Remove[Slice[Byte]]],
                                                             addWriter: MapEntryWriter[MapEntry.Put[Slice[Byte], Segment]],
                                                             keyValueLimiter: KeyValueLimiter,
                                                             fileOpenLimiter: FileLimiter,
                                                             groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                             val segmentIDGenerator: IDGenerator) extends LevelRef with LazyLogging { self =>

  logger.info(s"{}: Level started.", paths)

  private implicit val currentWalker =
    new CurrentWalker {
      override def get(key: Slice[Byte]): IO.Async[Option[ReadOnly.Put]] =
        self get key

      override def higher(key: Slice[Byte]): IO[Option[ReadOnly.SegmentResponse]] =
        higherInThisLevel(key)

      override def lower(key: Slice[Byte]): IO[Option[ReadOnly.SegmentResponse]] =
        self lowerInThisLevel key

      override def levelNumber: String =
        "Level: " + self.levelNumber
    }

  private implicit val nextWalker =
    new NextWalker {
      override def higher(key: Slice[Byte]): IO.Async[Option[ReadOnly.Put]] =
        higherInNextLevel(key)

      override def lower(key: Slice[Byte]): IO.Async[Option[ReadOnly.Put]] =
        lowerFromNextLevel(key)

      override def get(key: Slice[Byte]): IO.Async[Option[ReadOnly.Put]] =
        getFromNextLevel(key)

      override def hasStateChanged(previousState: Long): Boolean =
        appendix.stateID != previousState

      override def stateID: Long =
        appendix.stateID

      override def levelNumber: String =
        "Level: " + self.levelNumber
    }

  private implicit val currentGetter =
    new CurrentGetter {
      override def get(key: Slice[Byte]): IO[Option[ReadOnly.SegmentResponse]] =
        getFromThisLevel(key)
    }

  def rootPath: Path =
    dirs.head.path

  def appendixPath: Path =
    rootPath.resolve("appendix")

  def releaseLocks: IO[Unit] =
    IOEffect.release(lock) flatMap {
      _ =>
        nextLevel.map(_.releaseLocks) getOrElse IO.unit
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

  def put(segment: Segment, busySegments: Iterable[Segment]): IO[Unit] =
    put(Seq(segment), busySegments)

  def put(segments: Iterable[Segment], busySegments: Iterable[Segment]): IO[Unit] = {
    logger.trace(s"{}: Putting segments '{}' segments.", paths.head, segments.map(_.path.toString).toList)
    //check to ensure that the Segments do not overlap with busy Segments
    val appendixSegments = appendix.values().asScala
    Segment.overlapsWithBusySegments(segments, busySegments, appendixSegments) flatMap {
      overlaps =>
        if (overlaps) {
          logger.debug("{}: Segments '{}' intersect with current busy segments: {}", paths.head, segments.map(_.path.toString), busySegments.map(_.path.toString))
          IO.Failure.overlappingPushSegments
        } else { //only copy Segments if the both this Level and the Segments are persistent.
          val (segmentToMerge, segmentToCopy) = Segment.partitionOverlapping(segments, appendixSegments)
          put(
            segmentsToMerge = segmentToMerge,
            segmentsToCopy = segmentToCopy,
            busySegments = busySegments,
            targetSegments = appendixSegments,
          )
        }
    }
  }

  private def deleteCopiedSegments(copiedSegments: Iterable[Segment]) =
    if (deleteSegmentsEventually)
      copiedSegments foreach (_.deleteSegmentsEventually)
    else
      copiedSegments foreach {
        segmentToDelete =>
          segmentToDelete.delete onFailureSideEffect {
            failure =>
              logger.error(s"{}: Failed to delete copied Segment '{}'", paths.head, segmentToDelete.path, failure)
          }
      }

  private[level] def put(segmentsToMerge: Iterable[Segment],
                         segmentsToCopy: Iterable[Segment],
                         targetSegments: Iterable[Segment],
                         busySegments: Iterable[Segment]): IO[Unit] =
    if (segmentsToCopy.nonEmpty)
      copy(segmentsToCopy) flatMap {
        copiedSegments =>
          buildNewMapEntry(copiedSegments, None, None) flatMap {
            copiedSegmentsEntry =>
              val putResult: IO[Unit] =
                if (segmentsToMerge.nonEmpty)
                  merge(
                    segments = segmentsToMerge,
                    targetSegments = targetSegments,
                    busySegments = busySegments,
                    appendEntry = Some(copiedSegmentsEntry)
                  )
                else
                  appendix.write(copiedSegmentsEntry) map (_ => ())

              putResult onFailureSideEffect {
                failure =>
                  logFailure(s"${paths.head}: Failed to create a log entry. Deleting ${copiedSegments.size} copied segments", failure)
                  deleteCopiedSegments(copiedSegments)
              }
          }
      }
    else
      merge(
        segments = segmentsToMerge,
        targetSegments = targetSegments,
        busySegments = busySegments,
        appendEntry = None
      )

  def putMap(map: Map[Slice[Byte], Memory.SegmentResponse], busySegments: Iterable[Segment]): IO[Unit] = {
    logger.trace("{}: PutMap '{}' Maps.", paths.head, map.count())
    //do an initial check to ensure that the Segments do not overlap with busy Segments
    val appendixValues = appendix.values().asScala
    Segment.overlapsWithBusySegments(map, busySegments, appendixValues) flatMap {
      overlapsWithBusySegments =>
        if (overlapsWithBusySegments) {
          logger.debug("{}: Map '{}' contains key-values intersect with current busy segments: {}", paths.head, map.pathOption.map(_.toString), busySegments.map(_.path.toString))
          IO.Failure.overlappingPushSegments
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
                        .map(_ => ())
                  } onFailureSideEffect {
                    failure =>
                      logFailure(s"${paths.head}: Failed to create a log entry.", failure)
                      deleteCopiedSegments(newSegments)
                  }
                else
                  IO.unit
            }
          else
            putKeyValues(
              keyValues = Slice(map.values().toArray(new Array[Memory](map.skipList.size()))),
              targetSegments = appendixValues,
              busySegments = busySegments,
              appendEntry = None
            )
        }
    }
  }

  private[level] def copy(map: Map[Slice[Byte], Memory.SegmentResponse]): IO[Iterable[Segment]] = {
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

  private[level] def copy(segmentsToCopy: Iterable[Segment]): IO[Iterable[Segment]] = {
    logger.trace(s"{}: Copying {} Segments", paths.head, segmentsToCopy.map(_.path.toString))
    segmentsToCopy.flatMapIO[Segment](
      ioBlock =
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
          logFailure(s"${paths.head}: Failed to copy Segments. Deleting partially copied Segments ${segments.size} Segments", failure)
          segments foreach {
            segment =>
              segment.delete onFailureSideEffect {
                failure =>
                  logger.error(s"{}: Failed to delete copied Segment '{}'", paths.head, segment.path, failure)
              }
          }
        }
    )
  }

  def refresh(segment: Segment): IO[Unit] = {
    logger.debug("{}: Running refresh.", paths.head)
    segment.refresh(
      minSegmentSize = segmentSize,
      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
      compressDuplicateValues = compressDuplicateValues,
      targetPaths = paths
    ) flatMap {
      newSegments =>
        logger.debug(s"{}: Segment {} successfully refreshed. New Segments: {}.", paths.head, segment.path, newSegments.map(_.path).mkString(", "))
        buildNewMapEntry(newSegments, Some(segment), None) flatMap {
          entry =>
            appendix.write(entry).map(_ => ()) onSuccessSideEffect {
              _ =>
                if (deleteSegmentsEventually)
                  segment.deleteSegmentsEventually
                else
                  segment.delete onFailureSideEffect {
                    failure =>
                      logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segment.path, failure)
                  }
            }
        } onFailureSideEffect {
          _ =>
            newSegments foreach {
              segment =>
                segment.delete onFailureSideEffect {
                  failure =>
                    logger.error(s"{}: Failed to delete Segment {}", paths.head, segment.path, failure)
                }
            }
        }
    }
  }

  def getSegmentWithExpiredKeyValues(): Option[Segment] =
    Segment.getNearestDeadlineSegment(appendix.values().asScala)

  def removeSegments(segments: Iterable[Segment]): IO[Int] = {
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
            if (deleteSegmentsEventually) {
              segmentsToRemove foreach (_.deleteSegmentsEventually)
              IO.zero
            }
            else
              Segment.deleteSegments(segmentsToRemove) recoverWith {
                case exception =>
                  logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segmentsToRemove.map(_.path.toString).mkString(", "), exception)
                  IO.zero
              }
        }
    } getOrElse IO.Failure(IO.Error.NoSegmentsRemoved)
  }

  /**
    * A Segment is considered small if it's size is less than 60% of the default [[segmentSize]]
    */
  def isSmallSegment(segment: Segment): Boolean =
    segment.segmentSize < segmentSize * 0.60

  def collapse(segments: Iterable[Segment],
               busySegments: Iterable[Segment]): IO[Int] = {
    logger.trace(s"{}: Collapsing '{}' segments", paths.head, segments.size)
    if (segments.isEmpty) {
      IO.zero
    } else if (appendix.size == 1) { //if there is only one Segment in this Level which is a small segment. No collapse required
      IO.zero
    } else {
      //other segments in the appendix that are not the input segments (segments to collapse).
      val targetAppendixSegments = appendix.values().asScala.filterNot(map => segments.exists(_.path == map.path))
      val (segmentsToMerge, targetSegments) =
        if (targetAppendixSegments.nonEmpty) {
          logger.trace(s"{}: Target appendix segments {}", paths.head, targetAppendixSegments.size)
          (segments, targetAppendixSegments)
        } else {
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
      merge(
        segments = segmentsToMerge,
        targetSegments = targetSegments,
        busySegments = busySegments,
        appendEntry = appendEntry
      ) map {
        _ =>
          //delete the segments merged with self.
          if (deleteSegmentsEventually)
            segmentsToMerge foreach (_.deleteSegmentsEventually)
          else
            segmentsToMerge foreach {
              segment =>
                segment.delete onFailureSideEffect {
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
                    busySegments: Iterable[Segment],
                    appendEntry: Option[MapEntry[Slice[Byte], Segment]]): IO[Unit] = {
    logger.trace(s"{}: Merging segments {}", paths.head, segments.map(_.path.toString))
    Segment.getAllKeyValues(bloomFilterFalsePositiveRate, segments) flatMap {
      keyValues =>
        putKeyValues(
          keyValues = keyValues,
          targetSegments = targetSegments,
          busySegments = busySegments,
          appendEntry = appendEntry
        )
    }
  }

  private def putKeyValues(keyValues: Slice[KeyValue.ReadOnly],
                           targetSegments: Iterable[Segment],
                           busySegments: Iterable[Segment],
                           appendEntry: Option[MapEntry[Slice[Byte], Segment]]): IO[Unit] = {
    logger.trace(s"{}: Merging {} KeyValues.", paths.head, keyValues.size)
    SegmentAssigner.assign(keyValues, targetSegments) flatMap {
      assignments =>
        //check to ensure that assigned Segments do not overlap with busy Segments.
        if (Segment.intersects(assignments.keys, busySegments)) {
          logger.trace(s"{}: Assigned segments {} intersect with current busy segments: {}.", paths.head, assignments.map(_._1.path.toString), busySegments.map(_.path.toString))
          IO.Failure.overlappingPushSegments
        } else {
          logger.trace(s"{}: Assigned segments {} for {} KeyValues.", paths.head, assignments.map(_._1.path.toString), keyValues.size)
          if (assignments.isEmpty) {
            logger.error(s"{}: Assigned segments are empty. Cannot merge Segments to empty target Segments: {}.", paths.head, keyValues.size)
            IO.Failure(IO.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(keyValues.size))
          } else {
            logger.debug(s"{}: Assigned segments {}. Merging {} KeyValues.", paths.head, assignments.map(_._1.path.toString), keyValues.size)
            putAssignedKeyValues(assignments) flatMap {
              targetSegmentAndNewSegments =>
                targetSegmentAndNewSegments.foldLeftIO(Option.empty[MapEntry[Slice[Byte], Segment]]) {
                  case (mapEntry, (targetSegment, newSegments)) =>
                    buildNewMapEntry(newSegments, Some(targetSegment), mapEntry).map(Some(_))
                } flatMap {
                  case Some(mapEntry) =>
                    //also write appendEntry to this mapEntry before committing entries to appendix.
                    //Note: appendEntry should not overwrite new Segment's entries with same keys so perform distinct
                    //which will remove oldEntries with duplicates with newer keys.
                    val mapEntryToWrite = appendEntry.map(appendEntry => MapEntry.distinct(mapEntry, appendEntry)) getOrElse mapEntry
                    (appendix write mapEntryToWrite) map {
                      _ =>
                        logger.debug(s"{}: putKeyValues successful. Deleting assigned Segments. {}.", paths.head, assignments.map(_._1.path.toString))
                        //delete assigned segments as they are replaced with new segments.
                        if (deleteSegmentsEventually)
                          assignments foreach (_._1.deleteSegmentsEventually)
                        else
                          assignments foreach {
                            case (segment, _) =>
                              segment.delete onFailureSideEffect {
                                exception =>
                                  logger.error(s"{}: Failed to delete Segment {}", paths.head, segment.path, exception)
                              }
                          }
                    }

                  case None =>
                    IO.Failure(new Exception(s"${paths.head}: Failed to create map entry"))

                } onFailureSideEffect {
                  failure =>
                    logFailure(s"${paths.head}: Failed to write key-values. Reverting", failure)
                    targetSegmentAndNewSegments foreach {
                      case (_, newSegments) =>
                        newSegments foreach {
                          segment =>
                            segment.delete onFailureSideEffect {
                              exception =>
                                logger.error(s"{}: Failed to delete Segment {}", paths.head, segment.path, exception)
                            }
                        }
                    }
                }
            }
          }
        }
    }
  }

  private def putAssignedKeyValues(assignedSegments: mutable.Map[Segment, Slice[KeyValue.ReadOnly]]): IO[Slice[(Segment, Slice[Segment])]] =
    assignedSegments.mapIO[(Segment, Slice[Segment])](
      ioBlock = {
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
          logFailure(s"${paths.head}: Failed to do putAssignedKeyValues, Reverting and deleting written ${targetSegmentAndNewSegments.size} segments", failure)
          targetSegmentAndNewSegments foreach {
            case (_, newSegments) =>
              newSegments foreach {
                segment =>
                  segment.delete onFailureSideEffect {
                    exception =>
                      logger.error(s"{}: Failed to delete Segment '{}' in recovery for putAssignedKeyValues", paths.head, segment.path, exception)
                  }
              }

          }
        }
    )

  def buildNewMapEntry(newSegments: Iterable[Segment],
                       originalSegmentMayBe: Option[Segment] = None,
                       initialMapEntry: Option[MapEntry[Slice[Byte], Segment]]): IO[MapEntry[Slice[Byte], Segment]] = {

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
        IO.Success(value)
      case None =>
        IO.Failure(new Exception("Failed to build map entry"))
    }
  }

  def getFromThisLevel(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]] =
    appendix.floor(key) match {
      case Some(segment) =>
        segment get key

      case None =>
        IO.none
    }

  def getFromNextLevel(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]] =
    nextLevel.map(_.get(key)) getOrElse IO.none

  override def get(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]] =
    Get(key)

  private def mightContainInThisLevel(key: Slice[Byte]): IO[Boolean] =
    appendix.floor(key) match {
      case Some(segment) =>
        segment mightContain key

      case None =>
        IO.`false`
    }

  override def mightContain(key: Slice[Byte]): IO[Boolean] =
    mightContainInThisLevel(key) flatMap {
      yes =>
        if (yes)
          IO.Success(yes)
        else
          nextLevel.map(_.mightContain(key)) getOrElse IO.Success(yes)
    }

  private def lowerInThisLevel(key: Slice[Byte]): IO[Option[ReadOnly.SegmentResponse]] =
    appendix.lowerValue(key).map(_.lower(key)) getOrElse IO.none

  private def lowerFromNextLevel(key: Slice[Byte]): IO.Async[Option[ReadOnly.Put]] =
    nextLevel.map(_.lower(key)) getOrElse IO.none

  override def floor(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]] =
    get(key) match {
      case success @ IO.Success(Some(_)) =>
        success

      case later: IO.Later[_] =>
        later

      case IO.Success(None) =>
        lower(key)

      case failed @ IO.Failure(_) =>
        failed
    }

  override def lower(key: Slice[Byte]): IO.Async[Option[ReadOnly.Put]] =
    Lower(
      key = key,
      currentSeek = Seek.Read,
      nextSeek = Seek.Read
    )

  private def higherFromFloorSegment(key: Slice[Byte]): IO[Option[ReadOnly.SegmentResponse]] =
    appendix.floor(key).map(_.higher(key)) getOrElse IO.none

  private def higherFromHigherSegment(key: Slice[Byte]): IO[Option[ReadOnly.SegmentResponse]] =
    appendix.higherValue(key).map(_.higher(key)) getOrElse IO.none

  private[core] def higherInThisLevel(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]] =
    higherFromFloorSegment(key) flatMap {
      fromFloor =>
        if (fromFloor.isDefined)
          IO.Success(fromFloor)
        else
          higherFromHigherSegment(key)
    }

  private def higherInNextLevel(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]] =
    nextLevel.map(_.higher(key)) getOrElse IO.none

  def ceiling(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]] =
    get(key) match {
      case success @ IO.Success(Some(_)) =>
        success

      case later: IO.Later[_] =>
        later

      case IO.Success(None) =>
        higher(key)

      case failed @ IO.Failure(_) =>
        failed
    }

  override def higher(key: Slice[Byte]): IO.Async[Option[KeyValue.ReadOnly.Put]] =
    Higher(
      key = key,
      currentSeek = Seek.Read,
      nextSeek = Seek.Read
    )

  /**
    * Does a quick appendix lookup.
    * It does not check if the returned key is removed. Use [[Level.head]] instead.
    */
  override def headKey: IO.Async[Option[Slice[Byte]]] =
    nextLevel.map(_.headKey) getOrElse IO.none mapAsync {
      nextLevelFirstKey =>
        MinMax.min(appendix.firstKey, nextLevelFirstKey)(keyOrder)
    }

  /**
    * Does a quick appendix lookup.
    * It does not check if the returned key is removed. Use [[Level.last]] instead.
    */
  override def lastKey: IO.Async[Option[Slice[Byte]]] =
    nextLevel.map(_.lastKey) getOrElse IO.none mapAsync {
      nextLevelLastKey =>
        MinMax.max(appendix.lastValue().map(_.maxKey.maxKey), nextLevelLastKey)(keyOrder)
    }

  override def head: IO.Async[Option[KeyValue.ReadOnly.Put]] =
    headKey match {
      case IO.Success(firstKey) =>
        firstKey.map(ceiling) getOrElse IO.none

      case later @ IO.Later(_, _) =>
        later flatMap {
          firstKey =>
            firstKey.map(ceiling) getOrElse IO.none
        }

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  override def last =
    lastKey match {
      case IO.Success(lastKey) =>
        lastKey.map(floor) getOrElse IO.none

      case later @ IO.Later(_, _) =>
        later flatMap {
          lastKey =>
            lastKey.map(floor) getOrElse IO.none
        }

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    appendix contains minKey

  override def bloomFilterKeyValueCount: IO[Int] =
    appendix.foldLeft(IO(0)) {
      case (currentTotal, (_, segment)) =>
        segment.getBloomFilterKeyValueCount() flatMap {
          segmentSize =>
            currentTotal.map(_ + segmentSize)
        }
    } flatMap {
      thisLevelCount =>
        nextLevel.map(_.bloomFilterKeyValueCount).getOrElse(IO.zero) map (_ + thisLevelCount)
    }

  def getSegment(minKey: Slice[Byte]): Option[Segment] =
    appendix.get(minKey)(keyOrder)

  def lowerSegment(key: Slice[Byte]): Option[Segment] =
    (appendix lower key).map(_._2)

  def lowerSegmentMinKey(key: Slice[Byte]): Option[Slice[Byte]] =
    lowerSegment(key).map(_.minKey)

  def higherSegment(key: Slice[Byte]): Option[Segment] =
    (appendix higher key).map(_._2)

  override def segmentsCount(): Int =
    appendix.count()

  override def take(count: Int): Slice[Segment] =
    appendix take count

  def isEmpty: Boolean =
    appendix.isEmpty

  def segmentFilesOnDisk: Seq[Path] =
    IOEffect.segmentFilesOnDisk(dirs.map(_.path))

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

  def close: IO[Unit] =
    (nextLevel.map(_.close) getOrElse IO.unit) flatMap {
      _ =>
        appendix.close() onFailureSideEffect {
          failure =>
            logger.error("{}: Failed to close appendix", paths.head, failure)
        }
        closeSegments() onFailureSideEffect {
          failure =>
            logger.error("{}: Failed to close segments", paths.head, failure)
        }
        releaseLocks onFailureSideEffect {
          failure =>
            logger.error("{}: Failed to release locks", paths.head, failure)
        }
    }

  def closeSegments(): IO[Unit] = {
    segmentsInLevel().foreachIO(_.close, failFast = false) foreach {
      failure =>
        logger.error("{}: Failed to close Segment file.", paths.head, failure.exception)
    }

    nextLevel.map(_.closeSegments()) getOrElse IO.unit
  }

  override val isTrash: Boolean =
    false
}
