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
import swaydb.core.data.ValueType.{Add, Remove}
import swaydb.core.data.{PersistentReadOnly, _}
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.level.LevelException.NoNextLevel
import swaydb.core.level.actor.{LevelAPI, LevelActor, LevelCommand}
import swaydb.core.map.serializer.AppendixSerializer
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.retry.Retry
import swaydb.core.segment.SegmentException.SegmentFileMissing
import swaydb.core.segment.{Segment, SegmentAssigner, SegmentMerge}
import swaydb.core.util.ExceptionUtil._
import swaydb.core.util.FileUtil._
import swaydb.core.util.PipeOps._
import swaydb.core.util.TryUtil._
import swaydb.core.util.{MinMax, _}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.Dir
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._
import swaydb.data.storage.AppendixStorage.Persistent
import swaydb.data.storage.{AppendixStorage, LevelStorage}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

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
      Success(None)

  def apply(segmentSize: Long,
            bloomFilterFalsePositiveRate: Double,
            cacheKeysOnCreate: Boolean,
            levelStorage: LevelStorage,
            appendixStorage: AppendixStorage,
            readRetryLimit: Int,
            nextLevel: Option[LevelRef],
            pushForward: Boolean = false,
            throttle: LevelMeter => Throttle)(implicit ordering: Ordering[Slice[Byte]],
                                              ec: ExecutionContext,
                                              keyValueLimiter: (PersistentReadOnly, Segment) => Unit,
                                              fileOpenLimited: DBFile => Unit): Try[LevelRef] = {
    //acquire lock on folder
    acquireLock(levelStorage) flatMap {
      lock =>
        //lock acquired. Initialising Level.
        implicit val serializer: AppendixSerializer =
          AppendixSerializer(
            removeDeletedRecords = removeDeletes(nextLevel),
            mmapSegmentsOnRead = levelStorage.mmapSegmentsOnWrite,
            mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnRead,
            cacheKeysOnCreate = cacheKeysOnCreate
          )

        //initialise appendix
        val appendix =
          appendixStorage match {
            case Persistent(mmap, appendixFlushCheckpointSize) =>
              logger.info("{}: Initialising appendix.", levelStorage.dir)
              val appendixFolder = levelStorage.dir.resolve("appendix")
              //check if appendix folder/file was deleted.
              if ((!IO.exists(appendixFolder) || appendixFolder.files(Extension.Log).isEmpty) && FileUtil.segmentFilesOnDisk(levelStorage.dirs.pathsSet.toSeq).nonEmpty) {
                logger.info("{}: Failed to start Level. Appendix file is missing", appendixFolder)
                Failure(new IllegalStateException(s"Failed to start Level. Appendix file is missing '$appendixFolder'."))
              } else {
                IO createDirectoriesIfAbsent appendixFolder
                Map.persistent(appendixFolder, mmap, flushOnOverflow = true, appendixFlushCheckpointSize, dropCorruptedTailEntries = false).map(_.item)
              }

            case AppendixStorage.Memory =>
              logger.info("{}: Initialising appendix for in-memory Level", levelStorage.dir)
              Try(Map.memory())
          }

        appendix flatMap {
          (appendix: Map[Slice[Byte], Segment]) =>
            logger.debug("{}: Checking Segments exist.", levelStorage.dir)
            //check that all existing Segments in appendix also exists on disk or else return error message.
            appendix.asScala.tryForeach {
              case (_, segment) =>
                if (segment.existsOnDisk)
                  Success()
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
                    cacheKeysOnCreate = cacheKeysOnCreate,
                    readRetryLimit = readRetryLimit,
                    pushForward = pushForward,
                    mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnWrite,
                    mmapSegmentsOnRead = levelStorage.mmapSegmentsOnRead,
                    inMemory = levelStorage.memory,
                    segmentSize = segmentSize,
                    appendix = appendix,
                    throttle = throttle,
                    nextLevel = nextLevel,
                    lock = lock
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
                          cacheKeysOnCreate: Boolean,
                          readRetryLimit: Int,
                          val mmapSegmentsOnWrite: Boolean,
                          val mmapSegmentsOnRead: Boolean,
                          val inMemory: Boolean,
                          val segmentSize: Long,
                          val pushForward: Boolean,
                          val throttle: LevelMeter => Throttle,
                          val nextLevel: Option[LevelRef],
                          appendix: Map[Slice[Byte], Segment],
                          lock: Option[FileLock])(implicit ordering: Ordering[Slice[Byte]],
                                                  ec: ExecutionContext,
                                                  serializer: AppendixSerializer,
                                                  keyValueLimiter: (PersistentReadOnly, Segment) => Unit,
                                                  fileOpenLimited: DBFile => Unit) extends LevelRef with LazyLogging {

  val paths: PathsDistributor = PathsDistributor(dirs, () => appendix.values().asScala)

  import swaydb.core.util.TryUtil._

  logger.info(s"{}: Level started.", paths)

  implicit val orderOnReadOnly = ordering.on[PersistentReadOnly](_.key)

  val removeDeletedRecords = Level.removeDeletes(nextLevel)

  def rootPath: Path =
    dirs.head.path

  def appendixPath: Path =
    rootPath.resolve("appendix")

  def releaseLocks: Try[Unit] =
    Try(lock.foreach(_.release())) flatMap {
      _ =>
        nextLevel.map(_.releaseLocks) getOrElse Success()
    }

  private def deleteOrphanSegments(): Unit =
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
          logger.info("SEGMENT {} not in appendix. Deleting orphan segment.", segmentToDelete)
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

  private implicit val segmentIDGenerator = IDGenerator(initial = largestSegmentId)

  def init: Level = {
    if (existsOnDisk) deleteOrphanSegments()
    this
  }

  private val actor =
    LevelActor(ec, this, ordering)

  override def !(request: LevelAPI): Unit =
    actor ! request

  override def forward(command: LevelAPI): Try[Unit] =
    nextLevel map {
      nextLevel =>
        if (pushForward && isEmpty && nextLevel.isEmpty) {
          logger.debug("{}: Push forwarded.", paths)
          nextLevel ! command
          Success()
        }
        else
          Failure(LevelException.NotSentToNextLevel)
    } getOrElse Failure(LevelException.NotSentToNextLevel)

  override def push(command: LevelAPI): Unit =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel ! command
      case None =>
        logger.error("{}: Push submitted. But there is no lower level", paths.head, NoNextLevel)
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
    if (Segment.overlapsWithBusySegments(segments, busySegments, appendix.values().asScala)) {
      logger.debug("{}: Segments '{}' intersect with current busy segments: {}", paths.head, segments.map(_.path.toString), busySegments.map(_.path.toString))
      Failure(LevelException.ContainsOverlappingBusySegments)
    }
    //only copy Segments if the both this Level and the Segments are persistent.
    else if (!inMemory && segments.headOption.exists(_.persistent) && isEmpty)
      copy(segments)
    else
      merge(segments, appendix.values().asScala, None)
  }

  def putMap(map: Map[Slice[Byte], (ValueType, Option[Slice[Byte]])]): Try[Unit] = {
    logger.trace("{}: PutMap '{}' Maps.", paths.head, map.count())
    //do an initial check to ensure that the Segments do not overlap with busy Segments
    val busySegs = getBusySegments()
    if (Segment.overlapsWithBusySegments(map, busySegs, appendix.values().asScala)) {
      logger.debug("{}: Map '{}' contains key-values intersect with current busy segments: {}", paths.head, map.pathOption.map(_.toString), busySegs.map(_.path.toString))
      Failure(LevelException.ContainsOverlappingBusySegments)
    }
    else
      map.foldLeft(Slice.create[KeyValue](map.count())) {
        case (slice, (key, (valueType, value))) =>
          valueType match {
            case Add =>
              slice add Transient.Create(key, value, bloomFilterFalsePositiveRate, slice.lastOption)
            case Remove =>
              slice add Transient.Delete(key, bloomFilterFalsePositiveRate, slice.lastOption)
          }
      } ==> (putKeyValues(_, appendix.values().asScala, None))
  }

  def putKeyValues(keyValues: Slice[KeyValue]): Try[Unit] = {
    logger.trace(s"{}: Received put for '{}' KeyValues.", paths.head, keyValues.size)
    putKeyValues(keyValues, appendix.values().asScala, None)
  }

  def collapseAllSmallSegments(batch: Int): Try[Int] = {
    logger.trace("{}: Running collapseAllSmallSegments batch: '{}'.", paths.head, batch)
    collapseSegments(batch, _.segmentSize < segmentSize)
  }

  def collapseSegments(count: Int, condition: Segment => Boolean): Try[Int] = {
    @tailrec
    def run(): Try[Int] = {
      val smallSegments = takeSegments(count, condition)
      val busySegments = getBusySegments()
      if (smallSegments.isEmpty) {
        Success(0)
      } else if (Segment.intersects(smallSegments, busySegments)) {
        logger.debug(s"{}: Collapsing segments {} intersect with current busy segments: {}.", paths.head, smallSegments.map(_.path.toString), busySegments.map(_.path.toString))
        Failure(LevelException.ContainsOverlappingBusySegments)
      } else
        collapse(smallSegments, appendix.values().asScala) match {
          case success @ Success(value) if value == 0 =>
            success

          case Success(_) =>
            run()

          case Failure(exception) =>
            Failure(exception)
        }
    }

    run()
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

      //create an initialEntry that will remove smaller segments from the appendix.
      //this entry will get applied only if the putSegments is successful orElse this entry will be discarded.
      val initialEntry =
      segmentsToMerge.foldLeft(Option.empty[MapEntry[Slice[Byte], Segment]]) {
        case (mapEntry, smallMap) =>
          mapEntry.map(_ - smallMap.minKey) orElse Some(MapEntry.Remove(smallMap.minKey))
      }
      merge(segments, targetSegments, initialEntry) map {
        _ =>
          //delete the segments merged with self.
          segmentsToMerge foreach {
            segment =>
              segment.delete.failed.map {
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
                    initialEntry: Option[MapEntry[Slice[Byte], Segment]]): Try[Unit] = {
    logger.trace(s"{}: Merging segments {}", paths.head, segments.map(_.path.toString))
    Segment.getAllKeyValues(bloomFilterFalsePositiveRate, segments) flatMap {
      putKeyValues(_, targetSegments, initialEntry)
    }
  }

  private def copy(segmentsToCopy: Iterable[Segment]): Try[Unit] = {
    logger.trace(s"{}: Copying {} Segments", paths.head, segmentsToCopy.map(_.path.toString))
    segmentsToCopy.tryMap[Segment](
      tryBlock =
        segment => {
          val targetSegmentPath = paths.next.resolve(IDGenerator.segmentId(segmentIDGenerator.nextID))
          segment.copyTo(targetSegmentPath) flatMap {
            _ =>
              Segment(
                path = targetSegmentPath,
                cacheKeysOnCreate = cacheKeysOnCreate,
                mmapReads = mmapSegmentsOnRead,
                mmapWrites = mmapSegmentsOnWrite,
                minKey = segment.minKey,
                maxKey = segment.maxKey,
                segmentSize = segment.segmentSize,
                removeDeletes = removeDeletedRecords
              )
          }
        },
      recover =
        (copiedSegment, failure) => {
          logFailure(s"${paths.head}: Failed to copy ${segmentsToCopy.size} segment(s). Deleting ${copiedSegment.size} copied segment(s)", failure.exception)

          copiedSegment foreach {
            segment =>
              segment.delete.failed.map {
                exception =>
                  logger.warn(s"{}: Failed to delete partially copied Segment {} after failure to copy", paths.head, segment.path, exception)
              }
          }
        }
    ) flatMap {
      copiedSegment =>
        buildNewMapEntry(copiedSegment, None, None)
          .flatMap(appendix.write)
          .map(_ => alertActorOfSmallSegments(copiedSegment))
          .recoverWith {
            case exception =>
              logFailure(s"${paths.head}: Failed to create a log entry. Deleting ${copiedSegment.size} copied segments", exception)

              copiedSegment foreach {
                segmentToDelete =>
                  segmentToDelete.delete.failed foreach {
                    exception =>
                      logger.error(s"{}: Failed to delete Segment '{}'", paths.head, segmentToDelete.path, exception)
                  }
              }
              Failure(exception)
          }
    }
  }

  private def putKeyValues(keyValues: Slice[KeyValue],
                           targetSegments: Iterable[Segment],
                           initialEntry: Option[MapEntry[Slice[Byte], Segment]]): Try[Unit] = {
    logger.trace(s"{}: Merging {} KeyValues.", paths.head, keyValues.size)
    val assignments = SegmentAssigner.assign(keyValues, targetSegments)
    val busySegments: Seq[Segment] = getBusySegments()
    //check to ensure that assigned Segments do not overlap with busy Segments.
    if (Segment.intersects(assignments.keys, busySegments)) {
      logger.trace(s"{}: Assigned segments {} intersect with current busy segments: {}.", paths.head, assignments.map(_._1.path.toString), busySegments.map(_.path.toString))
      Failure(LevelException.ContainsOverlappingBusySegments)
    } else {
      logger.trace(s"{}: Assigned segments {} for KeyValues: {}.", paths.head, assignments.map(_._1.path.toString), keyValues.size)
      if (assignments.isEmpty) {
        logger.debug(s"{}: Assigned segments are empty. Adding a new segment for KeyValues: {}.", paths.head, keyValues.size)
        addAsNewSegments(keyValues, initialEntry)
      } else {
        logger.debug(s"{}: Assigned segments {}. Merging KeyValues: {}.", paths.head, assignments.map(_._1.path.toString), keyValues.size)
        putAssignedKeyValues(assignments) flatMap {
          targetSegmentAndNewSegments =>
            targetSegmentAndNewSegments.tryFoldLeft(initialEntry) {
              case (mapEntry, (targetSegment, newSegments)) =>
                buildNewMapEntry(newSegments, Some(targetSegment), mapEntry).map(Some(_))
            } flatMap {
              case Some(mapEntry) =>
                (appendix write mapEntry) map {
                  _ =>
                    logger.debug(s"{}: putKeyValues successful. Deleting assigned Segments. {}.", paths.head, assignments.map(_._1.path.toString))
                    //delete assigned segments as they are replaced with new segments.
                    assignments.foreach {
                      case (segment, _) =>
                        segment.delete.failed.map {
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
                    newSegments.foreach {
                      segment =>
                        segment.delete.failed.map {
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

  private def addAsNewSegments(keyValues: Slice[KeyValue],
                               initialEntry: Option[MapEntry[Slice[Byte], Segment]]): Try[Unit] = {
    logger.debug(s"{}: In addToNewSegments {} KeyValues.", paths.head, keyValues.size)

    SegmentMerge.split(keyValues, segmentSize, removeDeletedRecords, inMemory, bloomFilterFalsePositiveRate).tryMap[Segment](
      tryBlock =
        keyValues =>
          if (inMemory)
            Segment.memory(
              path = paths.next.resolve(segmentIDGenerator.nextSegmentID),
              bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
              keyValues = keyValues,
              removeDeletes = removeDeletedRecords
            )
          else
            Segment.persistent(
              path = paths.next.resolve(segmentIDGenerator.nextSegmentID),
              bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
              cacheKeysOnCreate = cacheKeysOnCreate,
              mmapReads = mmapSegmentsOnRead,
              mmapWrites = mmapSegmentsOnWrite,
              keyValues = keyValues,
              removeDeletes = removeDeletedRecords
            ),

      recover =
        (segments, failure) => {
          logFailure(s"${paths.head}: Failed to complete put request. Deleting partially written batch ${segments.size} IndexSegments", failure.exception)
          segments foreach {
            segment =>
              segment.delete.failed foreach {
                exception =>
                  logger.error(s"{}: Failed to delete Segment '{}' in recovery addAsNewSegments", paths.head, segment.path, exception)
              }
          }
        }
    ) flatMap {
      newSegments =>
        if (newSegments.isEmpty)
          Success()
        else
          buildNewMapEntry(newSegments, initialMapEntry = initialEntry)
            .flatMap {
              entry =>
                appendix.write(entry) map {
                  _ =>
                    alertActorOfSmallSegments(newSegments)
                }
            }
            .recoverWith {
              case exception =>
                logFailure(s"${paths.head}: Failed to complete put request. Deleting partially written batch ${segments.size} IndexSegments", exception)

                newSegments foreach {
                  segment =>
                    segment.delete.failed foreach {
                      exception =>
                        logger.error(s"{}: Failed to delete Segment '{}' in recovery addAsNewSegments", paths.head, segment.path, exception)
                    }
                }
                Failure(exception)
            }
    }
  }

  //if there is a small segment in the new segments, alert the Actor to collapse the small segments before next Push
  def alertActorOfSmallSegments(newSegments: Iterable[Segment]) =
    newSegments foreachBreak {
      newSegment =>
        val toAlert = newSegment.segmentSize < segmentSize
        if (toAlert) actor ! LevelCommand.CollapseSmallSegments
        toAlert
    }

  def putAssignedKeyValues(assignedSegments: mutable.Map[Segment, Slice[KeyValue]]): Try[Slice[(Segment, Slice[Segment])]] =
    assignedSegments.tryMap[(Segment, Slice[Segment])](
      tryBlock = {
        case (targetSegment, assignedKeyValues) =>
          targetSegment.put(assignedKeyValues, segmentSize, bloomFilterFalsePositiveRate, paths.addPriorityPath(targetSegment.path.getParent)) map {
            newSegments =>
              alertActorOfSmallSegments(newSegments)
              (targetSegment, newSegments)
          }
      },
      recover =
        (targetSegmentAndNewSegments, failure) => {
          logFailure(s"${paths.head}: Failed to do putAssignedKeyValues, Reverting and deleting written ${targetSegmentAndNewSegments.size} segments", failure.exception)
          targetSegmentAndNewSegments.foreach {
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

  def buildNewMapEntry(newSegments: Iterable[Segment],
                       originalSegmentMayBe: Option[Segment] = None,
                       initialMapEntry: Option[MapEntry[Slice[Byte], Segment]]): Try[MapEntry[Slice[Byte], Segment]] = {

    import ordering._

    var removeOriginalSegments = true
    val nextLogEntry =
      newSegments.foldLeft(initialMapEntry) {
        case (loggerCommand, newMap) =>
          //if one of the new segments have the same minKey as the original segment, remove is not required as 'put' will replace old key.
          if (removeOriginalSegments && originalSegmentMayBe.exists(_.minKey equiv newMap.minKey))
            removeOriginalSegments = false

          val nextLogEntry = MapEntry.Add(newMap.minKey, newMap)
          loggerCommand.map(_ ++ nextLogEntry) orElse Some(nextLogEntry)
      }
    (originalSegmentMayBe match {
      case Some(originalMap) if removeOriginalSegments =>
        val removeLogEntry = MapEntry.Remove[Slice[Byte], Segment](originalMap.minKey)
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
        segmentsToRemove.add(segmentToRemove)
        val nextEntry = MapEntry.Remove[Slice[Byte], Segment](segmentToRemove.minKey)
        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
    } map {
      mapEntry =>
        //        logger.info(s"$id. Build map entry: ${mapEntry.string(_.asInt().toString, _.id.toString)}")
        logger.trace(s"{}: Built map entry to remove Segments {}", paths.head, segments.map(_.path.toString))
        (appendix write mapEntry) flatMap {
          _ =>
            logger.debug(s"{}: MapEntry to delete Segments successful. Deleting physical Segments now: {}", paths.head, segments.map(_.path.toString))
            // If a delete fails that would be an OS issue.
            // But it's OK if it fails as long as appendix is updated with new segments. An error message will be printed
            // out asking to delete the deleted segments manually or do a database restart which will delete the orphan
            // Segments on reboot.
            Segment.deleteSegments(segmentsToRemove) match {
              case Success(_) =>
                Success(0)

              case Failure(exception) =>
                logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segmentsToRemove.map(_.path.toString).mkString(", "), exception)
                Failure(exception)
            }
        }
    } getOrElse Failure(LevelException.NoSegmentsRemoved)
  }

  def withRetry[T](tryBlock: => Try[T]): Try[T] =
    Retry[T](resourceId = paths.head.toString, maxRetryLimit = readRetryLimit, until = Retry.levelReadRetryUntil) {
      try
        tryBlock
      catch {
        case ex: Exception =>
          Failure(ex)
      }
    }

  def getFromThisLevel(key: Slice[Byte]): Try[Option[PersistentReadOnly]] =
    appendix.floor(key) match {
      case Some(segment) =>
        segment get key

      case None =>
        Success(None)
    }

  override def get(key: Slice[Byte]) =
    withRetry(getFromThisLevel(key)) flatMap {
      case result @ Some(_) =>
        Success(result)

      case None =>
        nextLevel.map(_.get(key)) getOrElse Success(None)
    }

  def mightContainInThisLevel(key: Slice[Byte]): Try[Boolean] =
    appendix.floor(key) match {
      case Some(segment) =>
        segment mightContain key

      case None =>
        Success(false)
    }

  def mightContain(key: Slice[Byte]): Try[Boolean] =
    withRetry(mightContainInThisLevel(key)) flatMap {
      yes =>
        if (yes)
          Success(yes)
        else
          nextLevel.map(_.mightContain(key)) getOrElse Success(yes)
    }

  def lowerInThisLevel(key: Slice[Byte]) =
    appendix.lowerValue(key).map(_.lower(key)) getOrElse Success(None)

  private def lowerFromNextLevel(key: Slice[Byte]) =
    nextLevel.map(_.lower(key)) getOrElse Success(None)

  override def lower(key: Slice[Byte]) =
    for {
      thisLevelLowest <- lowerInThisLevel(key)
      nextLevelLowest <- lowerFromNextLevel(key)
    } yield {
      MinMax.max(thisLevelLowest, nextLevelLowest)
    }

  private def higherFromFloorSegment(key: Slice[Byte]) =
    appendix.floor(key).map(_.higher(key)) getOrElse Success(None)

  private def higherFromHigherSegment(key: Slice[Byte]) =
    appendix.higherValue(key).map(_.higher(key)) getOrElse Success(None)

  private def higherInThisLevel(key: Slice[Byte]) =
    higherFromFloorSegment(key) flatMap {
      fromFloor =>
        if (fromFloor.isDefined)
          Success(fromFloor)
        else
          higherFromHigherSegment(key)
    }

  private def higherInNextLevel(key: Slice[Byte]) =
    nextLevel.map(_.higher(key)) getOrElse Success(None)

  override def higher(key: Slice[Byte]) =
    for {
      thisLevelHighest <- higherInThisLevel(key)
      nextLevelHighest <- higherInNextLevel(key)
    } yield {
      MinMax.min(thisLevelHighest, nextLevelHighest)
    }

  private def headInNextLevel =
    nextLevel.map(_.head) getOrElse Success(None)

  private def headInThisLevel =
    appendix.firstKey.map(get) getOrElse Success(None)

  override def head =
    for {
      thisLevelHead <- headInThisLevel
      nextLevelHead <- headInNextLevel
    } yield {
      MinMax.min(thisLevelHead, nextLevelHead)
    }

  private def lastInNextLevel: Try[Option[PersistentReadOnly]] =
    nextLevel.map(_.last) getOrElse Success(None)

  private def lastInThisLevel =
    appendix.last.map {
      case (_, lastSegment) =>
        lastSegment.get(lastSegment.maxKey)
    } getOrElse Success(None)

  override def last =
    for {
      thisLevelLast <- lastInThisLevel
      nextLevelLast <- lastInNextLevel
    } yield {
      MinMax.max(thisLevelLast, nextLevelLast)
    }

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    appendix contains minKey

  override def keyValueCount: Try[Int] =
    appendix.foldLeft(Try(0)) {
      case (currentTotal, (_, segment)) =>
        segment.getKeyValueCount() flatMap {
          segmentSize =>
            currentTotal.map(_ + segmentSize)
        }
    } flatMap {
      thisLevelSize =>
        nextLevel.map(_.keyValueCount).getOrElse(Success(0)) map (_ + thisLevelSize)
    }

  def getSegment(minKey: Slice[Byte]): Option[Segment] =
    (appendix get minKey).map(_._2)

  def lowerSegment(key: Slice[Byte]): Option[Segment] =
    (appendix lower key).map(_._2)

  def lowerSegmentMinKey(key: Slice[Byte]): Option[Slice[Byte]] =
    lowerSegment(key).map(_.minKey)

  def higherSegment(key: Slice[Byte]): Option[Segment] =
    (appendix higher key).map(_._2)

  def higherSegmentMaxKey(key: Slice[Byte]): Option[Slice[Byte]] =
    higherSegment(key).map(_.maxKey)

  def getBusySegments(): List[Segment] =
    actor.getBusySegments

  def segmentsCount(): Int =
    appendix.count()

  def take(count: Int): Slice[Segment] =
    appendix take count

  override def pickSegmentsToPush(count: Int): Iterable[Segment] = {
    if (count == 0)
      Iterable.empty
    else
      nextLevel.map(_.getBusySegments()) match {
        case Some(nextLevelsBusySegments) if nextLevelsBusySegments.nonEmpty =>
          Segment.nonOverlapping(
            segments1 = segments,
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

  def segments: Iterable[Segment] =
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

  def meterFor(levelNumber: Int): Option[LevelMeter] =
    if (levelNumber == paths.head.path.folderId)
      Some(meter)
    else
      nextLevel.flatMap(_.meterFor(levelNumber))

  override def takeSegments(size: Int, condition: Segment => Boolean): Iterable[Segment] = {
    val smallSegments = appendix.values().asScala.filter {
      segment =>
        condition(segment)
    } take size
    smallSegments
  }

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    appendix.values().asScala.filter(_.segmentSize > segmentSize) take size

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    appendix.values().asScala.filter(_.segmentSize < segmentSize) take size

  def close: Try[Unit] = {
    appendix.close().failed foreach {
      exception =>
        logger.error("{}: Failed to close appendix", paths.head, exception)
    }
    segments.tryForeach(_.close, failFast = false) match {
      case Some(failed) =>
        logger.error("{}: Failed to close file", paths.head, failed.exception)
        failed
      case None =>
        Success()
    }
  }

  override def isTrash: Boolean = false

}