/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.level

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.level.seek._
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.log.serializer._
import swaydb.core.log.{Log, LogEntry}
import swaydb.core.merge.stats.MergeStats
import swaydb.core.merge.stats.MergeStats.{Memory, Persistent}
import swaydb.core.segment._
import swaydb.core.segment.assigner.{Assignable, Assigner, Assignment, GapAggregator}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.defrag.{DefragMemorySegment, DefragPersistentSegment}
import swaydb.core.segment.io.{SegmentReadIO, SegmentWriteMemoryIO, SegmentWritePersistentIO}
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.Exceptions._
import swaydb.core.util._
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.compaction.{LevelMeter, LevelThrottle}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.SliceIOImplicits._
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.storage.LevelStorage
import swaydb.effect.Effect._
import swaydb.effect.{Dir, Effect, Extension, FileLocker}
import swaydb.utils.Futures
import swaydb.{Aggregator, Bag, Error, IO}

import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[core] case object Level extends LazyLogging {

  val emptySegmentsToPush = (Iterable.empty[Segment], Iterable.empty[Segment])

  def acquireLock(storage: LevelStorage.Persistent): IO[swaydb.Error.Level, Option[FileLocker]] =
    IO {
      Effect createDirectoriesIfAbsent storage.dir
      val lockFile = storage.dir.resolve("LOCK")
      logger.info("{}: Acquiring lock.", lockFile)
      Effect createFileIfAbsent lockFile
      val channel = FileChannel.open(lockFile, StandardOpenOption.WRITE)
      val lock = channel.tryLock()
      storage.dirs foreach {
        dir =>
          Effect createDirectoriesIfAbsent dir.path
      }
      Some(FileLocker(lock, channel))
    }

  def acquireLock(levelStorage: LevelStorage): IO[swaydb.Error.Level, Option[FileLocker]] =
    levelStorage match {
      case persistent: LevelStorage.Persistent =>
        acquireLock(persistent)

      case _: LevelStorage.Memory =>
        IO.none
    }

  def apply(bloomFilterConfig: BloomFilterBlockConfig,
            hashIndexConfig: HashIndexBlockConfig,
            binarySearchIndexConfig: BinarySearchIndexBlockConfig,
            sortedIndexConfig: SortedIndexBlockConfig,
            valuesConfig: ValuesBlockConfig,
            segmentConfig: SegmentBlockConfig,
            levelStorage: LevelStorage,
            nextLevel: Option[NextLevel],
            throttle: LevelMeter => LevelThrottle)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   functionStore: FunctionStore,
                                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                   blockSweeper: Option[MemorySweeper.Block],
                                                   fileSweeper: FileSweeper,
                                                   bufferCleaner: ByteBufferSweeperActor,
                                                   forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Level, Level] =
    acquireLock(levelStorage) flatMap { //acquire lock on folder
      lock =>
        //lock acquired.
        //initialise Segment IO for this Level.
        implicit val segmentIO: SegmentReadIO =
          SegmentReadIO(
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )

        //initialise readers & writers
        import AppendixLogEntryWriter.{AppendixPutWriter, AppendixRemoveWriter}

        val appendixReader =
          new AppendixLogEntryReader(
            mmapSegment = segmentConfig.mmap,
            segmentRefCacheLife = segmentConfig.segmentRefCacheLife
          )

        import appendixReader._

        //initialise appendix
        val appendix: IO[swaydb.Error.Level, Log[Slice[Byte], Segment, AppendixLogCache]] =
          levelStorage match {
            case LevelStorage.Persistent(_, _, mmap, appendixFlushCheckpointSize) =>
              logger.info("{}: Initialising appendix.", levelStorage.dir)
              val appendixFolder = levelStorage.dir.resolve("appendix")
              //check if appendix folder/file was deleted.
              if ((!Effect.exists(appendixFolder) || appendixFolder.files(Extension.Log).isEmpty) && Effect.segmentFilesOnDisk(levelStorage.dirs.pathsSet.toSeq).nonEmpty) {
                logger.error("{}: Failed to start Level. Appendix file is missing", appendixFolder)
                IO.failed(new IllegalStateException(s"Failed to start Level. Appendix file is missing '$appendixFolder'."))
              } else {
                IO {
                  Effect createDirectoriesIfAbsent appendixFolder
                  Log.persistent[Slice[Byte], Segment, AppendixLogCache](
                    folder = appendixFolder,
                    mmap = mmap,
                    flushOnOverflow = true,
                    fileSize = appendixFlushCheckpointSize,
                    dropCorruptedTailEntries = false
                  ).item
                }
              }

            case LevelStorage.Memory(_) =>
              logger.info("{}: Initialising appendix for in-memory Level", levelStorage.dir)
              IO(Log.memory[Slice[Byte], Segment, AppendixLogCache]())
          }

        //initialise Level
        appendix flatMap {
          appendix =>
            logger.debug("{}: Checking Segments exist.", levelStorage.dir)
            //check that all existing Segments in appendix also exists on disk or else return error message.
            appendix.cache.values() foreachIO {
              segment =>
                if (segment.existsOnDisk)
                  IO.unit
                else
                  IO.failed(swaydb.Exception.SegmentFileMissing(segment.path))
            } match {
              case Some(IO.Left(error)) =>
                IO.Left(error)

              case None =>
                val allSegments = appendix.cache.values()
                implicit val segmentIDGenerator: IDGenerator = IDGenerator(initial = largestSegmentId(allSegments))
                val paths: PathsDistributor = PathsDistributor(levelStorage.dirs, () => allSegments)

                val deletedUnCommittedSegments =
                  if (levelStorage.persistent)
                    deleteUncommittedSegments(levelStorage.dirs, appendix.cache.values())
                  else
                    IO.unit

                deletedUnCommittedSegments
                  .andThen {
                    new Level(
                      dirs = levelStorage.dirs,
                      bloomFilterConfig = bloomFilterConfig,
                      hashIndexConfig = hashIndexConfig,
                      binarySearchIndexConfig = binarySearchIndexConfig,
                      sortedIndexConfig = sortedIndexConfig,
                      valuesConfig = valuesConfig,
                      segmentConfig = segmentConfig,
                      inMemory = levelStorage.memory,
                      throttle = throttle,
                      nextLevel = nextLevel,
                      appendix = appendix,
                      lock = lock,
                      pathDistributor = paths
                    )
                  }
                  .onLeftSideEffect {
                    _ =>
                      appendix.close()
                  }
            }
        }
    }

  def removeDeletes(nextLevel: Option[LevelRef]): Boolean =
    nextLevel.isEmpty

  def largestSegmentId(appendix: Iterable[Segment]): Long =
    appendix.foldLeft(0L) {
      case (initialId, segment) =>
        val segmentNumber = segment.path.fileId._1
        if (initialId > segmentNumber)
          initialId
        else
          segmentNumber
    }

  /**
   * A Segment is considered small if it's size is less than 40% of the default [[Level.minSegmentSize]]
   */
  def isSmallSegment(segment: Segment, levelSegmentSize: Long): Boolean =
    segment.segmentSize < levelSegmentSize * 0.40

  def deleteUncommittedSegments(dirs: Seq[Dir], appendixSegments: Iterable[Segment]): IO[swaydb.Error.Level, Unit] =
    dirs
      .flatMap(_.path.files(Extension.Seg))
      .foreachIO {
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
            IO(Effect.delete(segmentToDelete))
          } else {
            IO.unit
          }
      }
      .getOrElse(IO.unit)

  def shouldCollapse(level: NextLevel,
                     segment: Segment): Boolean =
  //do not collapse segments that were created in other levels
  //see issue - //https://github.com/simerplaha/SwayDB/issues/299
    isSmallSegment(segment, level.minSegmentSize) // ||
  //if the Segment was not created in this level.
  //      segment.createdInLevel != level.levelNumber
}

/**
 * Committing data to a [[Level]] goes through the following 4 phases.
 *
 * 1. Assign - assign new key-values to existing [[Segment]]s in the Level
 * 2. Merge - process the assignments resulting in merged key-values.
 * 3. Persist - persist the merged key-values to create new [[Segment]]s (tied during merge to avoid accumulating too much memory before persisting)
 * 4. Commit - commit new [[Segment]]s replacing the old ones atomically.
 *
 * Compaction invokes these individually for maximum concurrency when performing
 * merge and controlled concurrency for write IO when persisting and committing
 * new segments.
 */
private[core] case class Level(dirs: Seq[Dir],
                               bloomFilterConfig: BloomFilterBlockConfig,
                               hashIndexConfig: HashIndexBlockConfig,
                               binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                               sortedIndexConfig: SortedIndexBlockConfig,
                               valuesConfig: ValuesBlockConfig,
                               segmentConfig: SegmentBlockConfig,
                               inMemory: Boolean,
                               throttle: LevelMeter => LevelThrottle,
                               nextLevel: Option[NextLevel],
                               appendix: Log[Slice[Byte], Segment, AppendixLogCache],
                               lock: Option[FileLocker],
                               pathDistributor: PathsDistributor)(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                  functionStore: FunctionStore,
                                                                  removeWriter: LogEntryWriter[LogEntry.Remove[Slice[Byte]]],
                                                                  addWriter: LogEntryWriter[LogEntry.Put[Slice[Byte], Segment]],
                                                                  val keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                  val fileSweeper: FileSweeper,
                                                                  val bufferCleaner: ByteBufferSweeperActor,
                                                                  val blockCacheSweeper: Option[MemorySweeper.Block],
                                                                  val segmentIDGenerator: IDGenerator,
                                                                  val segmentIO: SegmentReadIO,
                                                                  val forceSaveApplier: ForceSaveApplier) extends NextLevel with LazyLogging { self =>


  override val levelNumber: Int =
    pathDistributor
      .head
      .path
      .folderId
      .toInt

  logger.info(s"{}: Level $levelNumber started.", pathDistributor)

  private implicit val currentWalker =
    new CurrentWalker {
      override def get(key: Slice[Byte],
                       readState: ThreadReadState): KeyValue.PutOption =
        self.get(key, readState)

      override def higher(key: Slice[Byte], readState: ThreadReadState): LevelSeek[KeyValue] =
        higherInThisLevel(key, readState)

      override def lower(key: Slice[Byte], readState: ThreadReadState): LevelSeek[KeyValue] =
        self.lowerInThisLevel(key, readState)

      override def levelNumber: String =
        "Level: " + self.levelNumber
    }

  private implicit val nextWalker =
    new NextWalker {
      override def higher(key: Slice[Byte],
                          readState: ThreadReadState): KeyValue.PutOption =
        higherInNextLevel(key, readState)

      override def lower(key: Slice[Byte],
                         readState: ThreadReadState): KeyValue.PutOption =
        lowerFromNextLevel(key, readState)

      override def get(key: Slice[Byte],
                       readState: ThreadReadState): KeyValue.PutOption =
        getFromNextLevel(key, readState)

      override def levelNumber: String =
        "Level: " + self.levelNumber
    }

  private implicit val currentGetter =
    new CurrentGetter {
      override def get(key: Slice[Byte],
                       readState: ThreadReadState): KeyValueOption =
        getFromThisLevel(key, readState)
    }

  val meter =
    new LevelMeter {
      override def segmentsCount: Int =
        self.segmentsCount()

      override def levelSize: Long =
        self.levelSize

      override def requiresCleanUp: Boolean =
        !hasNextLevel && self.shouldSelfCompactOrExpire

      override def nextLevelMeter: Option[LevelMeter] =
        nextLevel.map(_.meter)

      override def segmentCountAndLevelSize: (Int, Long) =
        self.segmentCountAndLevelSize
    }

  def rootPath: Path =
    dirs.head.path

  def appendixPath: Path =
    rootPath.resolve("appendix")

  def nextPushDelay: FiniteDuration =
    throttle(meter).compactionDelay

  def compactDataSize: Long =
    throttle(meter).compactDataSize

  def assign(newKeyValues: Assignable.Collection,
             targetSegments: IterableOnce[Segment],
             removeDeletedRecords: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]] =
    assign(
      newKeyValues = Seq(newKeyValues),
      targetSegments = targetSegments,
      removeDeletedRecords = removeDeletedRecords,
      noGaps = false
    )

  def assign(newKeyValues: Iterable[Assignable.Collection],
             targetSegments: IterableOnce[Segment],
             removeDeletedRecords: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]] = {
    logger.trace(s"{}: Putting segments '{}' segments.", pathDistributor.head, newKeyValues.size)
    assign(
      newKeyValues = newKeyValues,
      targetSegments = targetSegments,
      removeDeletedRecords = removeDeletedRecords,
      noGaps = false
    )
  }

  def assign(newKeyValues: LevelZeroLog,
             targetSegments: IterableOnce[Segment],
             removeDeletedRecords: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]] = {
    logger.trace("{}: PutMap '{}' Maps.", pathDistributor.head, newKeyValues.cache.skipList.size)
    assign(
      newKeyValues = Seq(Assignable.Collection.fromMap(newKeyValues)),
      targetSegments = targetSegments,
      removeDeletedRecords = removeDeletedRecords,
      noGaps = false
    )
  }

  def refresh(segments: Iterable[Segment],
              removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                             parallelism: CompactionParallelism): Future[Iterable[DefIO[Segment, Slice[TransientSegment]]]] = {
    logger.debug("{}: Running refresh.", pathDistributor.head)
    Futures.traverseBounded(parallelism.levelSegmentAssignmentParallelism, segments) {
      case segment: MemorySegment =>
        Future {
          segment.refresh(
            removeDeletes = removeDeletedRecords,
            createdInLevel = levelNumber,
            segmentConfig = segmentConfig
          ).map(_.mapToSlice(TransientSegment.Memory))
        }

      case segment: PersistentSegment =>
        segment.refresh(
          removeDeletes = removeDeletedRecords,
          createdInLevel = levelNumber,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )
    }
  }

  /**
   * Input segments should always receive Segments that are in the Level itself.
   */
  def collapse(segments: Iterable[Segment],
               removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                              compactionIO: CompactionIO.Actor,
                                              parallelism: CompactionParallelism): Future[LevelCollapseResult] = {
    logger.trace(s"{}: Collapsing '{}' segments", pathDistributor.head, segments.size)
    if (segments.isEmpty || appendix.cache.size == 1) //if there is only one Segment in this Level which is a small segment. No collapse required
      Future.successful(LevelCollapseResult.Empty)
    else
      Future {
        //other segments in the appendix that are not the input segments (segments to collapse).
        val levelSegments = self.segments()
        val targetAppendixSegments = levelSegments.filterNot(levelSegment => segments.exists(_.path == levelSegment.path))

        val (segmentsToMerge, targetSegments) =
          if (targetAppendixSegments.nonEmpty) {
            logger.trace(s"{}: Target appendix segments {}", pathDistributor.head, targetAppendixSegments.size)
            (segments, targetAppendixSegments)
          } else {
            //If appendix without the small Segments is empty.
            // Then pick the first segment from the smallest segments and merge other small segments into it.
            val firstToCollapse = Iterable(segments.head)
            logger.trace(s"{}: Target segments {}", pathDistributor.head, firstToCollapse.size)
            (segments.drop(1), firstToCollapse)
          }

        //reserve the Level. It's unknown here what segments will value collapsed into what other Segments.
        val assignments =
          assign(
            newKeyValues = segmentsToMerge,
            targetSegments = targetSegments,
            removeDeletedRecords = removeDeletedRecords,
            noGaps = true
          )

        (segmentsToMerge, assignments)
      } flatMap {
        case (segmentsToMerge, assignments) =>
          merge(
            assigment = assignments,
            removeDeletedRecords = removeDeletedRecords
          ) map {
            mergeResult =>
              LevelCollapseResult.Collapsed(
                sourceSegments = segmentsToMerge,
                targetSegments = mergeResult
              )
          }
      }
  }

  def remove(segments: Iterable[Segment]): IO[swaydb.Error.Level, Unit] = {
    //create this list which is a copy of segments. Segments can be iterable only once if it's a Java iterable.
    //this copy is for second read to delete the segments after the LogEntry is successfully created.
    logger.trace(s"{}: Removing Segments {}", pathDistributor.head, segments.map(_.path.toString))
    val segmentsToRemove = Slice.of[Segment](segments.size)

    val logEntry =
      segments
        .foldLeft(Option.empty[LogEntry[Slice[Byte], Segment]]) {
          case (previousEntry, segmentToRemove) =>
            segmentsToRemove add segmentToRemove
            val nextEntry = LogEntry.Remove[Slice[Byte]](segmentToRemove.minKey)
            previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
        }

    logEntry match {
      case Some(logEntry) =>
        //        logger.info(s"$id. Build map entry: ${logEntry.string(_.asInt().toString, _.id.toString)}")
        logger.trace(s"{}: Built map entry to remove Segments {}", pathDistributor.head, segments.map(_.path.toString))
        appendix.writeSafe(logEntry) flatMap {
          _ =>
            logger.debug(s"{}: LogEntry delete Segments successfully written. Deleting physical Segments: {}", pathDistributor.head, segments.map(_.path.toString))
            // If a delete fails that would be due OS permission issue.
            // But it's OK if it fails as long as appendix is updated with new segments. An error message will be logged
            // asking to delete the uncommitted segments manually or do a database restart which will delete the uncommitted
            // Segments on reboot.
            if (segmentConfig.isDeleteEventually) {
              segmentsToRemove foreach (_.delete(segmentConfig.deleteDelay))
              IO.unit
            } else {
              IO(Segment.deleteSegments(segmentsToRemove))
                .map(_ => ())
                .recoverWith {
                  case exception =>
                    logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segmentsToRemove.mapToSlice(_.path.toString).mkString(", "), exception)
                    IO.unit
                }
            }
        }

      case None =>
        IO.Left[swaydb.Error.Level, Unit](swaydb.Error.NoSegmentsRemoved)
    }
  }

  private def assign(newKeyValues: Iterable[Assignable],
                     targetSegments: IterableOnce[Segment],
                     removeDeletedRecords: Boolean,
                     noGaps: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]] = {
    logger.trace(s"{}: Merging {} KeyValues.", pathDistributor.head, newKeyValues.size)

    val assignments: Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]] =
      if (inMemory)
        assignMemory(
          newKeyValues = newKeyValues,
          targetSegments = targetSegments,
          removeDeletedRecords = removeDeletedRecords,
          noGaps = noGaps
        )
      else
        assignPersistent(
          newKeyValues = newKeyValues,
          targetSegments = targetSegments,
          removeDeletedRecords = removeDeletedRecords,
          noGaps = noGaps
        )

    DefIO(
      input = newKeyValues,
      output = assignments
    )
  }

  def merge(assigment: DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]],
            removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                           compactionIO: CompactionIO.Actor,
                                           parallelism: CompactionParallelism): Future[Iterable[DefIO[SegmentOption, Iterable[Segment]]]] = {
    logger.trace(s"{}: Merging {} KeyValues.", pathDistributor.head, assigment.input.size)
    if (inMemory)
      mergeMemory(
        newKeyValues = assigment.input,
        removeDeletedRecords = removeDeletedRecords,
        assignments = assigment.output.asInstanceOf[Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]
      )
    else
      mergePersistent(
        newKeyValues = assigment.input,
        removeDeletedRecords = removeDeletedRecords,
        assignments = assigment.output.asInstanceOf[Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]
      )
  }

  private def assignMemory(newKeyValues: Iterable[Assignable],
                           targetSegments: IterableOnce[Segment],
                           removeDeletedRecords: Boolean,
                           noGaps: Boolean): Iterable[Assignment[ListBuffer[Assignable.Gap[Memory.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]] = {
    implicit def gapCreator: Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[Memory.Builder[Memory, ListBuffer]]]] =
      GapAggregator.create(removeDeletes = removeDeletedRecords)

    if (noGaps)
      Assigner.assignUnsafeNoGaps(
        keyValues = newKeyValues,
        segments = targetSegments,
        initialiseIteratorsInOneSeek = segmentConfig.initialiseIteratorsInOneSeek
      )
    else
      Assigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]]](
        keyValues = newKeyValues,
        segments = targetSegments,
        initialiseIteratorsInOneSeek = segmentConfig.initialiseIteratorsInOneSeek
      )
  }

  private def assignPersistent(newKeyValues: Iterable[Assignable],
                               targetSegments: IterableOnce[Segment],
                               removeDeletedRecords: Boolean,
                               noGaps: Boolean): Iterable[Assignment[ListBuffer[Assignable.Gap[Persistent.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]] = {
    implicit def gapCreator: Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]]] =
      GapAggregator.create[MergeStats.Persistent.Builder[Memory, ListBuffer]](removeDeletes = removeDeletedRecords)

    if (noGaps)
      Assigner.assignUnsafeNoGaps(
        keyValues = newKeyValues,
        segments = targetSegments,
        initialiseIteratorsInOneSeek = segmentConfig.initialiseIteratorsInOneSeek
      )
    else
      Assigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]]](
        keyValues = newKeyValues,
        segments = targetSegments,
        initialiseIteratorsInOneSeek = segmentConfig.initialiseIteratorsInOneSeek
      )
  }

  @inline private def mergeMemory(newKeyValues: Iterable[Assignable],
                                  removeDeletedRecords: Boolean,
                                  assignments: Iterable[Assignment[Iterable[Assignable.Gap[Memory.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]])(implicit ec: ExecutionContext,
                                                                                                                                                                    parallelism: CompactionParallelism): Future[Iterable[DefIO[SegmentOption, Iterable[MemorySegment]]]] =
    if (assignments.isEmpty) {
      //if there were not assignments then write new key-values are gap and run Defrag to avoid creating small Segments.
      val gap =
        GapAggregator
          .create[MergeStats.Memory.Builder[Memory, ListBuffer]](removeDeletes = removeDeletedRecords)
          .createNew()
          .addAll(newKeyValues)

      implicit val segmentConfigImplicit: SegmentBlockConfig = segmentConfig

      DefragMemorySegment.runOnGaps[Segment, SegmentOption](
        nullSegment = Segment.Null,
        headGap = gap.result,
        tailGap = ListBuffer.empty,
        removeDeletes = removeDeletedRecords,
        createdInLevel = self.levelNumber,
        pathsDistributor = pathDistributor
      ).map(Seq(_))
    } else {
      //Assignment successful. Defer merge to target Segments.
      Futures.traverseBounded(parallelism.levelSegmentAssignmentParallelism, assignments) {
        assignment =>
          assignment.segment.asInstanceOf[MemorySegment].put(
            //if noGaps == true then headGap and tailGap will be null. Perform null check
            headGap = Option(assignment.headGap).map(_.result).getOrElse(Iterable.empty),
            tailGap = Option(assignment.tailGap).map(_.result).getOrElse(Iterable.empty),
            newKeyValues = assignment.midOverlap.result.iterator,
            removeDeletes = removeDeletedRecords,
            createdInLevel = levelNumber,
            segmentConfig = segmentConfig
          )
      } map {
        mergeResults =>
          mergeResults map {
            result =>
              result.withInput(result.input.asSegmentOption)
          }
      }
    }

  @inline private def mergePersistent(newKeyValues: Iterable[Assignable],
                                      removeDeletedRecords: Boolean,
                                      assignments: Iterable[Assignment[Iterable[Assignable.Gap[Persistent.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]])(implicit ec: ExecutionContext,
                                                                                                                                                                            compactionIO: CompactionIO.Actor,
                                                                                                                                                                            parallelism: CompactionParallelism): Future[Iterable[DefIO[SegmentOption, Iterable[PersistentSegment]]]] =
    if (assignments.isEmpty) {
      //if there were not assignments then write new key-values are gap and run Defrag to avoid creating small Segments.
      val gap =
        GapAggregator
          .create[MergeStats.Persistent.Builder[Memory, ListBuffer]](removeDeletes = removeDeletedRecords)
          .createNew()
          .addAll(newKeyValues)

      implicit val valuesConfigImplicit: ValuesBlockConfig = valuesConfig
      implicit val sortedIndexConfigImplicit: SortedIndexBlockConfig = sortedIndexConfig
      implicit val binarySearchIndexConfigImplicit: BinarySearchIndexBlockConfig = binarySearchIndexConfig
      implicit val hashIndexConfigImplicit: HashIndexBlockConfig = hashIndexConfig
      implicit val bloomFilterConfigImplicit: BloomFilterBlockConfig = bloomFilterConfig
      implicit val segmentConfigImplicit: SegmentBlockConfig = segmentConfig

      DefragPersistentSegment.runOnGaps[Segment, SegmentOption](
        nullSegment = Segment.Null,
        headGap = gap.result,
        tailGap = ListBuffer.empty,
        removeDeletes = removeDeletedRecords,
        createdInLevel = self.levelNumber,
        pathsDistributor = pathDistributor,
        segmentRefCacheLife = segmentConfig.segmentRefCacheLife,
        mmap = segmentConfig.mmap
      ).map(Seq(_))
    } else {
      //Assignment successful. Defer merge to target Segments.
      Futures.traverseBounded(parallelism.levelSegmentAssignmentParallelism, assignments) {
        assignment =>
          assignment.segment.asInstanceOf[PersistentSegment].put(
            //if noGaps == true then headGap and tailGap will be null. Perform null check
            headGap = Option(assignment.headGap).map(_.result).getOrElse(Iterable.empty),
            tailGap = Option(assignment.tailGap).map(_.result).getOrElse(Iterable.empty),
            newKeyValues = assignment.midOverlap.result.iterator,
            removeDeletes = removeDeletedRecords,
            createdInLevel = levelNumber,
            valuesConfig = valuesConfig,
            sortedIndexConfig = sortedIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            hashIndexConfig = hashIndexConfig,
            bloomFilterConfig = bloomFilterConfig,
            segmentConfig = segmentConfig,
            pathsDistributor = pathDistributor,
            segmentRefCacheLife = segmentConfig.segmentRefCacheLife,
            mmap = segmentConfig.mmap
          )
      } map {
        mergeResults =>
          mergeResults map {
            result =>
              result.withInput(result.input.asSegmentOption)
          }
      }
    }

  override def commit(mergeResult: DefIO[SegmentOption, Iterable[TransientSegment]]): IO[Error.Level, Unit] =
    commit(Seq(mergeResult))

  override def commit(mergeResult: Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Level, Unit] =
    persistAndCommit(
      mergeResult = mergeResult,
      appendEntry = None
    )

  def commit(collapsed: LevelCollapseResult.Collapsed): IO[Error.Level, Unit] =
    commit(
      old = collapsed.sourceSegments,
      persisted = collapsed.targetSegments
    )

  override def commit(old: Iterable[Segment],
                      persisted: Iterable[DefIO[SegmentOption, Iterable[Segment]]]): IO[Error.Level, Unit] = {
    val appendEntry =
      old.foldLeft(Option.empty[LogEntry[Slice[Byte], Segment]]) {
        case (logEntry, smallSegment) =>
          val entry = LogEntry.Remove(smallSegment.minKey)
          logEntry.map(_ ++ entry) orElse Some(entry)
      }

    commitPersisted(
      result = persisted,
      appendEntry = appendEntry
    ) transform {
      _ =>
        //delete the segments merged with self.
        if (segmentConfig.isDeleteEventually)
          old foreach (_.delete(segmentConfig.deleteDelay))
        else
          old foreach {
            segment =>
              IO(segment.delete) onLeftSideEffect {
                exception =>
                  logger.warn(s"{}: Failed to delete Segment {} after successful collapse", pathDistributor.head, segment.path, exception)
              }
          }
    }
  }

  def commitPersisted(result: Iterable[DefIO[SegmentOption, Iterable[Segment]]]): IO[Error.Level, Unit] =
    commitPersisted(
      result = result,
      appendEntry = None
    )

  private def persistAndCommit(mergeResult: Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]],
                               appendEntry: Option[LogEntry[Slice[Byte], Segment]]): IO[Error.Level, Unit] =
    persist(mergeResult).flatMap(result => commitPersisted(result, appendEntry))

  private def commitPersisted(result: Iterable[DefIO[SegmentOption, Iterable[Segment]]],
                              appendEntry: Option[LogEntry[Slice[Byte], Segment]]): IO[Error.Level, Unit] = {

    val persistentAndCommitResult =
      if (result.isEmpty && appendEntry.isEmpty)
        IO.unit
      else
        prepareCommit(
          result = result,
          appendEntry = appendEntry
        ) flatMap {
          entry =>
            appendix
              .writeSafe(entry)
              .flatMap {
                success =>
                  if (!success)
                    IO.failed("Failed to write logEntry.")
                  else
                    IO.unit
              }
        }

    persistentAndCommitResult andThen {
      logger.debug(s"{}: putKeyValues successful. Deleting assigned Segments. {}.", pathDistributor.head, result.map(_.input.mapS(_.path)))
      //delete assigned segments as they are replaced with new segments.
      if (segmentConfig.isDeleteEventually)
        result foreach {
          mergeResult =>
            if (mergeResult.input.isSomeS)
              IO(mergeResult.input.getS.delete(segmentConfig.deleteDelay)) onLeftSideEffect {
                exception =>
                  logger.error(s"{}: Failed to delete Segment {}", pathDistributor.head, mergeResult.input.getS.path, exception)
              }
        }
      else
        result foreach {
          mergeResult =>
            if (mergeResult.input.isSomeS)
              IO(mergeResult.input.getS.delete) onLeftSideEffect {
                exception =>
                  logger.error(s"{}: Failed to delete Segment {}", pathDistributor.head, mergeResult.input.getS.path, exception)
              }
        }
    } onLeftSideEffect {
      error =>
        logFailure(s"${pathDistributor.head}: Failed to write key-values. Reverting", error)

        result foreach {
          result =>
            result.output foreach {
              segment =>
                IO(segment.delete) onLeftSideEffect {
                  exception =>
                    logger.error(s"${pathDistributor.head}: Failed to delete Segment ${segment.path}", exception)
                }
            }
        }
    }
  }

  def persist(mergeResult: Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Segment, Iterable[DefIO[SegmentOption, Iterable[Segment]]]] =
    if (inMemory)
      SegmentWriteMemoryIO.persistMerged(
        pathsDistributor = pathDistributor,
        segmentRefCacheLife = segmentConfig.segmentRefCacheLife,
        mmap = segmentConfig.mmap,
        mergeResult = mergeResult.asInstanceOf[Iterable[DefIO[SegmentOption, Iterable[TransientSegment.Memory]]]]
      )
    else
      SegmentWritePersistentIO.persistMerged(
        pathsDistributor = pathDistributor,
        segmentRefCacheLife = segmentConfig.segmentRefCacheLife,
        mmap = segmentConfig.mmap,
        mergeResult = mergeResult.asInstanceOf[Iterable[DefIO[SegmentOption, Iterable[TransientSegment.Persistent]]]]
      )

  private def prepareCommit(result: Iterable[DefIO[SegmentOption, Iterable[Segment]]],
                            appendEntry: Option[LogEntry[Slice[Byte], Segment]]): IO[Error.Level, LogEntry[Slice[Byte], Segment]] = {
    logger.trace(s"${pathDistributor.head}: Committing Segments. ${result.map { result => s"""${result.input.toOptionS.map(_.path)} -> ${result.output.map(_.path.toString).mkString(", ")}""" }.mkString("\n")}.")

    result.foldLeftRecoverIO(LogEntry.noneSegment) {
      case (logEntry, result) =>
        buildNewLogEntry(
          newSegments = result.output,
          originalSegmentMayBe = result.input,
          initialLogEntry = logEntry
        ).toOptionValue

    } flatMap {
      case Some(logEntry) =>
        //also write appendEntry to this logEntry before committing entries to appendix.
        //Note: appendEntry should not overwrite new Segment's entries with same keys so perform distinct
        //which will remove oldEntries with duplicates with newer keys.
        appendEntry match {
          case Some(appendEntry) =>
            IO(LogEntry.distinct(logEntry, appendEntry))

          case None =>
            IO(logEntry)
        }

      case None =>
        appendEntry match {
          case Some(appendEntry) =>
            IO(appendEntry)

          case None =>
            IO.failed(s"${pathDistributor.head}: Failed to create map entry")
        }

    } onLeftSideEffect {
      failure =>
        logFailure(s"${pathDistributor.head}: Failed to write key-values. Reverting", failure)
    }
  }

  def buildNewLogEntry(newSegments: Iterable[Segment],
                       originalSegmentMayBe: SegmentOption = Segment.Null,
                       initialLogEntry: Option[LogEntry[Slice[Byte], Segment]]): IO[swaydb.Error.Level, LogEntry[Slice[Byte], Segment]] = {
    import keyOrder._

    var removeOriginalSegments = true

    val nextLogEntry =
      newSegments.foldLeft(initialLogEntry) {
        case (logEntry, newSegment) =>
          //if one of the new segments have the same minKey as the original segment, remove is not required as 'put' will replace old key.
          if (removeOriginalSegments && originalSegmentMayBe.existsS(_.minKey equiv newSegment.minKey))
            removeOriginalSegments = false

          val nextLogEntry = LogEntry.Put(newSegment.minKey, newSegment)
          logEntry.map(_ ++ nextLogEntry) orElse Some(nextLogEntry)
      }

    val entryWithRemove: Option[LogEntry[Slice[Byte], Segment]] =
      originalSegmentMayBe match {
        case originalMap: Segment if removeOriginalSegments =>
          val removeLogEntry = LogEntry.Remove[Slice[Byte]](originalMap.minKey)
          nextLogEntry.map(_ ++ removeLogEntry) orElse Some(removeLogEntry)

        case _ =>
          nextLogEntry
      }

    entryWithRemove match {
      case Some(value) =>
        IO.Right(value)

      case None =>
        IO.failed("Failed to build map entry")
    }
  }

  def getFromThisLevel(key: Slice[Byte], readState: ThreadReadState): KeyValueOption =
    appendix
      .cache
      .floor(key)
      .flatMapSomeS(swaydb.core.data.Memory.Null: KeyValueOption)(_.get(key, readState))

  def getFromNextLevel(key: Slice[Byte],
                       readState: ThreadReadState): KeyValue.PutOption =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.get(key, readState)

      case None =>
        KeyValue.Put.Null
    }

  override def get(key: Slice[Byte], readState: ThreadReadState): KeyValue.PutOption =
    Get(key, readState)

  private def mightContainKeyInThisLevel(key: Slice[Byte], threadReadState: ThreadReadState): Boolean =
    appendix
      .cache
      .floor(key)
      .existsS(_.mightContainKey(key, threadReadState))

  private def mightContainFunctionInThisLevel(functionId: Slice[Byte]): Boolean =
    segments()
      .exists {
        segment =>
          segment
            .minMaxFunctionId
            .exists {
              minMax =>
                MinMax.contains(
                  key = functionId,
                  minMax = minMax
                )(FunctionStore.order)
            }
      }

  override def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean =
    mightContainKeyInThisLevel(key, threadState) ||
      nextLevel.exists(_.mightContainKey(key, threadState))

  override def mightContainFunction(functionId: Slice[Byte]): Boolean =
    mightContainFunctionInThisLevel(functionId) ||
      nextLevel.exists(_.mightContainFunction(functionId))

  private def lowerInThisLevel(key: Slice[Byte],
                               readState: ThreadReadState): LevelSeek[KeyValue] =
    appendix
      .cache
      .lower(key)
      .flatMapSomeS(LevelSeek.None: LevelSeek[KeyValue]) {
        segment =>
          LevelSeek(
            segmentNumber = segment.segmentNumber,
            result = segment.lower(key, readState).toOptional
          )
      }

  private def lowerFromNextLevel(key: Slice[Byte],
                                 readState: ThreadReadState): KeyValue.PutOption =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.lower(key, readState)

      case None =>
        KeyValue.Put.Null
    }

  override def floor(key: Slice[Byte],
                     readState: ThreadReadState): KeyValue.PutOption =
    get(key, readState) orElse lower(key, readState)

  override def lower(key: Slice[Byte],
                     readState: ThreadReadState): KeyValue.PutOption =
    Lower(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.readStart,
      nextSeek = Seek.Next.Read
    )

  private def higherFromFloorSegment(key: Slice[Byte],
                                     readState: ThreadReadState): LevelSeek[KeyValue] =
    appendix.cache.floor(key) match {
      case segment: Segment =>
        LevelSeek(
          segmentNumber = segment.segmentNumber,
          result = segment.higher(key, readState).toOptional
        )

      case Segment.Null =>
        LevelSeek.None
    }

  private def higherFromHigherSegment(key: Slice[Byte], readState: ThreadReadState): LevelSeek[KeyValue] =
    appendix.cache.higher(key) match {
      case segment: Segment =>
        LevelSeek(
          segmentNumber = segment.segmentNumber,
          result = segment.higher(key, readState).toOptional
        )

      case Segment.Null =>
        LevelSeek.None
    }

  private[core] def higherInThisLevel(key: Slice[Byte], readState: ThreadReadState): LevelSeek[KeyValue] = {
    val fromFloor = higherFromFloorSegment(key, readState)

    if (fromFloor.isDefined)
      fromFloor
    else
      higherFromHigherSegment(key, readState)
  }

  private def higherInNextLevel(key: Slice[Byte],
                                readState: ThreadReadState): KeyValue.PutOption =
    nextLevel match {
      case Some(nextLevel) =>
        nextLevel.higher(key, readState)

      case None =>
        KeyValue.Put.Null
    }

  def ceiling(key: Slice[Byte],
              readState: ThreadReadState): KeyValue.PutOption =
    get(key, readState) orElse higher(key, readState)

  override def higher(key: Slice[Byte],
                      readState: ThreadReadState): KeyValue.PutOption =
    Higher(
      key = key,
      readState = readState,
      currentSeek = Seek.Current.readStart,
      nextSeek = Seek.Next.Read
    )

  /**
   * Does a quick appendix lookup.
   * It does not check if the returned key is removed. Use [[Level.head]] instead.
   */
  override def headKey(readState: ThreadReadState): SliceOption[Byte] =
    nextLevel match {
      case Some(nextLevel) =>
        val thisLevelHeadKey = appendix.cache.headKey()
        val nextLevelHeadKey = nextLevel.headKey(readState)

        MinMax.minFavourLeftC[SliceOption[Byte], Slice[Byte]](
          left = thisLevelHeadKey,
          right = nextLevelHeadKey
        )(keyOrder)

      case None =>
        appendix.cache.headKey()
    }

  /**
   * Does a quick appendix lookup.
   * It does not check if the returned key is removed. Use [[Level.last]] instead.
   */
  override def lastKey(readState: ThreadReadState): SliceOption[Byte] =
    nextLevel match {
      case Some(nextLevel) =>
        val thisLevelLastKey = appendix.cache.maxKey()
        val nextLevelLastKey = nextLevel.lastKey(readState)

        MinMax.maxFavourLeftC[SliceOption[Byte], Slice[Byte]](
          left = thisLevelLastKey,
          right = nextLevelLastKey
        )(keyOrder)

      case None =>
        appendix.cache.maxKey()
    }

  override def head(readState: ThreadReadState): KeyValue.PutOption =
    headKey(readState)
      .flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption) {
        firstKey =>
          ceiling(firstKey, readState)
      }

  override def last(readState: ThreadReadState): KeyValue.PutOption =
    lastKey(readState)
      .flatMapSomeC(KeyValue.Put.Null: KeyValue.PutOption) {
        lastKey =>
          floor(lastKey, readState)
      }

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    appendix.cache.contains(minKey)

  override def keyValueCount: Int = {
    val countFromThisLevel =
      appendix.cache.foldLeft(0) {
        case (currentTotal, (_, segment)) =>
          currentTotal + segment.keyValueCount
      }

    val countFromNextLevel =
      nextLevel match {
        case Some(nextLevel) =>
          nextLevel.keyValueCount

        case None =>
          0
      }

    countFromThisLevel + countFromNextLevel
  }

  def getSegment(minKey: Slice[Byte]): SegmentOption =
    appendix.cache.get(minKey)

  override def segmentsCount(): Int =
    appendix.cache.size

  override def take(count: Int): Slice[Segment] =
    appendix.cache.take(count)

  override def isNonEmpty(): Boolean =
    appendix.cache.nonEmpty()

  def isEmpty: Boolean =
    appendix.cache.isEmpty

  def segmentFilesOnDisk: Seq[Path] =
    Effect.segmentFilesOnDisk(dirs.map(_.path))

  def segmentFilesInAppendix: Int =
    appendix.cache.size

  def foreachSegment[T](f: (Slice[Byte], Segment) => T): Unit =
    appendix.cache.foreach(f)

  def segments(): Iterable[Segment] =
    appendix.cache.values()

  def hasNextLevel: Boolean =
    nextLevel.isDefined

  override def existsOnDisk =
    dirs.forall(_.path.exists)

  override def levelSize: Long =
    appendix.cache.foldLeft(0L)(_ + _._2.segmentSize)

  override def sizeOfSegments: Long =
    levelSize + nextLevel.map(_.levelSize).getOrElse(0L)

  def segmentCountAndLevelSize: (Int, Long) =
    appendix.cache.foldLeft((0: Int, 0L: Long)) {
      case ((segments, size), (_, segment)) =>
        (segments + 1, size + segment.segmentSize)
    }

  def meterFor(levelNumber: Int): Option[LevelMeter] =
    if (levelNumber == pathDistributor.head.path.folderId)
      Some(meter)
    else
      nextLevel.flatMap(_.meterFor(levelNumber))

  override def takeSegments(size: Int, condition: Segment => Boolean): Iterable[Segment] =
    segments()
      .filter(condition)
      .take(size)

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    segments()
      .filter(_.segmentSize > minSegmentSize)
      .take(size)

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    segments()
      .filter(Level.isSmallSegment(_, minSegmentSize))
      .take(size)

  def hasSmallSegments: Boolean =
    segments()
      .exists(Level.isSmallSegment(_, minSegmentSize))

  def shouldSelfCompactOrExpire: Boolean =
    segments()
      .exists {
        segment =>
          Level.shouldCollapse(self, segment) || segment.nearestPutDeadline.exists(_.isOverdue())
      }

  def hasKeyValuesToExpire: Boolean =
    Segment
      .getNearestDeadlineSegment(segments())
      .isSomeS

  override def isZero: Boolean =
    false

  def lastSegmentId: Option[Long] =
    appendix
      .cache
      .last()
      .mapS(_.segmentNumber)

  override def stateId: Long =
    segmentIDGenerator.current

  override def nextCompactionDelay: FiniteDuration =
    throttle(meter).compactionDelay

  override def minSegmentSize: Int =
    segmentConfig.minSize

  override def blockCacheSize(): Option[Long] =
    blockCacheSweeper.flatMap(_.actor.map(_.totalWeight))

  override def cachedKeyValuesSize(): Option[Long] =
    keyValueMemorySweeper.flatMap(_.actor.map(_.totalWeight))

  override def openedFiles(): Long =
    fileSweeper.closer.totalWeight

  override def pendingDeletes(): Long =
    fileSweeper.deleter.totalWeight

  /**
   * Closing and delete functions
   */

  def releaseLocks: IO[swaydb.Error.Close, Unit] =
    nextLevel
      .map(_.releaseLocks)
      .getOrElse(IO.unit)
      .and(IO[swaydb.Error.Close, Unit](Effect.release(lock)))
      .onLeftSideEffect {
        failure =>
          logger.error("{}: Failed to release locks", pathDistributor.head, failure.exception)
      }

  def closeAppendixInThisLevel(): IO[Error.Level, Unit] =
    IO(appendix.close())
      .onLeftSideEffect {
        failure =>
          logger.error("{}: Failed to close appendix", pathDistributor.head, failure.exception)
      }

  def closeSegmentsInThisLevel(): IO[Error.Level, Unit] =
    segments()
      .foreachIO(segment => IO(segment.close), failFast = false)
      .getOrElse(IO.unit)
      .onLeftSideEffect {
        failure =>
          logger.error("{}: Failed to close Segment file.", pathDistributor.head, failure.exception)
      }

  //close without terminating Actors and without releasing locks.
  def closeNoSweepNoRelease(): IO[swaydb.Error.Level, Unit] =
    nextLevel
      .map(_.closeNoSweepNoRelease())
      .getOrElse(IO.unit)
      .and(closeAppendixInThisLevel())
      .and(closeSegmentsInThisLevel())

  def close[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    bag
      .fromIO(closeNoSweepNoRelease()) //close all the files first without releasing locks
      .and(LevelCloser.close()) //close background Actors and Caches if any.
      .andIO(releaseLocks) //finally release locks

  def closeNoSweep(): IO[swaydb.Error.Level, Unit] =
    closeNoSweepNoRelease()
      .and(releaseLocks)

  def closeSegments(): IO[swaydb.Error.Level, Unit] =
    nextLevel
      .map(_.closeSegments())
      .getOrElse(IO.unit)
      .and(closeSegmentsInThisLevel())

  private def deleteFiles() =
    pathDistributor
      .dirs
      .foreachIO {
        dir =>
          IO(Effect.walkDelete(dir.path))
      }
      .getOrElse(IO.unit)

  def deleteNoSweepNoClose(): IO[swaydb.Error.Level, Unit] =
    nextLevel
      .map(_.deleteNoSweepNoClose())
      .getOrElse(IO.unit)
      .and(deleteFiles())

  override def deleteNoSweep: IO[swaydb.Error.Level, Unit] =
    closeNoSweep()
      .and(deleteNoSweepNoClose())
      .and(deleteFiles())

  override def delete[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    close()
      .andIO(
        nextLevel
          .map(_.deleteNoSweepNoClose())
          .getOrElse(IO.unit)
      )
      .andIO(deleteFiles())
}
