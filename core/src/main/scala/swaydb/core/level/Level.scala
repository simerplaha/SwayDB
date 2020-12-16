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

package swaydb.core.level

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.Error.Level.ExceptionHandler
import swaydb.IO._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.{KeyValue, MergeResult, _}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.{Effect, FileLocker, ForceSaveApplier}
import swaydb.core.level.seek._
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.map.serializer._
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.merge.MergeStats
import swaydb.core.segment._
import swaydb.core.segment.assigner.{Assignable, GapAggregator, SegmentAssigner}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.util.Collections._
import swaydb.core.util.Exceptions._
import swaydb.core.util.{MinMax, _}
import swaydb.data.compaction.{LevelMeter, ParallelMerge, Throttle}
import swaydb.data.config.{Dir, PushForwardStrategy}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.storage.LevelStorage
import swaydb.data.util.FiniteDurations
import swaydb.{Bag, Error, IO}

import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[core] object Level extends LazyLogging {

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

  def apply(bloomFilterConfig: BloomFilterBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            valuesConfig: ValuesBlock.Config,
            segmentConfig: SegmentBlock.Config,
            levelStorage: LevelStorage,
            nextLevel: Option[NextLevel],
            throttle: LevelMeter => Throttle)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore,
                                              keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                              blockSweeper: Option[MemorySweeper.Block],
                                              fileSweeper: FileSweeper,
                                              bufferCleaner: ByteBufferSweeperActor,
                                              forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Level, Level] = {
    //acquire lock on folder
    acquireLock(levelStorage) flatMap {
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
        import AppendixMapEntryWriter.{AppendixPutWriter, AppendixRemoveWriter}

        val removeDeletes = Level.removeDeletes(nextLevel)

        val appendixReader =
          new AppendixMapEntryReader(
            mmapSegment = segmentConfig.mmap,
            removeDeletes = removeDeletes,
            segmentRefCacheWeight = segmentConfig.segmentRefCacheWeight
          )

        import appendixReader._

        //initialise appendix
        val appendix: IO[swaydb.Error.Level, Map[Slice[Byte], Segment, AppendixMapCache]] =
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
                  Map.persistent[Slice[Byte], Segment, AppendixMapCache](
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
              IO(Map.memory[Slice[Byte], Segment, AppendixMapCache]())
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
                implicit val segmentIDGenerator = IDGenerator(initial = largestSegmentId(allSegments))
                implicit val reserveRange = ReserveRange.create[Unit]()
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
                      pathDistributor = paths,
                      removeDeletedRecords = removeDeletes
                    )
                  }
                  .onLeftSideEffect {
                    _ =>
                      appendix.close()
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

  def optimalSegmentsToPushForward(level: NextLevel,
                                   nextLevel: NextLevel,
                                   take: Int)(implicit reserve: ReserveRange.State[Unit],
                                              keyOrder: KeyOrder[Slice[Byte]]): (Iterable[Segment], Iterable[Segment]) = {
    val segmentsToCopy = ListBuffer.empty[Segment]
    val segmentsToMerge = ListBuffer.empty[(Int, Segment)]

    level
      .segmentsInLevel()
      .foreachBreak {
        segment =>
          val count = Segment.overlapsCount(segment, nextLevel.segmentsInLevel())

          if (count == 0)
            segmentsToCopy += segment
          //only cache enough Segments to merge.
          else
            segmentsToMerge += ((count, segment))

          segmentsToCopy.size >= take
      }

    //Important! returned segments should always be in order
    val segmentsToMergeSorted =
      segmentsToMerge
        .sortBy(_._1)
        .take(take)
        .map(_._2)
        .sortBy(_.minKey)

    (segmentsToCopy, segmentsToMergeSorted)
  }

  def shouldCollapse(level: NextLevel,
                     segment: Segment)(implicit reserve: ReserveRange.State[Unit],
                                       keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    ReserveRange.isUnreserved(segment) && {
      isSmallSegment(segment, level.minSegmentSize) ||
        //if the Segment was not created in this level.
        segment.createdInLevel != level.levelNumber
    }

  def optimalSegmentsToCollapse(level: NextLevel,
                                take: Int)(implicit reserve: ReserveRange.State[Unit],
                                           keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] = {
    var segmentsTaken: Int = 0
    val segmentsToCollapse = ListBuffer.empty[Segment]

    level
      .segmentsInLevel()
      .foreachBreak {
        segment =>
          if (shouldCollapse(level, segment)) {
            segmentsToCollapse += segment
            segmentsTaken += 1
          }

          segmentsTaken >= take
      }

    segmentsToCollapse
  }
}

private[core] case class Level(dirs: Seq[Dir],
                               bloomFilterConfig: BloomFilterBlock.Config,
                               hashIndexConfig: HashIndexBlock.Config,
                               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                               sortedIndexConfig: SortedIndexBlock.Config,
                               valuesConfig: ValuesBlock.Config,
                               segmentConfig: SegmentBlock.Config,
                               inMemory: Boolean,
                               throttle: LevelMeter => Throttle,
                               nextLevel: Option[NextLevel],
                               appendix: Map[Slice[Byte], Segment, AppendixMapCache],
                               lock: Option[FileLocker],
                               pathDistributor: PathsDistributor,
                               removeDeletedRecords: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                              functionStore: FunctionStore,
                                                              removeWriter: MapEntryWriter[MapEntry.Remove[Slice[Byte]]],
                                                              addWriter: MapEntryWriter[MapEntry.Put[Slice[Byte], Segment]],
                                                              val keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                              val fileSweeper: FileSweeper,
                                                              val bufferCleaner: ByteBufferSweeperActor,
                                                              val blockCacheSweeper: Option[MemorySweeper.Block],
                                                              val segmentIDGenerator: IDGenerator,
                                                              val segmentIO: SegmentReadIO,
                                                              reserve: ReserveRange.State[Unit],
                                                              val forceSaveApplier: ForceSaveApplier) extends NextLevel with LazyLogging { self =>


  override val levelNumber: Int =
    pathDistributor
      .head
      .path
      .folderId
      .toInt

  override val pushForwardStrategy: PushForwardStrategy =
    segmentConfig.pushForward

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

      override val pushForwardStrategy: PushForwardStrategy =
        self.pushForwardStrategy
    }

  def rootPath: Path =
    dirs.head.path

  def appendixPath: Path =
    rootPath.resolve("appendix")

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

  /**
   * Partitions [[Segment]]s that can be copied into this Segment without requiring merge.
   */
  def partitionCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment]) =
    segments partition {
      segment =>
        !Segment.overlaps(
          segment = segment,
          segments2 = segmentsInLevel()
        )
    }

  /**
   * Quick lookup to check if the keys are copyable.
   */
  def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean =
    !Segment.overlaps(
      minKey = minKey,
      maxKey = maxKey,
      maxKeyInclusive = maxKeyInclusive,
      segments = segmentsInLevel()
    )

  /**
   * Quick lookup to check if the [[Map]] can be copied without merge.
   */
  def isCopyable(map: Map[Slice[Byte], Memory, LevelZeroMapCache]): Boolean =
    Segment
      .minMaxKey(map.cache.skipList)
      .forall {
        case (minKey, maxKey, maxInclusive) =>
          isCopyable(
            minKey = minKey,
            maxKey = maxKey,
            maxKeyInclusive = maxInclusive
          )
      }

  /**
   * Persists a [[Segment]] to this Level.
   *
   * @returns A Set of persisted Segment ID's
   */
  def put(segment: Segment,
          parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]] =
    put(Seq(segment), parallelMerge)

  /**
   * Persists multiple [[Segment]] to this Level.
   *
   * @returns A Set of persisted [[Segment]] ID's
   */
  def put(segments: Iterable[Segment],
          parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]] = {
    logger.trace(s"{}: Putting segments '{}' segments.", pathDistributor.head, segments.map(_.path.toString).toList)
    assignMerge(
      assignablesCount = segments.size,
      assignables = segments,
      targetSegments = appendix.cache.values(),
      parallelMerge = parallelMerge
    )
  }

  def put(map: Map[Slice[Byte], Memory, LevelZeroMapCache],
          parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]] = {
    logger.trace("{}: PutMap '{}' Maps.", pathDistributor.head, map.cache.skipList.size)
    assignMerge(
      map = map,
      targetSegments = appendix.cache.values(),
      parallelMerge = parallelMerge
    )
  }

  override def isNonEmpty(): Boolean =
    appendix.cache.nonEmpty()

  def removeSegments(segments: Iterable[Segment]): IO[swaydb.Error.Level, Int] = {
    //create this list which is a copy of segments. Segments can be iterable only once if it's a Java iterable.
    //this copy is for second read to delete the segments after the MapEntry is successfully created.
    logger.trace(s"{}: Removing Segments {}", pathDistributor.head, segments.map(_.path.toString))
    val segmentsToRemove = Slice.of[Segment](segments.size)

    val mapEntry =
      segments
        .foldLeft(Option.empty[MapEntry[Slice[Byte], Segment]]) {
          case (previousEntry, segmentToRemove) =>
            segmentsToRemove add segmentToRemove
            val nextEntry = MapEntry.Remove[Slice[Byte]](segmentToRemove.minKey)
            previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
        }

    mapEntry match {
      case Some(mapEntry) =>
        //        logger.info(s"$id. Build map entry: ${mapEntry.string(_.asInt().toString, _.id.toString)}")
        logger.trace(s"{}: Built map entry to remove Segments {}", pathDistributor.head, segments.map(_.path.toString))
        appendix.writeSafe(mapEntry) flatMap {
          _ =>
            logger.debug(s"{}: MapEntry delete Segments successfully written. Deleting physical Segments: {}", pathDistributor.head, segments.map(_.path.toString))
            // If a delete fails that would be due OS permission issue.
            // But it's OK if it fails as long as appendix is updated with new segments. An error message will be logged
            // asking to delete the uncommitted segments manually or do a database restart which will delete the uncommitted
            // Segments on reboot.
            if (segmentConfig.isDeleteEventually) {
              segmentsToRemove foreach (_.delete(segmentConfig.deleteDelay))
              IO.zero
            } else {
              IO(Segment.deleteSegments(segmentsToRemove)) recoverWith {
                case exception =>
                  logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segmentsToRemove.map(_.path.toString).mkString(", "), exception)
                  IO.zero
              }
            }
        }

      case None =>
        IO.Left[swaydb.Error.Level, Int](swaydb.Error.NoSegmentsRemoved)
    }
  }

  @inline private[core] def assignMerge(map: Map[Slice[Byte], Memory, LevelZeroMapCache],
                                        targetSegments: Iterable[Segment],
                                        parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]] =

    assignMerge(
      assignablesCount = 1,
      assignables = Seq(Assignable.Collection.fromMap(map)),
      targetSegments = targetSegments,
      parallelMerge = parallelMerge
    )

  /**
   * @return Newly created Segments.
   */
  private[core] def assignMerge(assignablesCount: Int,
                                assignables: Iterable[Assignable],
                                targetSegments: Iterable[Segment],
                                parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]] = {
    logger.trace(s"{}: Merging {} KeyValues.", pathDistributor.head, assignables.size)
    if (inMemory)
      Future {
        implicit val gapCreator = GapAggregator.memory(removeDeletes = removeDeletedRecords)

        SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]]](
          assignablesCount = assignablesCount,
          assignables = assignables,
          segments = targetSegments
        )
      } flatMap {
        assignments =>
          if (assignments.isEmpty) {
            logger.error(s"{}: Assigned segments are empty. Cannot merge Segments to empty target Segments: {}.", pathDistributor.head, assignables.size)
            Future.failed(swaydb.Exception.MergeKeyValuesWithoutTargetSegment(assignables.size))
          } else {
            //            Future.traverse(assignments) {
            //              assignment =>
            //                assignment.segment.asInstanceOf[MemorySegment].put(
            //                  headGap = assignment.headGap.result,
            //                  tailGap = assignment.tailGap.result,
            //                  mergeableCount = assignment.midOverlap.size,
            //                  mergeable = assignment.midOverlap.iterator,
            //                  removeDeletes = removeDeletedRecords,
            //                  createdInLevel = levelNumber,
            //                  segmentParallelism = parallelMerge.segment,
            //                  valuesConfig = valuesConfig,
            //                  sortedIndexConfig = sortedIndexConfig,
            //                  binarySearchIndexConfig = binarySearchIndexConfig,
            //                  hashIndexConfig = hashIndexConfig,
            //                  bloomFilterConfig = bloomFilterConfig,
            //                  segmentConfig = segmentConfig
            //                ) map {
            //                  newSegments =>
            //                    LevelMergeResult(
            //                      level = self,
            //                      segments = newSegments
            //                    )
            //                }
            //            }
            ???
          }
      }
    else
      Future {
        implicit val gapCreator = GapAggregator.persistent(removeDeletes = removeDeletedRecords)

        SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]]](
          assignablesCount = assignablesCount,
          assignables = assignables,
          segments = targetSegments
        )
      } flatMap {
        assignments =>
          //          if (assignments.isEmpty) {
          //            logger.error(s"{}: Assigned segments are empty. Cannot merge Segments to empty target Segments: {}.", pathDistributor.head, assignables.size)
          //            Future.failed(swaydb.Exception.MergeKeyValuesWithoutTargetSegment(assignables.size))
          //          } else {
          //            Future.traverse(assignments) {
          //              assignment =>
          //                assignment.segment.asInstanceOf[PersistentSegment].put(
          //                  headGap = assignment.headGap.result,
          //                  tailGap = assignment.tailGap.result,
          //                  mergeableCount = assignment.midOverlap.size,
          //                  mergeable = assignment.midOverlap.iterator,
          //                  removeDeletes = removeDeletedRecords,
          //                  createdInLevel = levelNumber,
          //                  segmentParallelism = parallelMerge.segment,
          //                  valuesConfig = valuesConfig,
          //                  sortedIndexConfig = sortedIndexConfig,
          //                  binarySearchIndexConfig = binarySearchIndexConfig,
          //                  hashIndexConfig = hashIndexConfig,
          //                  bloomFilterConfig = bloomFilterConfig,
          //                  segmentConfig = segmentConfig
          //                ) map {
          //                  newSegments =>
          //                    LevelMergeResult(
          //                      level = self,
          //                      segments = newSegments
          //                    )
          //                }
          //            }
          //          }
          ???
      }
  }

  def commit(putResult: Iterable[(Segment, MergeResult[SegmentOption, Slice[Segment]])],
             appendEntry: Option[MapEntry[Slice[Byte], Segment]])(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Unit] = {
    logger.trace(s"${pathDistributor.head}: Committing Segments. ${putResult.map { case (old, newSegments) => s"""${old.path.toString} -> ${newSegments.result.map(_.path.toString).mkString(", ")}""" }.mkString("\n")}.")

    //    putResult.foldLeftRecoverIO(MapEntry.noneSegment) {
    //      case (mapEntry, (originalSegment, newSegments)) =>
    //        buildNewMapEntry(
    //          newSegments = newSegments.result,
    //          originalSegmentMayBe = if (newSegments.replaced) originalSegment else Segment.Null,
    //          initialMapEntry = mapEntry
    //        ).toOptionValue
    //
    //    } flatMap {
    //      case Some(mapEntry) =>
    //        //also write appendEntry to this mapEntry before committing entries to appendix.
    //        //Note: appendEntry should not overwrite new Segment's entries with same keys so perform distinct
    //        //which will remove oldEntries with duplicates with newer keys.
    //        val mapEntryToWrite = appendEntry.map(appendEntry => MapEntry.distinct(mapEntry, appendEntry)) getOrElse mapEntry
    //
    //        appendix.writeSafe(mapEntryToWrite) transform {
    //          _ =>
    //            logger.debug(s"{}: putKeyValues successful. Deleting assigned Segments. {}.", pathDistributor.head, putResult.map(_._1.path.toString))
    //            //delete assigned segments as they are replaced with new segments.
    //            if (segmentConfig.isDeleteEventually)
    //              putResult foreach {
    //                case (originalSegment, newSegments) =>
    //                  if (newSegments.replaced)
    //                    originalSegment.delete(segmentConfig.deleteDelay)
    //              }
    //            else
    //              putResult foreach {
    //                case (originalSegment, newSegments) =>
    //                  if (newSegments.replaced)
    //                    IO(originalSegment.delete) onLeftSideEffect {
    //                      exception =>
    //                        logger.error(s"{}: Failed to delete Segment {}", pathDistributor.head, originalSegment.path, exception)
    //                    }
    //              }
    //        }
    //
    //      case None =>
    //        IO.failed(s"${pathDistributor.head}: Failed to create map entry")
    //
    //    } onLeftSideEffect {
    //      failure =>
    //        logFailure(s"${pathDistributor.head}: Failed to write key-values. Reverting", failure)
    //
    //        putResult foreach {
    //          case (_, newSegments) =>
    //            newSegments.result foreach {
    //              segment =>
    //                IO(segment.delete) onLeftSideEffect {
    //                  exception =>
    //                    logger.error(s"{}: Failed to delete Segment {}", pathDistributor.head, segment.path, exception)
    //                }
    //            }
    //        }
    //    }
    ???
  }

  private def buildNewMapEntry(newSegments: Iterable[Segment],
                               originalSegmentMayBe: SegmentOption = Segment.Null,
                               initialMapEntry: Option[MapEntry[Slice[Byte], Segment]]): IO[swaydb.Error.Level, MapEntry[Slice[Byte], Segment]] = {
    import keyOrder._

    var removeOriginalSegments = true

    val nextLogEntry =
      newSegments.foldLeft(initialMapEntry) {
        case (logEntry, newSegment) =>
          //if one of the new segments have the same minKey as the original segment, remove is not required as 'put' will replace old key.
          if (removeOriginalSegments && originalSegmentMayBe.existsS(_.minKey equiv newSegment.minKey))
            removeOriginalSegments = false

          val nextLogEntry = MapEntry.Put(newSegment.minKey, newSegment)
          logEntry.map(_ ++ nextLogEntry) orElse Some(nextLogEntry)
      }

    val entryWithRemove: Option[MapEntry[Slice[Byte], Segment]] =
      originalSegmentMayBe match {
        case originalMap: Segment if removeOriginalSegments =>
          val removeLogEntry = MapEntry.Remove[Slice[Byte]](originalMap.minKey)
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
      .flatMapSomeS(Memory.Null: KeyValueOption)(_.get(key, readState))

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
    appendix
      .cache
      .values()
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

  def isEmpty: Boolean =
    appendix.cache.isEmpty

  def segmentFilesOnDisk: Seq[Path] =
    Effect.segmentFilesOnDisk(dirs.map(_.path))

  def segmentFilesInAppendix: Int =
    appendix.cache.size

  def foreachSegment[T](f: (Slice[Byte], Segment) => T): Unit =
    appendix.cache.foreach(f)

  def segmentsInLevel(): Iterable[Segment] =
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
    appendix
      .cache
      .values()
      .filter(condition)
      .take(size)

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    appendix
      .cache
      .values()
      .filter(_.segmentSize > minSegmentSize)
      .take(size)

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    appendix
      .cache
      .values()
      .filter(Level.isSmallSegment(_, minSegmentSize))
      .take(size)

  override def optimalSegmentsPushForward(take: Int): (Iterable[Segment], Iterable[Segment]) =
    nextLevel match {
      case Some(nextLevel) =>
        Level.optimalSegmentsToPushForward(
          level = self,
          nextLevel = nextLevel,
          take = take
        )

      case None =>
        Level.emptySegmentsToPush
    }

  override def optimalSegmentsToCollapse(take: Int): Iterable[Segment] =
    Level.optimalSegmentsToCollapse(
      level = self,
      take = take
    )

  def hasSmallSegments: Boolean =
    appendix
      .cache
      .values()
      .exists(Level.isSmallSegment(_, minSegmentSize))

  def shouldSelfCompactOrExpire: Boolean =
    segmentsInLevel()
      .exists {
        segment =>
          Level.shouldCollapse(self, segment) ||
            FiniteDurations.getNearestDeadline(
              deadline = None,
              next = segment.nearestPutDeadline
            ).exists(_.isOverdue())
      }

  def hasKeyValuesToExpire: Boolean =
    Segment
      .getNearestDeadlineSegment(segmentsInLevel())
      .isSomeS

  override val isTrash: Boolean =
    false

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
    throttle(meter).pushDelay

  override def nextThrottlePushCount: Int =
    throttle(meter).segmentsToPush

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
    segmentsInLevel()
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
