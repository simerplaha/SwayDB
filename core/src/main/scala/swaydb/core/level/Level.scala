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

package swaydb.core.level

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ErrorHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.io.file.IOEffect._
import swaydb.core.io.file.{BlockCache, IOEffect}
import swaydb.core.level.seek._
import swaydb.core.map.serializer._
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.{Segment, SegmentAssigner}
import swaydb.core.util.Collections._
import swaydb.core.util.Exceptions._
import swaydb.core.util.{MinMax, _}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._
import swaydb.data.storage.{AppendixStorage, LevelStorage}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}

private[core] object Level extends LazyLogging {

  val emptySegmentsToPush = (Iterable.empty[Segment], Iterable.empty[Segment])

  def acquireLock(storage: LevelStorage.Persistent): IO[swaydb.Error.Level, Option[FileLock]] =
    IO {
      IOEffect createDirectoriesIfAbsent storage.dir
      val lockFile = storage.dir.resolve("LOCK")
      logger.info("{}: Acquiring lock.", lockFile)
      IOEffect createFileIfAbsent lockFile
      val lock = FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()
      storage.dirs foreach {
        dir =>
          IOEffect createDirectoriesIfAbsent dir.path
      }
      Some(lock)
    }

  def acquireLock(levelStorage: LevelStorage): IO[swaydb.Error.Level, Option[FileLock]] =
    levelStorage match {
      case persistent: LevelStorage.Persistent =>
        acquireLock(persistent)

      case _: LevelStorage.Memory =>
        IO.none
    }

  def apply(segmentSize: Long,
            bloomFilterConfig: BloomFilterBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            valuesConfig: ValuesBlock.Config,
            segmentConfig: SegmentBlock.Config,
            levelStorage: LevelStorage,
            appendixStorage: AppendixStorage,
            nextLevel: Option[NextLevel],
            pushForward: Boolean = false,
            throttle: LevelMeter => Throttle,
            deleteSegmentsEventually: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore,
                                               memorySweeper: Option[MemorySweeper.KeyValue],
                                               blockCache: Option[BlockCache.State],
                                               fileSweeper: FileSweeper.Enabled,
                                               groupBy: Option[GroupByInternal.KeyValues]): IO[swaydb.Error.Level, Level] = {
    //acquire lock on folder
    acquireLock(levelStorage) flatMap {
      lock =>
        //lock acquired.
        //initialise Segment IO for this Level.
        implicit val segmentIO =
          SegmentIO(
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )

        //initialise readers & writers
        import AppendixMapEntryWriter.{AppendixPutWriter, AppendixRemoveWriter}

        val appendixReader =
          new AppendixMapEntryReader(
            mmapSegmentsOnRead = levelStorage.mmapSegmentsOnWrite,
            mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnRead
          )

        import appendixReader._

        //default merger
        implicit val merger = AppendixSkipListMerger

        //initialise appendix
        val appendix: IO[swaydb.Error.Level, Map[Slice[Byte], Segment]] =
          appendixStorage match {
            case AppendixStorage.Persistent(mmap, appendixFlushCheckpointSize) =>
              logger.info("{}: Initialising appendix.", levelStorage.dir)
              val appendixFolder = levelStorage.dir.resolve("appendix")
              //check if appendix folder/file was deleted.
              if ((!IOEffect.exists(appendixFolder) || appendixFolder.files(Extension.Log).isEmpty) && IOEffect.segmentFilesOnDisk(levelStorage.dirs.pathsSet.toSeq).nonEmpty) {
                logger.info("{}: Failed to start Level. Appendix file is missing", appendixFolder)
                IO.failed(new IllegalStateException(s"Failed to start Level. Appendix file is missing '$appendixFolder'."))
              } else {
                IOEffect createDirectoriesIfAbsent appendixFolder
                Map.persistent[Slice[Byte], Segment](
                  folder = appendixFolder,
                  mmap = mmap,
                  flushOnOverflow = true,
                  initialWriteCount = 0,
                  fileSize = appendixFlushCheckpointSize,
                  dropCorruptedTailEntries = false
                ).map(_.item) recoverWith {
                  case error =>
                    IO.Failure(ErrorHandler.fromException(error.exception))
                }
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
            appendix.skipList.asScala foreachIO {
              case (_, segment) =>
                if (segment.existsOnDisk)
                  IO.unit
                else
                  IO.failed(swaydb.Exception.SegmentFileMissing(segment.path))
            } match {
              case Some(IO.Failure(error)) =>
                IO.Failure(error)

              case None =>
                logger.info("{}: Starting level.", levelStorage.dir)

                val allSegments = appendix.skipList.values().asScala
                implicit val segmentIDGenerator = IDGenerator(initial = largestSegmentId(allSegments))
                implicit val reserveRange = ReserveRange.create[Unit]()
                val paths: PathsDistributor = PathsDistributor(levelStorage.dirs, () => allSegments)

                val deletedUnCommittedSegments =
                  if (appendixStorage.persistent)
                    deleteUncommittedSegments(levelStorage.dirs, appendix.skipList.values().asScala)
                  else
                    IO.unit

                deletedUnCommittedSegments map {
                  _ =>
                    new Level(
                      dirs = levelStorage.dirs,
                      bloomFilterConfig = bloomFilterConfig,
                      hashIndexConfig = hashIndexConfig,
                      binarySearchIndexConfig = binarySearchIndexConfig,
                      sortedIndexConfig = sortedIndexConfig,
                      valuesConfig = valuesConfig,
                      segmentConfig = segmentConfig,
                      mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnWrite,
                      mmapSegmentsOnRead = levelStorage.mmapSegmentsOnRead,
                      inMemory = levelStorage.memory,
                      segmentSize = segmentSize,
                      pushForward = pushForward,
                      throttle = throttle,
                      nextLevel = nextLevel,
                      appendix = appendix,
                      lock = lock,
                      deleteSegmentsEventually = deleteSegmentsEventually,
                      paths = paths,
                      removeDeletedRecords = Level.removeDeletes(nextLevel),
                      appendixReadWriteLock = new ReentrantReadWriteLock()
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

  /**
   * A Segment is considered small if it's size is less than 40% of the default [[Level.segmentSize]]
   */
  def isSmallSegment(segment: Segment, levelSegmentSize: Long): Boolean =
    segment.segmentSize < levelSegmentSize * 0.40

  def deleteUncommittedSegments(dirs: Seq[Dir], appendixSegments: Iterable[Segment]): IO[swaydb.Error.Level, Unit] =
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

  def optimalSegmentsToPushForward(level: NextLevel,
                                   nextLevel: NextLevel,
                                   take: Int)(implicit reserve: ReserveRange.State[Unit],
                                              keyOrder: KeyOrder[Slice[Byte]]): (Iterable[Segment], Iterable[Segment]) = {
    val segmentsToCopy = ListBuffer.empty[Segment]
    val segmentsToMerge = ListBuffer.empty[Segment]
    level
      .segmentsInLevel()
      .foreachBreak {
        segment =>
          if (ReserveRange.isUnreserved(segment) && nextLevel.isUnreserved(segment))
            if (!Segment.overlaps(segment, nextLevel.segmentsInLevel()))
              segmentsToCopy += segment
            else if (segmentsToMerge.size < take) //only cache enough Segments to merge.
              segmentsToMerge += segment

          segmentsToCopy.size >= take
      }
    (segmentsToCopy, segmentsToMerge)
  }

  def shouldCollapse(level: NextLevel,
                     segment: Segment)(implicit reserve: ReserveRange.State[Unit],
                                       keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    ReserveRange.isUnreserved(segment) && {
      isSmallSegment(segment, level.segmentSize) ||
        //if group strategy in the level is defined and segment's grouping is undefined or vice-versa.
        level.groupBy.isDefined != segment.isGrouped.getOrElse(level.groupBy.isDefined) ||
        //if grouping is as expected by the Segment was not created in this level.
        segment.createdInLevel.getOrElse(0) != level.levelNumber
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

  def delete(level: NextLevel): IO[swaydb.Error.Delete, Unit] =
    level.close flatMap {
      _ =>
        import swaydb.Error.Delete.ErrorHandler
        level
          .nextLevel
          .map(_.delete)
          .getOrElse {
            level.paths.dirs foreachIO {
              path =>
                IOEffect.walkDelete(path.path)
            } getOrElse IO.unit
          }
    }
}

private[core] case class Level(dirs: Seq[Dir],
                               bloomFilterConfig: BloomFilterBlock.Config,
                               hashIndexConfig: HashIndexBlock.Config,
                               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                               sortedIndexConfig: SortedIndexBlock.Config,
                               valuesConfig: ValuesBlock.Config,
                               segmentConfig: SegmentBlock.Config,
                               mmapSegmentsOnWrite: Boolean,
                               mmapSegmentsOnRead: Boolean,
                               inMemory: Boolean,
                               segmentSize: Long,
                               pushForward: Boolean,
                               throttle: LevelMeter => Throttle,
                               nextLevel: Option[NextLevel],
                               appendix: Map[Slice[Byte], Segment],
                               lock: Option[FileLock],
                               deleteSegmentsEventually: Boolean,
                               paths: PathsDistributor,
                               removeDeletedRecords: Boolean,
                               appendixReadWriteLock: ReentrantReadWriteLock)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                                              functionStore: FunctionStore,
                                                                              removeWriter: MapEntryWriter[MapEntry.Remove[Slice[Byte]]],
                                                                              addWriter: MapEntryWriter[MapEntry.Put[Slice[Byte], Segment]],
                                                                              memorySweeper: Option[MemorySweeper.KeyValue],
                                                                              fileSweeper: FileSweeper.Enabled,
                                                                              blockCache: Option[BlockCache.State],
                                                                              val groupBy: Option[GroupByInternal.KeyValues],
                                                                              val segmentIDGenerator: IDGenerator,
                                                                              segmentIO: SegmentIO,
                                                                              reserve: ReserveRange.State[Unit]) extends NextLevel with LazyLogging { self =>

  logger.info(s"{}: Level started.", paths)

  private implicit val currentWalker =
    new CurrentWalker {
      override def get(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        self get key

      override def higher(key: Slice[Byte]): IO[swaydb.Error.Level, Option[ReadOnly.SegmentResponse]] =
        higherInThisLevel(key)

      override def lower(key: Slice[Byte]): IO[swaydb.Error.Level, Option[ReadOnly.SegmentResponse]] =
        self lowerInThisLevel key

      override def levelNumber: String =
        "Level: " + self.levelNumber
    }

  private implicit val nextWalker =
    new NextWalker {
      override def higher(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        higherInNextLevel(key)

      override def lower(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        lowerFromNextLevel(key)

      override def get(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
        getFromNextLevel(key)

      override def hasStateChanged(previousState: Long): Boolean =
        appendix.writeCountStateId != previousState

      override def stateID: Long =
        appendix.writeCountStateId

      override def levelNumber: String =
        "Level: " + self.levelNumber
    }

  private implicit val currentGetter =
    new CurrentGetter {
      override def get(key: Slice[Byte]): IO[swaydb.Error.Level, Option[ReadOnly.SegmentResponse]] =
        getFromThisLevel(key)
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

  private def appendixWriteLocked(initialMapEntry: MapEntry[Slice[Byte], Segment]): IO[swaydb.Error.Level, Boolean] = {
    appendixReadWriteLock.writeLock().lock()
    try
      appendix write initialMapEntry
    finally
      appendixReadWriteLock.writeLock().unlock()
  }

  private def appendixWithReadLocked[T](f: Map[Slice[Byte], Segment] => T): T = {
    appendixReadWriteLock.readLock().lock()
    try
      f(appendix)
    finally
      appendixReadWriteLock.readLock().unlock()
  }

  def rootPath: Path =
    dirs.head.path

  def appendixPath: Path =
    rootPath.resolve("appendix")

  def releaseLocks: IO[swaydb.Error.Close, Unit] =
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

  def ensureRelease[T](key: Slice[Byte])(f: => T): T =
    try
      f
    finally
      ReserveRange.free(key)

  private[level] def reserve(segments: Iterable[Segment]): IO[swaydb.Error.Level, Either[Promise[Unit], Slice[Byte]]] =
    SegmentAssigner.assignMinMaxOnly(
      inputSegments = segments,
      targetSegments = appendix.skipList.values().asScala
    ) map {
      assigned =>
        Segment.minMaxKey(assigned, segments)
          .map {
            case (minKey, maxKey, toInclusive) =>
              ReserveRange.reserveOrListen(minKey, maxKey, toInclusive, ())
          }
          .getOrElse(Left(Promise.successful()))
    }

  private[level] def reserve(map: Map[Slice[Byte], Memory.SegmentResponse]): IO[swaydb.Error.Level, Either[Promise[Unit], Slice[Byte]]] =
    SegmentAssigner.assignMinMaxOnly(
      map = map,
      targetSegments = appendix.skipList.values().asScala
    ) map {
      assigned =>
        Segment.minMaxKey(
          left = assigned,
          right = map
        ) map {
          case (minKey, maxKey, toInclusive) =>
            ReserveRange.reserveOrListen(minKey, maxKey, toInclusive, ())
        } getOrElse Left(Promise.successful())
    }

  def partitionUnreservedCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment]) =
    segments partition {
      segment =>
        ReserveRange.isUnreserved(
          from = segment.minKey,
          to = segment.maxKey.maxKey,
          toInclusive = segment.maxKey.inclusive
        ) &&
          !Segment.overlaps(
            segment = segment,
            segments2 = segmentsInLevel()
          )
    }

  def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean =
    ReserveRange.isUnreserved(
      from = minKey,
      to = maxKey,
      toInclusive = maxKeyInclusive
    ) &&
      !Segment.overlaps(
        minKey = minKey,
        maxKey = maxKey,
        maxKeyInclusive = maxKeyInclusive,
        segments = segmentsInLevel()
      )

  def isCopyable(map: Map[Slice[Byte], Memory.SegmentResponse]): Boolean =
    Segment.minMaxKey(map) forall {
      case (minKey, maxKey, maxInclusive) =>
        isCopyable(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxInclusive
        )
    }

  def isUnreserved(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean =
    ReserveRange.isUnreserved(
      from = minKey,
      to = maxKey,
      toInclusive = maxKeyInclusive
    )

  def isUnreserved(segment: Segment): Boolean =
    isUnreserved(
      minKey = segment.minKey,
      maxKey = segment.maxKey.maxKey,
      maxKeyInclusive = segment.maxKey.inclusive
    )

  def reserveAndRelease[T](segments: Iterable[Segment])(f: => IO[swaydb.Error.Level, T]): Either[Promise[Unit], IO[swaydb.Error.Level, T]] =
    reserve(segments) map {
      reserveOutcome =>
        reserveOutcome map {
          minKey =>
            ensureRelease(minKey)(f)
        }
    } match {
      case IO.Success(either) =>
        either

      case IO.Failure(error) =>
        Right(IO.Failure(error))
    }

  def reserveAndRelease[T](map: Map[Slice[Byte], Memory.SegmentResponse])(f: => IO[swaydb.Error.Level, T]): Either[Promise[Unit], IO[swaydb.Error.Level, T]] =
    reserve(map) map {
      reserveOutcome =>
        reserveOutcome map {
          minKey =>
            ensureRelease(minKey)(f)
        }
    } match {
      case IO.Success(either) =>
        either

      case IO.Failure(error) =>
        Right(IO.Failure(error))
    }

  def put(segment: Segment)(implicit ec: ExecutionContext): Either[Promise[Unit], IO[swaydb.Error.Level, Unit]] =
    put(Seq(segment))

  def put(segments: Iterable[Segment])(implicit ec: ExecutionContext): Either[Promise[Unit], IO[swaydb.Error.Level, Unit]] = {
    logger.trace(s"{}: Putting segments '{}' segments.", paths.head, segments.map(_.path.toString).toList)
    reserveAndRelease(segments) {
      val appendixSegments = appendix.skipList.values().asScala
      val (segmentToMerge, segmentToCopy) = Segment.partitionOverlapping(segments, appendixSegments)
      put(
        segmentsToMerge = segmentToMerge,
        segmentsToCopy = segmentToCopy,
        targetSegments = appendixSegments
      )
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
                         targetSegments: Iterable[Segment])(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Unit] =
    if (segmentsToCopy.nonEmpty)
      copyForwardOrCopyLocal(segmentsToCopy) flatMap {
        newlyCopiedSegments =>
          if (newlyCopiedSegments.nonEmpty) //all Segments were copied.
            buildNewMapEntry(newlyCopiedSegments, None, None) flatMap {
              copiedSegmentsEntry =>
                val putResult: IO[swaydb.Error.Level, Unit] =
                  if (segmentsToMerge.nonEmpty)
                    merge(
                      segments = segmentsToMerge,
                      targetSegments = targetSegments,
                      appendEntry = Some(copiedSegmentsEntry)
                    )
                  else
                    appendixWriteLocked(copiedSegmentsEntry) map (_ => ())

                putResult onFailureSideEffect {
                  failure =>
                    logFailure(s"${paths.head}: Failed to create a log entry. Deleting ${newlyCopiedSegments.size} copied segments", failure)
                    deleteCopiedSegments(newlyCopiedSegments)
                }
            }
          else if (segmentsToMerge.nonEmpty) //check if there are Segments to merge.
            merge( //no segments were copied. Do merge!
              segments = segmentsToMerge,
              targetSegments = targetSegments,
              appendEntry = None
            )
          else { //all Segments were forward copied, increment the stateID so reads can reset.
            appendix.incrementWriteCountStateId
            IO.unit
          }
      }
    else
      merge(
        segments = segmentsToMerge,
        targetSegments = targetSegments,
        appendEntry = None
      )

  def put(map: Map[Slice[Byte], Memory.SegmentResponse])(implicit ec: ExecutionContext): Either[Promise[Unit], IO[swaydb.Error.Level, Unit]] = {
    logger.trace("{}: PutMap '{}' Maps.", paths.head, map.skipList.count())
    reserveAndRelease(map) {
      val appendixValues = appendix.skipList.values().asScala
      if (Segment.overlaps(map, appendixValues))
        putKeyValues(
          keyValues = Slice(map.skipList.values().toArray(new Array[Memory](map.skipList.size))),
          targetSegments = appendixValues,
          appendEntry = None
        )
      else
        copyForwardOrCopyLocal(map) flatMap {
          newSegments =>
            if (newSegments.nonEmpty)
              buildNewMapEntry(newSegments, None, None) flatMap {
                entry =>
                  appendixWriteLocked(entry)
              } onFailureSideEffect {
                failure =>
                  logFailure(s"${paths.head}: Failed to create a log entry.", failure)
                  deleteCopiedSegments(newSegments)
              }
            else {
              appendix.incrementWriteCountStateId
              Segment.emptyIterableIO
            }
        } map {
          _ =>
            ()
        }
    }
  }

  /**
   * @return empty if copied into next Level else Segments copied into this Level.
   */
  private def copyForwardOrCopyLocal(map: Map[Slice[Byte], Memory.SegmentResponse])(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Iterable[Segment]] =
    forward(map) flatMap {
      copied =>
        if (copied)
          Segment.emptyIterableIO
        else
          copy(map)
    }

  /**
   * Returns segments that were not forwarded.
   */
  private def forward(map: Map[Slice[Byte], Memory.SegmentResponse])(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Boolean] = {
    logger.trace(s"{}: forwarding {} Map", paths.head, map.pathOption)
    nextLevel map {
      nextLevel =>
        if (!nextLevel.isCopyable(map))
          IO.`false`
        else if (nextLevel.put(map).exists(_.isSuccess))
          IO.`true`
        else
          IO.`false`
    } getOrElse IO.`false`
  }

  private[level] def copy(map: Map[Slice[Byte], Memory.SegmentResponse])(implicit blockCache: Option[BlockCache.State]): IO[swaydb.Error.Level, Iterable[Segment]] = {
    logger.trace(s"{}: Copying {} Map", paths.head, map.pathOption)

    def targetSegmentPath = paths.next.resolve(IDGenerator.segmentId(segmentIDGenerator.nextID))

    implicit val groupBy =
      self.groupBy flatMap {
        groupBy =>
          if (groupBy.applyGroupingOnCopy)
            self.groupBy
          else
            None
      }

    val keyValues = Slice(map.skipList.values().asScala.toArray)
    if (inMemory)
      Segment.copyToMemory(
        keyValues = keyValues,
        fetchNextPath = targetSegmentPath,
        removeDeletes = removeDeletedRecords,
        minSegmentSize = segmentSize,
        createdInLevel = levelNumber,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )
    else
      Segment.copyToPersist(
        keyValues = keyValues,
        segmentConfig = segmentConfig,
        createdInLevel = levelNumber,
        fetchNextPath = targetSegmentPath,
        mmapSegmentsOnRead = mmapSegmentsOnRead,
        mmapSegmentsOnWrite = mmapSegmentsOnWrite,
        removeDeletes = removeDeletedRecords,
        minSegmentSize = segmentSize,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )
  }

  /**
   * Returns newly created Segments.
   */
  private def copyForwardOrCopyLocal(segments: Iterable[Segment])(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Iterable[Segment]] =
    forward(segments) match {
      case IO.Success(segmentsNotForwarded) =>
        if (segmentsNotForwarded.isEmpty)
          Segment.emptyIterableIO
        else
          copy(segmentsNotForwarded)

      case IO.Failure(error) =>
        logger.trace(s"{}: Copying forward failed. Trying to copy locally {} Segments", paths.head, segments.map(_.path.toString), error.exception)
        copy(segments)
    }

  /**
   * Returns segments that were not forwarded.
   */
  private def forward(segments: Iterable[Segment])(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Iterable[Segment]] = {
    logger.trace(s"{}: Copying forward {} Segments", paths.head, segments.map(_.path.toString))
    nextLevel map {
      nextLevel =>
        val (copyable, nonCopyable) = nextLevel partitionUnreservedCopyable segments
        if (copyable.isEmpty)
          IO.Success(segments)
        else if (nextLevel.put(copyable).exists(_.isSuccess))
          IO.Success(nonCopyable)
        else
          IO.Success(segments)
    } getOrElse IO.Success(segments)
  }

  private[level] def copy(segments: Iterable[Segment])(implicit blockCache: Option[BlockCache.State]): IO[swaydb.Error.Level, Iterable[Segment]] = {
    logger.trace(s"{}: Copying {} Segments", paths.head, segments.map(_.path.toString))
    segments.flatMapIO[Segment](
      ioBlock =
        segment => {
          def targetSegmentPath = paths.next.resolve(IDGenerator.segmentId(segmentIDGenerator.nextID))

          implicit val groupBy =
            self.groupBy flatMap {
              groupBy =>
                if (groupBy.applyGroupingOnCopy)
                  self.groupBy
                else
                  None
            }

          if (inMemory)
            Segment.copyToMemory(
              segment = segment,
              createdInLevel = levelNumber,
              fetchNextPath = targetSegmentPath,
              removeDeletes = removeDeletedRecords,
              minSegmentSize = segmentSize,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig
            )
          else
            Segment.copyToPersist(
              segment = segment,
              segmentConfig = segmentConfig,
              createdInLevel = levelNumber,
              fetchNextPath = targetSegmentPath,
              mmapSegmentsOnRead = mmapSegmentsOnRead,
              mmapSegmentsOnWrite = mmapSegmentsOnWrite,
              removeDeletes = removeDeletedRecords,
              minSegmentSize = segmentSize,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig
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

  def refresh(segment: Segment)(implicit ec: ExecutionContext): Either[Promise[Unit], IO[swaydb.Error.Level, Unit]] = {
    logger.debug("{}: Running refresh.", paths.head)
    reserveAndRelease(Seq(segment)) {
      segment.refresh(
        minSegmentSize = segmentSize,
        removeDeletes = removeDeletedRecords,
        createdInLevel = levelNumber,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig,
        targetPaths = paths
      ) flatMap {
        newSegments =>
          logger.debug(s"{}: Segment {} successfully refreshed. New Segments: {}.", paths.head, segment.path, newSegments.map(_.path).mkString(", "))
          buildNewMapEntry(newSegments, Some(segment), None) flatMap {
            entry =>
              appendixWriteLocked(entry).map(_ => ()) onSuccessSideEffect {
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
  }

  def removeSegments(segments: Iterable[Segment]): IO[swaydb.Error.Level, Int] = {
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
        appendixWriteLocked(mapEntry) flatMap {
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
              Segment.deleteSegments(segmentsToRemove).recoverWith[swaydb.Error.Level, Int] {
                case exception =>
                  logger.error(s"Failed to delete Segments '{}'. Manually delete these Segments or reboot the database.", segmentsToRemove.map(_.path.toString).mkString(", "), exception)
                  IO.zero
              }
        }
    } getOrElse IO.Failure[swaydb.Error.Level, Int](swaydb.Error.NoSegmentsRemoved)
  }

  def collapse(segments: Iterable[Segment])(implicit ec: ExecutionContext): Either[Promise[Unit], IO[swaydb.Error.Level, Int]] = {
    logger.trace(s"{}: Collapsing '{}' segments", paths.head, segments.size)
    if (segments.isEmpty || appendix.size == 1) { //if there is only one Segment in this Level which is a small segment. No collapse required
      Right(IO.zero)
    } else {
      //other segments in the appendix that are not the input segments (segments to collapse).
      val levelSegments = segmentsInLevel()
      val targetAppendixSegments = levelSegments.filterNot(map => segments.exists(_.path == map.path))

      val (segmentsToMerge, targetSegments) =
        if (targetAppendixSegments.nonEmpty) {
          logger.trace(s"{}: Target appendix segments {}", paths.head, targetAppendixSegments.size)
          (segments, targetAppendixSegments)
        } else {
          //If appendix without the small Segments is empty.
          // Then pick the first segment from the smallest segments and merge other small segments into it.
          val firstToCollapse = Iterable(segments.head)
          logger.trace(s"{}: Target segments {}", paths.head, firstToCollapse.size)
          (segments.drop(1), firstToCollapse)
        }

      //reserve the Level. It's unknown here what segments will value collapsed into what other Segments.
      reserveAndRelease(levelSegments) {
        //create an appendEntry that will remove smaller segments from the appendix.
        //this entry will value applied only if the putSegments is successful orElse this entry will be discarded.
        val appendEntry =
        segmentsToMerge.foldLeft(Option.empty[MapEntry[Slice[Byte], Segment]]) {
          case (mapEntry, smallSegment) =>
            val entry = MapEntry.Remove(smallSegment.minKey)
            mapEntry.map(_ ++ entry) orElse Some(entry)
        }

        merge(
          segments = segmentsToMerge,
          targetSegments = targetSegments,
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
  }

  private def merge(segments: Iterable[Segment],
                    targetSegments: Iterable[Segment],
                    appendEntry: Option[MapEntry[Slice[Byte], Segment]]): IO[swaydb.Error.Level, Unit] = {
    logger.trace(s"{}: Merging segments {}", paths.head, segments.map(_.path.toString))
    Segment.getAllKeyValues(segments) flatMap {
      keyValues =>
        putKeyValues(
          keyValues = keyValues,
          targetSegments = targetSegments,
          appendEntry = appendEntry
        )
    }
  }

  /**
   * @return Newly created Segments.
   */
  private[core] def putKeyValues(keyValues: Slice[KeyValue.ReadOnly],
                                 targetSegments: Iterable[Segment],
                                 appendEntry: Option[MapEntry[Slice[Byte], Segment]]): IO[swaydb.Error.Level, Unit] = {
    logger.trace(s"{}: Merging {} KeyValues.", paths.head, keyValues.size)
    SegmentAssigner.assign(keyValues, targetSegments) flatMap {
      assignments =>
        logger.trace(s"{}: Assigned segments {} for {} KeyValues.", paths.head, assignments.map(_._1.path.toString), keyValues.size)
        if (assignments.isEmpty) {
          logger.error(s"{}: Assigned segments are empty. Cannot merge Segments to empty target Segments: {}.", paths.head, keyValues.size)
          IO.Failure[swaydb.Error.Level, Unit](swaydb.Error.MergeKeyValuesWithoutTargetSegment(keyValues.size))
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
                  appendixWriteLocked(mapEntryToWrite) map {
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
                  IO.failed(s"${paths.head}: Failed to create map entry")
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

  private def putAssignedKeyValues(assignedSegments: mutable.Map[Segment, Slice[KeyValue.ReadOnly]]): IO[swaydb.Error.Level, Slice[(Segment, Slice[Segment])]] =
    assignedSegments.mapIO[(Segment, Slice[Segment])](
      block = {
        case (targetSegment, assignedKeyValues) =>
          targetSegment.put(
            newKeyValues = assignedKeyValues,
            minSegmentSize = segmentSize,
            removeDeletes = removeDeletedRecords,
            createdInLevel = levelNumber,
            valuesConfig = valuesConfig,
            sortedIndexConfig = sortedIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            hashIndexConfig = hashIndexConfig,
            bloomFilterConfig = bloomFilterConfig,
            segmentConfig = segmentConfig,
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
                       initialMapEntry: Option[MapEntry[Slice[Byte], Segment]]): IO[swaydb.Error.Level, MapEntry[Slice[Byte], Segment]] = {
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
        IO.failed("Failed to build map entry")
    }
  }

  def getFromThisLevel(key: Slice[Byte]): IO[swaydb.Error.Level, Option[KeyValue.ReadOnly.SegmentResponse]] =
    appendixWithReadLocked(_.skipList.floor(key)) match {
      case Some(segment) =>
        segment get key

      case None =>
        IO.none
    }

  def getFromNextLevel(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    nextLevel.map(_.get(key)) getOrElse IO.Defer.none

  override def get(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Get(key)

  private def mightContainKeyInThisLevel(key: Slice[Byte]): IO[swaydb.Error.Level, Boolean] =
    appendixWithReadLocked(_.skipList.floor(key)) match {
      case Some(segment) =>
        segment mightContainKey key

      case None =>
        IO.`false`
    }

  private def mightContainFunctionInThisLevel(functionId: Slice[Byte]): IO[swaydb.Error.Level, Boolean] =
    IO {
      appendix.skipList.values().asScala exists {
        segment =>
          segment.minMaxFunctionId exists {
            minMax =>
              MinMax.contains(
                key = functionId,
                minMax = minMax
              )(FunctionStore.order)
          }
      }
    }

  override def mightContainKey(key: Slice[Byte]): IO[swaydb.Error.Level, Boolean] =
    mightContainKeyInThisLevel(key) flatMap {
      yes =>
        if (yes)
          IO.`true`
        else
          nextLevel.map(_.mightContainKey(key)) getOrElse IO.`false`
    }

  override def mightContainFunction(functionId: Slice[Byte]): IO[swaydb.Error.Level, Boolean] =
    mightContainFunctionInThisLevel(functionId) flatMap {
      yes =>
        if (yes)
          IO.`true`
        else
          nextLevel.map(_.mightContainFunction(functionId)) getOrElse IO.`false`
    }

  private def lowerInThisLevel(key: Slice[Byte]): IO[swaydb.Error.Level, Option[ReadOnly.SegmentResponse]] =
    appendixWithReadLocked(_.skipList.lower(key)).map(_.lower(key)) getOrElse IO.none

  private def lowerFromNextLevel(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
    nextLevel.map(_.lower(key)) getOrElse IO.Defer.none

  override def floor(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    get(key) flatMap {
      case some @ Some(_) =>
        IO.Defer(some)

      case None =>
        lower(key)
    }

  override def lower(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
    Lower(
      key = key,
      currentSeek = Seek.Read,
      nextSeek = Seek.Read
    )

  private def higherFromFloorSegment(key: Slice[Byte]): IO[swaydb.Error.Level, Option[ReadOnly.SegmentResponse]] =
    appendixWithReadLocked(_.skipList.floor(key)).map(_.higher(key)) getOrElse IO.none

  private def higherFromHigherSegment(key: Slice[Byte]): IO[swaydb.Error.Level, Option[ReadOnly.SegmentResponse]] =
    appendixWithReadLocked(_.skipList.higher(key)).map(_.higher(key)) getOrElse IO.none

  private[core] def higherInThisLevel(key: Slice[Byte]): IO[swaydb.Error.Level, Option[KeyValue.ReadOnly.SegmentResponse]] =
    higherFromFloorSegment(key) flatMap {
      fromFloor =>
        if (fromFloor.isDefined)
          IO.Success(fromFloor)
        else
          higherFromHigherSegment(key)
    }

  private def higherInNextLevel(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    nextLevel.map(_.higher(key)) getOrElse IO.Defer.none

  def ceiling(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    get(key) flatMap {
      case got @ Some(_) =>
        IO.Defer(got)

      case None =>
        higher(key)
    }

  override def higher(key: Slice[Byte]): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Higher(
      key = key,
      currentSeek = Seek.Read,
      nextSeek = Seek.Read
    )

  /**
   * Does a quick appendix lookup.
   * It does not check if the returned key is removed. Use [[Level.head]] instead.
   */
  override def headKey: IO.Defer[swaydb.Error.Level, Option[Slice[Byte]]] =
    nextLevel.map(_.headKey) getOrElse IO.Defer.none map {
      nextLevelFirstKey =>
        MinMax.minFavourLeft(appendixWithReadLocked(_.skipList.headKey), nextLevelFirstKey)(keyOrder)
    }

  /**
   * Does a quick appendix lookup.
   * It does not check if the returned key is removed. Use [[Level.last]] instead.
   */
  override def lastKey: IO.Defer[swaydb.Error.Level, Option[Slice[Byte]]] =
    nextLevel.map(_.lastKey) getOrElse IO.Defer.none map {
      nextLevelLastKey =>
        MinMax.maxFavourLeft(appendixWithReadLocked(_.skipList.last()).map(_.maxKey.maxKey), nextLevelLastKey)(keyOrder)
    }

  override def head: IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    headKey flatMap {
      firstKey =>
        firstKey.map(ceiling) getOrElse IO.Defer.none
    }

  override def last =
    lastKey flatMap {
      lastKey =>
        lastKey.map(floor) getOrElse IO.Defer.none
    }

  def containsSegmentWithMinKey(minKey: Slice[Byte]): Boolean =
    appendix.skipList contains minKey

  override def bloomFilterKeyValueCount: IO[swaydb.Error.Level, Int] =
    appendix.skipList.foldLeft(IO[swaydb.Error.Level, Int](0)) {
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
    appendix.skipList.get(minKey)

  override def segmentsCount(): Int =
    appendix.skipList.count()

  override def take(count: Int): Slice[Segment] =
    appendix.skipList take count

  def isEmpty: Boolean =
    appendix.isEmpty

  def segmentFilesOnDisk: Seq[Path] =
    IOEffect.segmentFilesOnDisk(dirs.map(_.path))

  def segmentFilesInAppendix: Int =
    appendix.skipList.count()

  def foreachSegment[T](f: (Slice[Byte], Segment) => T): Unit =
    appendix.skipList.foreach(f)

  def segmentsInLevel(): Iterable[Segment] =
    appendix.skipList.values().asScala

  def hasNextLevel: Boolean =
    nextLevel.isDefined

  override def existsOnDisk =
    dirs.forall(_.path.exists)

  override def levelSize: Long =
    appendix.skipList.foldLeft(0)(_ + _._2.segmentSize)

  override def sizeOfSegments: Long =
    levelSize + nextLevel.map(_.levelSize).getOrElse(0L)

  def segmentCountAndLevelSize: (Int, Long) =
    appendix.skipList.foldLeft(0, 0L) {
      case ((segments, size), (_, segment)) =>
        (segments + 1, size + segment.segmentSize)
    }

  val levelNumber: Int =
    paths.head.path.folderId.toInt

  def meterFor(levelNumber: Int): Option[LevelMeter] =
    if (levelNumber == paths.head.path.folderId)
      Some(meter)
    else
      nextLevel.flatMap(_.meterFor(levelNumber))

  override def takeSegments(size: Int, condition: Segment => Boolean): Iterable[Segment] =
    appendix.skipList.values().asScala filter {
      segment =>
        condition(segment)
    } take size

  override def takeLargeSegments(size: Int): Iterable[Segment] =
    appendix
      .skipList.
      values()
      .asScala
      .filter(_.segmentSize > segmentSize) take size

  override def takeSmallSegments(size: Int): Iterable[Segment] =
    appendix
      .skipList.
      values()
      .asScala
      .filter(Level.isSmallSegment(_, segmentSize)) take size

  override def optimalSegmentsPushForward(take: Int): (Iterable[Segment], Iterable[Segment]) =
    nextLevel map {
      nextLevel =>
        Level.optimalSegmentsToPushForward(
          level = self,
          nextLevel = nextLevel,
          take = take
        )
    } getOrElse Level.emptySegmentsToPush

  override def optimalSegmentsToCollapse(take: Int): Iterable[Segment] =
    Level.optimalSegmentsToCollapse(
      level = self,
      take = take
    )

  def hasSmallSegments: Boolean =
    appendix
      .skipList.
      values()
      .asScala exists (Level.isSmallSegment(_, segmentSize))

  def shouldSelfCompactOrExpire: Boolean =
    segmentsInLevel()
      .exists {
        segment =>
          Level.shouldCollapse(self, segment) ||
            FiniteDurations.getNearestDeadline(None, segment.nearestExpiryDeadline).exists(_.isOverdue())
      }

  def hasKeyValuesToExpire: Boolean =
    Segment.getNearestDeadlineSegment(segmentsInLevel()).isDefined

  def close: IO[swaydb.Error.Close, Unit] =
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

  def closeSegments(): IO[swaydb.Error.Level, Unit] = {
    segmentsInLevel().foreachIO(_.close, failFast = false) foreach {
      failure =>
        logger.error("{}: Failed to close Segment file.", paths.head, failure.exception)
    }

    nextLevel.map(_.closeSegments()) getOrElse IO.unit
  }

  override val isTrash: Boolean =
    false

  override def isZero: Boolean =
    false

  override def stateID: Long =
    appendix.writeCountStateId

  override def nextCompactionDelay: FiniteDuration =
    throttle(meter).pushDelay

  override def nextThrottlePushCount: Int =
    throttle(meter).segmentsToPush

  override def delete: IO[swaydb.Error.Delete, Unit] =
    Level.delete(self)
}
