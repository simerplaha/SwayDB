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

package swaydb.core.segment

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper
import swaydb.core.actor.{FileSweeperItem, MemorySweeper}
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile, Effect, ForceSaveApplier}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner}
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.segment.data.TransientSegment
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.{MergeStats, SegmentGrouper}
import swaydb.core.util.Collections._
import swaydb.core.util._
import swaydb.core.util.skiplist.{SkipList, SkipListTreeMap}
import swaydb.data.MaxKey
import swaydb.data.compaction.ParallelMerge.SegmentParallelism
import swaydb.data.config.{Dir, ForceSave, MMAP}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.{FiniteDurations, SomeOrNone}
import swaydb.{Aggregator, IO}

import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.Try

private[swaydb] sealed trait SegmentOption extends SomeOrNone[SegmentOption, Segment] {
  override def noneS: SegmentOption =
    Segment.Null
}

private[core] case object Segment extends LazyLogging {

  final case object Null extends SegmentOption {
    override def isNoneS: Boolean = true

    override def getS: Segment = throw new Exception("Segment is of type Null")
  }

  val emptyIterable = Iterable.empty[Segment]

  def memory(minSegmentSize: Int,
             maxKeyValueCountPerSegment: Int,
             pathsDistributor: PathsDistributor,
             createdInLevel: Long,
             stats: MergeStats.Memory.Closed[Iterable])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore,
                                                        fileSweeper: FileSweeper,
                                                        idGenerator: IDGenerator): Slice[MemorySegment] =
    if (stats.isEmpty) {
      throw IO.throwable("Empty key-values submitted to memory Segment.")
    } else {
      val segments = ListBuffer.empty[MemorySegment]

      var skipList = SkipListTreeMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      var minMaxFunctionId: Option[MinMax[Slice[Byte]]] = None
      var nearestDeadline: Option[Deadline] = None
      var hasRange: Boolean = false
      var hasPut: Boolean = false
      var currentSegmentSize = 0
      var currentSegmentKeyValuesCount = 0
      var minKey: Slice[Byte] = null
      var lastKeyValue: Memory = null

      def setClosed(): Unit = {
        skipList = SkipListTreeMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        minMaxFunctionId = None
        nearestDeadline = None
        hasRange = false
        hasPut = false
        currentSegmentSize = 0
        currentSegmentKeyValuesCount = 0
        minKey = null
        lastKeyValue = null
      }

      def put(keyValue: Memory): Unit =
        keyValue.unslice() match {
          case keyValue: Memory.Put =>
            hasPut = true
            nearestDeadline = FiniteDurations.getNearestDeadline(nearestDeadline, keyValue.deadline)
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Update =>
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Function =>
            minMaxFunctionId = Some(MinMax.minMaxFunction(keyValue, minMaxFunctionId))
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.PendingApply =>
            minMaxFunctionId = MinMax.minMaxFunction(keyValue.applies, minMaxFunctionId)
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Remove =>
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Range =>
            hasRange = true
            keyValue.fromValue foreachS {
              case put: Value.Put =>
                nearestDeadline = FiniteDurations.getNearestDeadline(nearestDeadline, put.deadline)
              case _: Value.RangeValue =>
              //no need to do anything here. Just put deadline required.
            }
            minMaxFunctionId = MinMax.minMaxFunction(keyValue, minMaxFunctionId)
            hasPut = hasPut || keyValue.fromValue.existsS(_.isPut)
            skipList.put(keyValue.key, keyValue)
        }

      def createSegment() = {
        val path = pathsDistributor.next.resolve(IDGenerator.segment(idGenerator.next))

        //Note: Memory key-values can be received from Persistent Segments in which case it's important that
        //all byte arrays are unsliced before writing them to Memory Segment.

        val segment =
          MemorySegment(
            path = path,
            minKey = minKey.unslice(),
            maxKey =
              lastKeyValue match {
                case range: Memory.Range =>
                  MaxKey.Range(range.fromKey.unslice(), range.toKey.unslice())

                case keyValue: Memory.Fixed =>
                  MaxKey.Fixed(keyValue.key.unslice())
              },
            minMaxFunctionId = minMaxFunctionId,
            segmentSize = currentSegmentSize,
            hasRange = hasRange,
            hasPut = hasPut,
            createdInLevel = createdInLevel.toInt,
            skipList = skipList,
            nearestPutDeadline = nearestDeadline
          )

        segments += segment
        setClosed()
      }

      stats.keyValues foreach {
        keyValue =>
          if (minKey == null) minKey = keyValue.key
          lastKeyValue = keyValue

          put(keyValue)

          currentSegmentSize += MergeStats.Memory calculateSize keyValue
          currentSegmentKeyValuesCount += 1

          if (currentSegmentSize >= minSegmentSize || currentSegmentKeyValuesCount >= maxKeyValueCountPerSegment)
            createSegment()
      }

      if (lastKeyValue != null)
        createSegment()

      Slice.from(segments, segments.size)
    }

  def persistent(pathsDistributor: PathsDistributor,
                 createdInLevel: Int,
                 bloomFilterConfig: BloomFilterBlock.Config,
                 hashIndexConfig: HashIndexBlock.Config,
                 binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                 sortedIndexConfig: SortedIndexBlock.Config,
                 valuesConfig: ValuesBlock.Config,
                 segmentConfig: SegmentBlock.Config,
                 mergeStats: MergeStats.Persistent.Closed[Iterable])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                                     functionStore: FunctionStore,
                                                                     fileSweeper: FileSweeper,
                                                                     bufferCleaner: ByteBufferSweeperActor,
                                                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                     blockCache: Option[BlockCache.State],
                                                                     segmentIO: SegmentIO,
                                                                     idGenerator: IDGenerator,
                                                                     forceSaveApplier: ForceSaveApplier): Slice[PersistentSegment] = {
    val transient =
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

    persistent(
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      mmap = segmentConfig.mmap,
      transient = transient
    )
  }

  def persistent(pathsDistributor: PathsDistributor,
                 createdInLevel: Int,
                 mmap: MMAP.Segment,
                 transient: Iterable[TransientSegment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore,
                                                        fileSweeper: FileSweeper,
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                        blockCache: Option[BlockCache.State],
                                                        segmentIO: SegmentIO,
                                                        idGenerator: IDGenerator,
                                                        forceSaveApplier: ForceSaveApplier): Slice[PersistentSegment] =
    transient.mapRecover(
      block =
        segment =>
          if (segment.hasEmptyByteSlice) {
            //This is fatal!! Empty Segments should never be created. If this does have for whatever reason it should
            //not be allowed so that whatever is creating this Segment (eg: compaction) does not progress with a success response.
            throw IO.throwable("Empty key-values submitted to persistent Segment.")
          } else {
            val path = pathsDistributor.next.resolve(IDGenerator.segment(idGenerator.next))

            segment match {
              case segment: TransientSegment.One =>
                val file: DBFile =
                  segmentFile(
                    path = path,
                    mmap = mmap,
                    segmentSize = segment.segmentSize,
                    applier =
                      file => {
                        file.append(segment.fileHeader)
                        file.append(segment.bodyBytes)
                      }
                  )

                PersistentSegmentOne(
                  file = file,
                  createdInLevel = createdInLevel,
                  segment = segment
                )

              case segment: TransientSegment.Remote =>
                val segmentSize = segment.segmentSize

                val file: DBFile =
                  segmentFile(
                    path = path,
                    mmap = mmap,
                    segmentSize = segmentSize,
                    applier =
                      file => {
                        file.append(segment.fileHeader)
                        segment.ref.segmentBlockCache.transfer(0, segment.segmentSizeIgnoreHeader, file)
                      }
                  )

                PersistentSegmentOne(
                  file = file,
                  createdInLevel = segment.ref.createdInLevel,
                  segment = segment
                )

              case segment: TransientSegment.Many =>
                //many can contain remote segments so createdInLevel is the smallest remote.
                var createdInLevelMin = createdInLevel

                val file: DBFile =
                  segmentFile(
                    path = path,
                    mmap = mmap,
                    segmentSize = segment.segmentSize,
                    applier =
                      file => {
                        file.append(segment.fileHeader)
                        file.append(segment.listSegment.bodyBytes)

                        segment.segments foreach {
                          case remote: TransientSegment.Remote =>
                            createdInLevelMin = createdInLevelMin min remote.ref.createdInLevel
                            remote.ref.segmentBlockCache.transfer(0, remote.segmentSize, file)

                          case one: TransientSegment.One =>
                            file.append(one.bodyBytes)
                        }
                      }
                  )

                PersistentSegmentMany(
                  file = file,
                  createdInLevel = createdInLevelMin,
                  segment = segment
                )
            }
          },
      recover =
        (segments: Slice[PersistentSegment], _: Throwable) =>
          segments foreach {
            segmentToDelete =>
              try
                segmentToDelete.delete
              catch {
                case exception: Exception =>
                  logger.error(s"Failed to delete Segment '${segmentToDelete.path}' in recover due to failed put", exception)
              }
          }
    )

  private def segmentFile(path: Path,
                          mmap: MMAP.Segment,
                          segmentSize: Int,
                          applier: DBFile => Unit)(implicit segmentIO: SegmentIO,
                                                   fileSweeper: FileSweeper,
                                                   bufferCleaner: ByteBufferSweeperActor,
                                                   blockCache: Option[BlockCache.State],
                                                   forceSaveApplier: ForceSaveApplier): DBFile =
    mmap match {
      case MMAP.On(deleteAfterClean, forceSave) => //if both read and writes are mmaped. Keep the file open.
        DBFile.mmapWriteAndReadApplier(
          path = path,
          fileOpenIOStrategy = segmentIO.fileOpenIO,
          autoClose = true,
          deleteAfterClean = deleteAfterClean,
          forceSave = forceSave,
          blockCacheFileId = BlockCacheFileIDGenerator.next,
          bufferSize = segmentSize,
          applier = applier
        )

      case MMAP.ReadOnly(deleteAfterClean) =>
        val channelWrite =
          DBFile.channelWrite(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = BlockCacheFileIDGenerator.next,
            autoClose = true,
            forceSave = ForceSave.Off
          )

        try
          applier(channelWrite)
        catch {
          case throwable: Throwable =>
            logger.error(s"Failed to write $mmap file with applier. Closing file: $path", throwable)
            channelWrite.close()
            throw throwable
        }

        channelWrite.close()

        DBFile.mmapRead(
          path = channelWrite.path,
          fileOpenIOStrategy = segmentIO.fileOpenIO,
          autoClose = true,
          deleteAfterClean = deleteAfterClean,
          blockCacheFileId = BlockCacheFileIDGenerator.next
        )

      case _: MMAP.Off =>
        val channelWrite =
          DBFile.channelWrite(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = BlockCacheFileIDGenerator.next,
            autoClose = true,
            forceSave = ForceSave.Off
          )

        try
          applier(channelWrite)
        catch {
          case throwable: Throwable =>
            logger.error(s"Failed to write $mmap file with applier. Closing file: $path", throwable)
            channelWrite.close()
            throw throwable
        }

        channelWrite.close()

        DBFile.channelRead(
          path = channelWrite.path,
          fileOpenIOStrategy = segmentIO.fileOpenIO,
          autoClose = true,
          blockCacheFileId = BlockCacheFileIDGenerator.next
        )

      //another case if mmapReads is false, write bytes in mmaped mode and then close and re-open for read. Currently not inuse.
      //    else if (mmap.mmapWrites && !mmap.mmapReads) {
      //      val file =
      //        DBFile.mmapWriteAndRead(
      //          path = path,
      //          autoClose = true,
      //          ioStrategy = segmentIO.segmentBlockIO(IOAction.OpenResource),
      //          blockCacheFileId = BlockCacheFileIDGenerator.nextID,
      //          bytes = segmentBytes
      //        )
      //
      //      //close immediately to force flush the bytes to disk. Having mmapWrites == true and mmapReads == false,
      //      //is probably not the most efficient and should not be used.
      //      file.close()
      //      DBFile.channelRead(
      //        path = file.path,
      //        ioStrategy = segmentIO.segmentBlockIO(IOAction.OpenResource),
      //        blockCacheFileId = BlockCacheFileIDGenerator.nextID,
      //        autoClose = true
      //      )
      //    }
    }

  def copyToPersist(segment: Segment,
                    createdInLevel: Int,
                    pathsDistributor: PathsDistributor,
                    removeDeletes: Boolean,
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore,
                                                        keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                        fileSweeper: FileSweeper,
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        blockCache: Option[BlockCache.State],
                                                        segmentIO: SegmentIO,
                                                        idGenerator: IDGenerator,
                                                        forceSaveApplier: ForceSaveApplier): Slice[PersistentSegment] =
    segment match {
      case segment: PersistentSegment =>
        val nextPath = pathsDistributor.next.resolve(IDGenerator.segment(idGenerator.next))

        segment.copyTo(nextPath)
        try
          Slice(
            Segment(
              path = nextPath,
              formatId = segment.formatId,
              createdInLevel = segment.createdInLevel,
              blockCacheFileId = segment.file.blockCacheFileId,
              copiedFrom = Some(segment),
              mmap = segmentConfig.mmap,
              minKey = segment.minKey,
              maxKey = segment.maxKey,
              segmentSize = segment.segmentSize,
              minMaxFunctionId = segment.minMaxFunctionId,
              nearestExpiryDeadline = segment.nearestPutDeadline
            )
          )
        catch {
          case exception: Exception =>
            logger.error("Failed to copyToPersist Segment {}", segment.path, exception)
            try
              Effect.deleteIfExists(nextPath)
            catch {
              case exception: Exception =>
                logger.error(s"Failed to delete copied persistent Segment ${segment.path}", exception)
            }
            throw exception
        }

      case memory: MemorySegment =>
        copyToPersist(
          keyValues = memory.skipList.values(),
          createdInLevel = createdInLevel,
          pathsDistributor = pathsDistributor,
          removeDeletes = removeDeletes,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )
    }

  def copyToPersist(keyValues: Iterable[Memory],
                    createdInLevel: Int,
                    pathsDistributor: PathsDistributor,
                    removeDeletes: Boolean,
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore,
                                                        keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                        fileSweeper: FileSweeper,
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        blockCache: Option[BlockCache.State],
                                                        segmentIO: SegmentIO,
                                                        idGenerator: IDGenerator,
                                                        forceSaveApplier: ForceSaveApplier): Slice[PersistentSegment] = {
    val builder =
      if (removeDeletes)
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(SegmentGrouper.addLastLevel)
      else
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

    keyValues foreach builder.add

    val closedStats =
      builder.close(
        hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
      )

    Segment.persistent(
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig,
      mergeStats = closedStats
    )
  }

  def copyToMemory(segment: Segment,
                   createdInLevel: Int,
                   pathsDistributor: PathsDistributor,
                   removeDeletes: Boolean,
                   minSegmentSize: Int,
                   maxKeyValueCountPerSegment: Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                    functionStore: FunctionStore,
                                                    fileSweeper: FileSweeper,
                                                    idGenerator: IDGenerator): Slice[MemorySegment] =
    copyToMemory(
      keyValues = segment.iterator(),
      pathsDistributor = pathsDistributor,
      removeDeletes = removeDeletes,
      minSegmentSize = minSegmentSize,
      maxKeyValueCountPerSegment = maxKeyValueCountPerSegment,
      createdInLevel = createdInLevel
    )

  def copyToMemory(keyValues: Iterator[KeyValue],
                   pathsDistributor: PathsDistributor,
                   removeDeletes: Boolean,
                   minSegmentSize: Int,
                   maxKeyValueCountPerSegment: Int,
                   createdInLevel: Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore,
                                        fileSweeper: FileSweeper,
                                        idGenerator: IDGenerator): Slice[MemorySegment] = {
    val builder =
      new MergeStats.Memory.Closed[Iterable](
        isEmpty = false,
        keyValues = Segment.toMemoryIterator(keyValues, removeDeletes).to(Iterable)
      )

    Segment.memory(
      minSegmentSize = minSegmentSize,
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      maxKeyValueCountPerSegment = maxKeyValueCountPerSegment,
      stats = builder
    )
  }

  def mergePut(headGap: Iterable[Assignable],
               tailGap: Iterable[Assignable],
               mergeableCount: Int,
               mergeable: Iterator[Assignable],
               oldKeyValuesCount: Int,
               oldKeyValues: Iterator[Persistent],
               removeDeletes: Boolean,
               createdInLevel: Int,
               valuesConfig: ValuesBlock.Config,
               sortedIndexConfig: SortedIndexBlock.Config,
               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
               hashIndexConfig: HashIndexBlock.Config,
               bloomFilterConfig: BloomFilterBlock.Config,
               segmentConfig: SegmentBlock.Config,
               pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                   keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   functionStore: FunctionStore,
                                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                   fileSweeper: FileSweeper,
                                                   bufferCleaner: ByteBufferSweeperActor,
                                                   blockCache: Option[BlockCache.State],
                                                   segmentIO: SegmentIO,
                                                   forceSaveApplier: ForceSaveApplier): SegmentPutResult[Slice[PersistentSegment]] = {
    val transient: Iterable[TransientSegment] =
      SegmentRef.mergeWrite(
        oldKeyValuesCount = oldKeyValuesCount,
        oldKeyValues = oldKeyValues,
        headGap = headGap,
        tailGap = tailGap,
        mergeableCount = mergeableCount,
        mergeable = mergeable,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    val newSegments =
      Segment.persistent(
        pathsDistributor = pathsDistributor,
        mmap = segmentConfig.mmap,
        createdInLevel = createdInLevel,
        transient = transient
      )

    SegmentPutResult(result = newSegments, replaced = true)
  }

  /**
   * This refresh does not compute new sortedIndex size and uses existing. Saves an iteration
   * and performs refresh instantly.
   */
  def refreshForSameLevel(sortedIndexBlock: SortedIndexBlock,
                          valuesBlock: Option[ValuesBlock],
                          iterator: Iterator[Persistent],
                          keyValuesCount: Int,
                          removeDeletes: Boolean,
                          createdInLevel: Int,
                          valuesConfig: ValuesBlock.Config,
                          sortedIndexConfig: SortedIndexBlock.Config,
                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                          hashIndexConfig: HashIndexBlock.Config,
                          bloomFilterConfig: BloomFilterBlock.Config,
                          segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment] = {

    val sortedIndexSize =
      sortedIndexBlock.compressionInfo match {
        case Some(compressionInfo) =>
          compressionInfo.decompressedLength

        case None =>
          sortedIndexBlock.offset.size
      }

    val valuesSize =
      valuesBlock match {
        case Some(valuesBlock) =>
          valuesBlock.compressionInfo match {
            case Some(value) =>
              value.decompressedLength

            case None =>
              valuesBlock.offset.size
          }
        case None =>
          0
      }

    val keyValues =
      Segment
        .toMemoryIterator(iterator, removeDeletes)
        .to(Iterable)

    val mergeStats =
      new MergeStats.Persistent.Closed[Iterable](
        isEmpty = false,
        keyValuesCount = keyValuesCount,
        keyValues = keyValues,
        totalValuesSize = valuesSize,
        maxSortedIndexSize = sortedIndexSize
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

  def refreshForNewLevel(keyValues: Iterator[Persistent],
                         removeDeletes: Boolean,
                         createdInLevel: Int,
                         valuesConfig: ValuesBlock.Config,
                         sortedIndexConfig: SortedIndexBlock.Config,
                         binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                         hashIndexConfig: HashIndexBlock.Config,
                         bloomFilterConfig: BloomFilterBlock.Config,
                         segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment] = {
    val memoryKeyValues =
      Segment
        .toMemoryIterator(keyValues, removeDeletes)
        .to(Iterable)

    val builder =
      MergeStats
        .persistentBuilder(memoryKeyValues)
        .close(
          hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
          optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
        )

    SegmentBlock.writeOneOrMany(
      mergeStats = builder,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    )
  }

  def refreshForNewLevelPut(removeDeletes: Boolean,
                            createdInLevel: Int,
                            keyValues: Iterator[Persistent],
                            valuesConfig: ValuesBlock.Config,
                            sortedIndexConfig: SortedIndexBlock.Config,
                            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                            hashIndexConfig: HashIndexBlock.Config,
                            bloomFilterConfig: BloomFilterBlock.Config,
                            segmentConfig: SegmentBlock.Config,
                            pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                                keyOrder: KeyOrder[Slice[Byte]],
                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                functionStore: FunctionStore,
                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                fileSweeper: FileSweeper,
                                                                bufferCleaner: ByteBufferSweeperActor,
                                                                blockCache: Option[BlockCache.State],
                                                                segmentIO: SegmentIO,
                                                                forceSaveApplier: ForceSaveApplier): Slice[PersistentSegment] = {

    val transient: Iterable[TransientSegment] =
      Segment.refreshForNewLevel(
        keyValues = keyValues,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    Segment.persistent(
      pathsDistributor = pathsDistributor,
      mmap = segmentConfig.mmap,
      createdInLevel = createdInLevel,
      transient = transient
    )
  }

  def apply(path: Path,
            formatId: Byte,
            createdInLevel: Int,
            blockCacheFileId: Long,
            mmap: MMAP.Segment,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            segmentSize: Int,
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            nearestExpiryDeadline: Option[Deadline],
            copiedFrom: Option[PersistentSegment],
            checkExists: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                         timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore,
                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                         fileSweeper: FileSweeper,
                                         bufferCleaner: ByteBufferSweeperActor,
                                         blockCache: Option[BlockCache.State],
                                         segmentIO: SegmentIO,
                                         forceSaveApplier: ForceSaveApplier): PersistentSegment = {

    val file =
      mmap match {
        case _: MMAP.On | _: MMAP.ReadOnly =>
          DBFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = blockCacheFileId,
            autoClose = true,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Off =>
          DBFile.channelRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = blockCacheFileId,
            autoClose = true,
            checkExists = checkExists
          )
      }

    if (formatId == PersistentSegmentOne.formatId)
      copiedFrom match {
        case Some(one: PersistentSegmentOne) =>
          PersistentSegmentOne(
            file = file,
            createdInLevel = createdInLevel,
            minKey = minKey,
            maxKey = maxKey,
            segmentSize = segmentSize,
            minMaxFunctionId = minMaxFunctionId,
            nearestExpiryDeadline = nearestExpiryDeadline,
            valuesReaderCacheable = one.ref.segmentBlockCache.cachedValuesSliceReader(),
            sortedIndexReaderCacheable = one.ref.segmentBlockCache.cachedSortedIndexSliceReader(),
            hashIndexReaderCacheable = one.ref.segmentBlockCache.cachedHashIndexSliceReader(),
            binarySearchIndexReaderCacheable = one.ref.segmentBlockCache.cachedBinarySearchIndexSliceReader(),
            bloomFilterReaderCacheable = one.ref.segmentBlockCache.cachedBloomFilterSliceReader(),
            footerCacheable = one.ref.segmentBlockCache.cachedFooter()
          )

        case Some(segment: PersistentSegmentMany) =>
          throw new Exception(s"Invalid copy. Copied as ${PersistentSegmentOne.getClass.getSimpleName} but received ${segment.getClass.getSimpleName}.")

        case None =>
          PersistentSegmentOne(
            file = file,
            createdInLevel = createdInLevel,
            minKey = minKey,
            maxKey = maxKey,
            segmentSize = segmentSize,
            minMaxFunctionId = minMaxFunctionId,
            nearestExpiryDeadline = nearestExpiryDeadline,
            valuesReaderCacheable = None,
            sortedIndexReaderCacheable = None,
            hashIndexReaderCacheable = None,
            binarySearchIndexReaderCacheable = None,
            bloomFilterReaderCacheable = None,
            footerCacheable = None
          )
      }
    else if (formatId == PersistentSegmentMany.formatId)
      PersistentSegmentMany(
        file = file,
        createdInLevel = createdInLevel,
        minKey = minKey,
        maxKey = maxKey,
        segmentSize = segmentSize,
        minMaxFunctionId = minMaxFunctionId,
        nearestExpiryDeadline = nearestExpiryDeadline
      )
    else
      throw new Exception(s"Invalid segment formatId: $formatId")
  }

  /**
   * Reads the [[PersistentSegmentOne]] when the min, max keys & fileSize is not known.
   *
   * This function requires the Segment to be opened and read. After the Segment is successfully
   * read the file is closed.
   *
   * This function is only used for Appendix file recovery initialization.
   */
  def apply(path: Path,
            mmap: MMAP.Segment,
            checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore,
                                  blockCache: Option[BlockCache.State],
                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                  fileSweeper: FileSweeper,
                                  bufferCleaner: ByteBufferSweeperActor,
                                  forceSaveApplier: ForceSaveApplier): PersistentSegment = {

    implicit val segmentIO: SegmentIO = SegmentIO.defaultSynchronisedStoredIfCompressed
    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val file =
      mmap match {
        case _: MMAP.On | _: MMAP.ReadOnly =>
          DBFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = BlockCacheFileIDGenerator.next,
            autoClose = false,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Off =>
          DBFile.channelRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = BlockCacheFileIDGenerator.next,
            autoClose = false,
            checkExists = checkExists
          )
      }


    val formatId = file.get(0)

    val segment =
      if (formatId == PersistentSegmentOne.formatId)
        PersistentSegmentOne(file = file)
      else if (formatId == PersistentSegmentMany.formatId)
        PersistentSegmentMany(file = file)
      else
        throw new Exception(s"Invalid Segment formatId: $formatId")

    file.close()

    segment
  }

  def segmentSizeForMerge(segment: Segment): Int =
    segment match {
      case segment: MemorySegment =>
        segment.segmentSize

      case segment: PersistentSegmentOne =>
        segmentSizeForMerge(segment.ref)

      case segment: PersistentSegmentMany =>
        val listSegmentSize = segmentSizeForMerge(segment.listSegmentCache.value(()))

        //1+ for formatId
        segment.getAllSegmentRefs().foldLeft(1 + listSegmentSize) {
          case (size, ref) =>
            size + segmentSizeForMerge(ref)
        }
    }

  def segmentSizeForMerge(segment: SegmentRef): Int = {
    val footer = segment.getFooter()
    footer.sortedIndexOffset.size +
      footer.valuesOffset.map(_.size).getOrElse(0)
  }

  def keyOverlaps(keyValue: KeyValue,
                  segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    keyOverlaps(
      keyValue = keyValue,
      minKey = segment.minKey,
      maxKey = segment.maxKey
    )

  def keyOverlaps(keyValue: KeyValue,
                  minKey: Slice[Byte],
                  maxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
    import keyOrder._
    keyValue.key >= minKey && {
      if (maxKey.inclusive)
        keyValue.key <= maxKey.maxKey
      else
        keyValue.key < maxKey.maxKey
    }
  }

  def overlaps(assignable: Assignable,
               segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    overlaps(
      assignable = assignable,
      minKey = segment.minKey,
      maxKey = segment.maxKey
    )

  def overlaps(assignable: Assignable,
               minKey: Slice[Byte],
               maxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    assignable match {
      case keyValue: KeyValue =>
        keyOverlaps(
          keyValue = keyValue,
          minKey = minKey,
          maxKey = maxKey
        )

      case collection: Assignable.Collection =>
        overlaps(
          minKey = collection.key,
          maxKey = collection.maxKey.maxKey,
          maxKeyInclusive = collection.maxKey.inclusive,
          targetMinKey = minKey,
          targetMaxKey = maxKey
        )
    }

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    overlaps(
      minKey = minKey,
      maxKey = maxKey,
      maxKeyInclusive = maxKeyInclusive,
      targetMinKey = segment.minKey,
      targetMaxKey = segment.maxKey
    )

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               targetMinKey: Slice[Byte],
               targetMaxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey, maxKeyInclusive), (targetMinKey, targetMaxKey.maxKey, targetMaxKey.inclusive))

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments.exists(segment => overlaps(minKey, maxKey, maxKeyInclusive, segment))

  def overlaps(keyValues: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    keyValues match {
      case util.Left(value) =>
        overlaps(value, segments)

      case util.Right(value) =>
        overlaps(value, segments)
    }

  def overlaps(keyValues: Slice[Memory],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(keyValues) exists {
      case (minKey, maxKey, maxKeyInclusive) =>
        Segment.overlaps(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxKeyInclusive,
          segments = segments
        )
    }

  def overlaps(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(map) exists {
      case (minKey, maxKey, maxKeyInclusive) =>
        Segment.overlaps(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxKeyInclusive,
          segments = segments
        )
    }

  def overlaps(segment1: Segment,
               segment2: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((segment1.minKey, segment1.maxKey.maxKey, segment1.maxKey.inclusive), (segment2.minKey, segment2.maxKey.maxKey, segment2.maxKey.inclusive))

  def partitionOverlapping(segments1: Iterable[Segment],
                           segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): (Iterable[Segment], Iterable[Segment]) =
    segments1
      .partition(segmentToWrite => segments2.exists(existingSegment => Segment.overlaps(segmentToWrite, existingSegment)))

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    nonOverlapping(segments1, segments2, segments1.size)

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment],
                     count: Int)(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] = {
    if (count == 0)
      Iterable.empty
    else {
      val resultSegments = ListBuffer.empty[Segment]
      segments1 foreachBreak {
        segment1 =>
          if (!segments2.exists(segment2 => overlaps(segment1, segment2)))
            resultSegments += segment1
          resultSegments.size == count
      }
      resultSegments
    }
  }

  def overlaps(segments1: Iterable[Segment],
               segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    segments1.filter(segment1 => segments2.exists(segment2 => overlaps(segment1, segment2)))

  def overlaps(segment: Segment,
               segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments2.exists(segment2 => overlaps(segment, segment2))

  def intersects(segments1: Iterable[Segment], segments2: Iterable[Segment]): Boolean =
    if (segments1.isEmpty || segments2.isEmpty)
      false
    else
      segments1.exists(segment1 => segments2.exists(_.path == segment1.path))

  def intersects(segment: Segment, segments2: Iterable[Segment]): Boolean =
    segments2.exists(_.path == segment.path)

  def deleteSegments(segments: Iterable[Segment]): Int =
    segments.foldLeftRecover(0, failFast = false) {
      case (deleteCount, segment) =>
        segment.delete
        deleteCount + 1
    }

  def tempMinMaxKeyValues(segments: Iterable[Segment]): Slice[Memory] =
    segments.foldLeft(Slice.of[Memory](segments.size * 2)) {
      case (keyValues, segment) =>
        keyValues add Memory.Put(segment.minKey, Slice.Null, None, Time.empty)
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            keyValues add Memory.Put(maxKey, Slice.Null, None, Time.empty)

          case MaxKey.Range(fromKey, maxKey) =>
            keyValues add Memory.Range(fromKey, maxKey, Value.FromValue.Null, Value.Update(maxKey, None, Time.empty))
        }
    }

  @inline def tempMinMaxKeyValuesFrom[I](map: I, head: I => Option[Memory], last: I => Option[Memory]): Slice[Memory] = {
    for {
      minKey <- head(map).map(memory => Memory.Put(memory.key, Slice.Null, None, Time.empty))
      maxKey <- last(map) map {
        case fixed: Memory.Fixed =>
          Memory.Put(fixed.key, Slice.Null, None, Time.empty)

        case Memory.Range(fromKey, toKey, _, _) =>
          Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Update(Slice.Null, None, Time.empty))
      }
    } yield
      Slice(minKey, maxKey)
  } getOrElse Slice.of[Memory](0)

  def tempMinMaxKeyValues(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Slice[Memory] =
    tempMinMaxKeyValuesFrom[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]](map, _.head().toOptionS, _.last().toOptionS)

  def tempMinMaxKeyValues(keyValues: Slice[Memory]): Slice[Memory] =
    tempMinMaxKeyValuesFrom[Slice[Memory]](keyValues, _.headOption, _.lastOption)

  @inline def minMaxKeyFrom[I](input: I, head: I => Option[Memory], last: I => Option[Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- head(input).map(_.key)
      maxKey <- last(input) map {
        case fixed: Memory.Fixed =>
          (fixed.key, true)

        case range: Memory.Range =>
          (range.toKey, false)
      }
    } yield (minKey, maxKey._1, maxKey._2)

  def minMaxKey(keyValues: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    keyValues match {
      case util.Left(value) =>
        minMaxKey(value)

      case util.Right(value) =>
        minMaxKey(value)
    }

  @inline def minMaxKey(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    minMaxKeyFrom[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]](map, _.head().toOptionS, _.last().toOptionS)

  @inline def minMaxKey(map: Slice[Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    minMaxKeyFrom[Slice[Memory]](map, _.headOption, _.lastOption)

  def minMaxKey(segment: Iterable[Segment]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- segment.headOption.map(_.minKey)
      maxKey <- segment.lastOption.map(_.maxKey) map {
        case MaxKey.Fixed(maxKey) =>
          (maxKey, true)

        case MaxKey.Range(_, maxKey) =>
          (maxKey, false)
      }
    } yield {
      (minKey, maxKey._1, maxKey._2)
    }

  def minMaxKey(left: Iterable[Segment],
                right: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def minMaxKey(left: Iterable[Segment],
                right: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    right match {
      case util.Left(right) =>
        Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

      case util.Right(right) =>
        Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))
    }

  def minMaxKey(left: Iterable[Segment],
                right: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def minMaxKey(left: Iterable[Segment],
                right: Slice[Memory])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    if (busySegments.isEmpty)
      false
    else {
      val assignments =
        SegmentAssigner.assignMinMaxOnlyUnsafeNoGaps(
          inputSegments = inputSegments,
          targetSegments = appendixSegments
        )

      Segment.overlaps(
        segments1 = busySegments,
        segments2 = assignments
      ).nonEmpty
    }

  def overlapsWithBusySegments(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    if (busySegments.isEmpty)
      false
    else {
      for {
        head <- map.head().toOptionS
        last <- map.last().toOptionS
      } yield {
        val assignments =
          if (keyOrder.equiv(head.key, last.key))
            SegmentAssigner.assignUnsafeNoGaps(assignables = Slice(head), segments = appendixSegments)
          else
            SegmentAssigner.assignUnsafeNoGaps(assignables = Slice(head, last), segments = appendixSegments)

        Segment.overlaps(
          segments1 = busySegments,
          segments2 = assignments.map(_.segment)
        ).nonEmpty
      }
    } getOrElse false

  def getNearestPutDeadline(deadline: Option[Deadline],
                            next: KeyValue): Option[Deadline] =
    next match {
      case readOnly: KeyValue.Put =>
        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)

      case _: KeyValue.Remove =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.Update =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.PendingApply =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.Function =>
        deadline

      case range: KeyValue.Range =>
        range.fetchFromAndRangeValueUnsafe match {
          case (fromValue: Value.FromValue, _) =>
            getNearestPutDeadline(deadline, fromValue)

          case (Value.FromValue.Null, _) =>
            deadline
        }
    }

  def getNearestPutDeadline(deadline: Option[Deadline],
                            keyValue: Memory): Option[Deadline] =
    keyValue match {
      case writeOnly: Memory.Fixed =>
        FiniteDurations.getNearestDeadline(deadline, writeOnly.deadline)

      case range: Memory.Range =>
        (range.fromValue, range.rangeValue) match {
          case (fromValue: Value.FromValue, rangeValue) =>
            val fromValueDeadline = getNearestPutDeadline(deadline, fromValue)
            getNearestPutDeadline(fromValueDeadline, rangeValue)

          case (Value.FromValue.Null, rangeValue) =>
            getNearestPutDeadline(deadline, rangeValue)
        }
    }

  def getNearestPutDeadline(deadline: Option[Deadline],
                            keyValue: Value.FromValue): Option[Deadline] =
    keyValue match {
      case _: Value.RangeValue =>
        //        getNearestDeadline(deadline, rangeValue)
        deadline

      case put: Value.Put =>
        FiniteDurations.getNearestDeadline(deadline, put.deadline)
    }

  //  def getNearestDeadline(deadline: Option[Deadline],
  //                         rangeValue: Value.RangeValue): Option[Deadline] =
  //    rangeValue match {
  //      case remove: Value.Remove =>
  //        FiniteDurations.getNearestDeadline(deadline, remove.deadline)
  //      case update: Value.Update =>
  //        FiniteDurations.getNearestDeadline(deadline, update.deadline)
  //      case _: Value.Function =>
  //        deadline
  //      case pendingApply: Value.PendingApply =>
  //        FiniteDurations.getNearestDeadline(deadline, pendingApply.deadline)
  //    }

  //  def getNearestDeadline(previous: Option[Deadline],
  //                         applies: Slice[Value.Apply]): Option[Deadline] =
  //    applies.foldLeft(previous) {
  //      case (deadline, apply) =>
  //        getNearestDeadline(
  //          deadline = deadline,
  //          rangeValue = apply
  //        )
  //    }

  def getNearestDeadline(keyValues: Iterable[KeyValue]): Option[Deadline] =
    keyValues.foldLeftRecover(Option.empty[Deadline])(getNearestPutDeadline)

  def getNearestDeadlineSegment(previous: Segment,
                                next: Segment): SegmentOption =
    (previous.nearestPutDeadline, next.nearestPutDeadline) match {
      case (None, None) => Segment.Null
      case (Some(_), None) => previous
      case (None, Some(_)) => next
      case (Some(previousDeadline), Some(nextDeadline)) =>
        if (previousDeadline < nextDeadline)
          previous
        else
          next
    }

  def getNearestDeadlineSegment(segments: Iterable[Segment]): SegmentOption =
    segments.foldLeft(Segment.Null: SegmentOption) {
      case (previous, next) =>
        previous mapS {
          previous =>
            getNearestDeadlineSegment(previous, next)
        } getOrElse {
          if (next.nearestPutDeadline.isDefined)
            next
          else
            Segment.Null
        }
    }

  def toMemoryIterator(fullIterator: Iterator[KeyValue],
                       removeDeletes: Boolean): Iterator[Memory] =
    new Iterator[Memory] {

      var nextOne: Memory = _

      @tailrec
      final override def hasNext: Boolean =
        if (fullIterator.hasNext) {
          val nextKeyValue = fullIterator.next()
          val nextKeyValueOrNull =
            if (removeDeletes)
              SegmentGrouper.addLastLevel(nextKeyValue)
            else
              nextKeyValue.toMemory()

          if (nextKeyValueOrNull == null) {
            hasNext
          } else {
            nextOne = nextKeyValueOrNull
            true
          }
        } else {
          false
        }

      override def next(): Memory =
        nextOne
    }

  def hasOnlyOneSegment(segments: Iterable[Segment]): Boolean = {
    val iterator = segments.iterator
    iterator.hasNext && {
      iterator.next()
      !iterator.hasNext //no next segment.
    }
  }

  def runOnGapsParallel[A](headGap: Iterable[Assignable],
                           tailGap: Iterable[Assignable],
                           empty: A,
                           minGapSize: Int,
                           segmentParallelism: SegmentParallelism)(thunk: Iterable[Assignable] => A)(implicit ec: ExecutionContext): (Iterable[Assignable], Iterable[Assignable], Future[(A, A)]) =
    if (headGap.isEmpty && tailGap.isEmpty) {
      (headGap, tailGap, Future.successful((empty, empty)))
    } else {
      var availableThreads = segmentParallelism.parallelism

      def runConcurrentMayBe(gap: Iterable[Assignable]) =
        if (gap.size < minGapSize) {
          (gap, Future.successful(empty))
        } else if (availableThreads <= 0) {
          (Assignable.emptyIterable, Future.fromTry(Try(thunk(gap))))
        } else {
          availableThreads -= 1
          (Assignable.emptyIterable, Future(thunk(gap)))
        }

      val (head, headFuture: Future[A]) = runConcurrentMayBe(headGap)

      val (tail, tailFuture: Future[A]) = runConcurrentMayBe(tailGap)

      val future =
        headFuture flatMap {
          head =>
            tailFuture map {
              tail =>
                (head, tail)
            }
        }

      (head, tail, future)
    }
}

private[core] trait Segment extends FileSweeperItem with SegmentOption with Assignable.Collection { self =>

  final def key: Slice[Byte] =
    minKey

  def minKey: Slice[Byte]

  def maxKey: MaxKey[Slice[Byte]]

  def segmentSize: Int

  def nearestPutDeadline: Option[Deadline]

  def minMaxFunctionId: Option[MinMax[Slice[Byte]]]

  def formatId: Byte

  def createdInLevel: Int

  def path: Path

  def isMMAP: Boolean

  def segmentNumber: Long =
    Effect.numberFileId(path)._1

  def put(headGap: Iterable[Assignable],
          tailGap: Iterable[Assignable],
          mergeableCount: Int,
          mergeable: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          segmentParallelism: SegmentParallelism,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config,
          pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator,
                                                                                                           executionContext: ExecutionContext): SegmentPutResult[Slice[Segment]]

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment]

  def getFromCache(key: Slice[Byte]): KeyValueOption

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean

  def mightContainFunction(key: Slice[Byte]): Boolean

  def get(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def lower(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def higher(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def iterator(): Iterator[KeyValue]

  def delete: Unit

  def delete(delay: FiniteDuration): Unit

  def close: Unit

  def getKeyValueCount(): Int

  def clearCachedKeyValues(): Unit

  def clearAllCaches(): Unit

  def isInKeyValueCache(key: Slice[Byte]): Boolean

  def isKeyValueCacheEmpty: Boolean

  def areAllCachesEmpty: Boolean

  def cachedKeyValueSize: Int

  def hasRange: Boolean

  def hasPut: Boolean

  def isFooterDefined: Boolean

  def isOpen: Boolean

  def isFileDefined: Boolean

  def memory: Boolean

  def persistent: Boolean

  def existsOnDisk: Boolean

  def hasBloomFilter: Boolean

  override def isNoneS: Boolean =
    false

  override def getS: Segment =
    this

  override def equals(other: Any): Boolean =
    other match {
      case other: Segment =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()
}
