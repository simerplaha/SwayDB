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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{FileSweeperItem, MemorySweeper}
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile, Effect, ForceSaveApplier}
import swaydb.core.level.PathsDistributor
import swaydb.core.map.Map
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
import swaydb.core.util.skiplist.SkipList
import swaydb.data.MaxKey
import swaydb.data.config.{Dir, MMAP}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.{FiniteDurations, SomeOrNone}

import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline
import scala.jdk.CollectionConverters._

private[swaydb] sealed trait SegmentOption extends SomeOrNone[SegmentOption, Segment] {
  override def noneS: SegmentOption =
    Segment.Null
}

private[core] object Segment extends LazyLogging {

  final case object Null extends SegmentOption {
    override def isNoneS: Boolean = true

    override def getS: Segment = throw new Exception("Segment is of type Null")
  }

  val emptyIterable = Iterable.empty[Segment]

  def memory(minSegmentSize: Int,
             maxKeyValueCountPerSegment: Int,
             pathsDistributor: PathsDistributor,
             createdInLevel: Long,
             keyValues: MergeStats.Memory.Closed[Iterable])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                            timeOrder: TimeOrder[Slice[Byte]],
                                                            functionStore: FunctionStore,
                                                            fileSweeper: FileSweeperActor,
                                                            idGenerator: IDGenerator): Slice[MemorySegment] =
    if (keyValues.isEmpty) {
      throw IO.throwable("Empty key-values submitted to memory Segment.")
    } else {
      val segments = ListBuffer.empty[MemorySegment]

      var skipList = SkipList.treeMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      var minMaxFunctionId: Option[MinMax[Slice[Byte]]] = None
      var nearestDeadline: Option[Deadline] = None
      var hasRange: Boolean = false
      var hasPut: Boolean = false
      var currentSegmentSize = 0
      var currentSegmentKeyValuesCount = 0
      var minKey: Slice[Byte] = null
      var lastKeyValue: Memory = null

      def setClosed(): Unit = {
        skipList = SkipList.treeMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
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
        val path = pathsDistributor.next.resolve(IDGenerator.segmentId(idGenerator.nextID))

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

      keyValues.keyValues foreach {
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
                                                                     fileSweeper: FileSweeperActor,
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
      segments = transient
    )
  }

  def persistent(pathsDistributor: PathsDistributor,
                 createdInLevel: Int,
                 mmap: MMAP.Segment,
                 segments: Iterable[TransientSegment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                       functionStore: FunctionStore,
                                                       fileSweeper: FileSweeperActor,
                                                       bufferCleaner: ByteBufferSweeperActor,
                                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                       blockCache: Option[BlockCache.State],
                                                       segmentIO: SegmentIO,
                                                       idGenerator: IDGenerator,
                                                       forceSaveApplier: ForceSaveApplier): Slice[PersistentSegment] =
    segments.mapRecover(
      block =
        segment =>
          if (segment.isEmpty) {
            //This is fatal!! Empty Segments should never be created. If this does have for whatever reason it should
            //not be allowed so that whatever is creating this Segment (eg: compaction) does not progress with a success response.
            throw IO.throwable("Empty key-values submitted to persistent Segment.")
          } else {
            val path = pathsDistributor.next.resolve(IDGenerator.segmentId(idGenerator.nextID))

            val file: DBFile =
              segmentFile(
                mmap = mmap,
                segmentBytes = segment.segmentBytes,
                path = path
              )

            segment match {
              case segment: TransientSegment.One =>
                PersistentSegmentOne(
                  file = file,
                  createdInLevel = createdInLevel,
                  segment = segment
                )

              case segment: TransientSegment.Many =>
                PersistentSegmentMany(
                  file = file,
                  createdInLevel = createdInLevel,
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
                          segmentBytes: Slice[Slice[Byte]])(implicit segmentIO: SegmentIO,
                                                            fileSweeper: FileSweeperActor,
                                                            bufferCleaner: ByteBufferSweeperActor,
                                                            blockCache: Option[BlockCache.State],
                                                            forceSaveApplier: ForceSaveApplier): DBFile =
    mmap match {
      case MMAP.Enabled(deleteAfterClean, forceSave) => //if both read and writes are mmaped. Keep the file open.
        DBFile.mmapWriteAndRead(
          path = path,
          fileOpenIOStrategy = segmentIO.fileOpenIO,
          autoClose = true,
          deleteAfterClean = deleteAfterClean,
          forceSave = forceSave,
          blockCacheFileId = BlockCacheFileIDGenerator.nextID,
          bytes = segmentBytes
        )

      case MMAP.ReadOnly(deleteAfterClean) =>
        DBFile.mmapRead(
          path = Effect.write(path, segmentBytes),
          fileOpenIOStrategy = segmentIO.fileOpenIO,
          blockCacheFileId = BlockCacheFileIDGenerator.nextID,
          autoClose = true,
          deleteAfterClean = deleteAfterClean
        )

      case _: MMAP.Disabled =>
        DBFile.channelRead(
          path = Effect.write(path, segmentBytes),
          fileOpenIOStrategy = segmentIO.fileOpenIO,
          blockCacheFileId = BlockCacheFileIDGenerator.nextID,
          autoClose = true
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
                                                        fileSweeper: FileSweeperActor,
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        blockCache: Option[BlockCache.State],
                                                        segmentIO: SegmentIO,
                                                        idGenerator: IDGenerator,
                                                        forceSaveApplier: ForceSaveApplier): Slice[Segment] =
    segment match {
      case segment: PersistentSegment =>
        val nextPath = pathsDistributor.next.resolve(IDGenerator.segmentId(idGenerator.nextID))

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
                                                        fileSweeper: FileSweeperActor,
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        blockCache: Option[BlockCache.State],
                                                        segmentIO: SegmentIO,
                                                        idGenerator: IDGenerator,
                                                        forceSaveApplier: ForceSaveApplier): Slice[PersistentSegment] = {
    val builder =
      if (removeDeletes)
        MergeStats.persistent[Memory, ListBuffer](ListBuffer.newBuilder)(SegmentGrouper.addLastLevel)
      else
        MergeStats.persistent[Memory, ListBuffer](ListBuffer.newBuilder)

    keyValues foreach builder.add

    val closedStats =
      builder close sortedIndexConfig.enableAccessPositionIndex

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
                                                    fileSweeper: FileSweeperActor,
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
                                        fileSweeper: FileSweeperActor,
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
      keyValues = builder
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
                                         fileSweeper: FileSweeperActor,
                                         bufferCleaner: ByteBufferSweeperActor,
                                         blockCache: Option[BlockCache.State],
                                         segmentIO: SegmentIO,
                                         forceSaveApplier: ForceSaveApplier): PersistentSegment = {

    val file =
      mmap match {
        case _: MMAP.Enabled | _: MMAP.ReadOnly =>
          DBFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = blockCacheFileId,
            autoClose = true,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Disabled =>
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
        nearestExpiryDeadline = nearestExpiryDeadline,
        //todo initial should be set for many copied Segment.
        initial = None
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
                                  fileSweeper: FileSweeperActor,
                                  bufferCleaner: ByteBufferSweeperActor,
                                  forceSaveApplier: ForceSaveApplier): PersistentSegment = {

    implicit val segmentIO: SegmentIO = SegmentIO.defaultSynchronisedStoredIfCompressed
    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val file =
      mmap match {
        case _: MMAP.Enabled | _: MMAP.ReadOnly =>
          DBFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = BlockCacheFileIDGenerator.nextID,
            autoClose = false,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Disabled =>
          DBFile.channelRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            blockCacheFileId = BlockCacheFileIDGenerator.nextID,
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
        val footer = segment.ref.getFooter()
        footer.sortedIndexOffset.size +
          footer.valuesOffset.map(_.size).getOrElse(0)
    }

  def belongsTo(keyValue: KeyValue,
                segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
    import keyOrder._
    keyValue.key >= segment.minKey && {
      if (segment.maxKey.inclusive)
        keyValue.key <= segment.maxKey.maxKey
      else
        keyValue.key < segment.maxKey.maxKey
    }
  }

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey, maxKeyInclusive), (segment.minKey, segment.maxKey.maxKey, segment.maxKey.inclusive))

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments.exists(segment => overlaps(minKey, maxKey, maxKeyInclusive, segment))

  def overlaps(map: Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
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

  /**
   * Pre condition: Segments should be sorted with their minKey in ascending order.
   */
  def getAllKeyValues(segments: Iterable[Segment]): Slice[KeyValue] =
    if (segments.isEmpty) {
      Slice.empty
    } else if (segments.size == 1) {
      segments.head.toSlice()
    } else {
      val totalKeyValues =
        segments.foldLeftRecover(0) {
          case (total, segment) =>
            segment.getKeyValueCount() + total
        }

      val aggregator = Slice.newAggregator[KeyValue](totalKeyValues)

      segments foreach {
        segment =>
          segment.iterator() foreach aggregator.add
      }

      aggregator.result
    }

  def getAllKeyValuesRef(segments: Iterable[SegmentRef]): Slice[Persistent] =
    if (segments.isEmpty) {
      Slice.empty
    } else if (segments.size == 1) {
      segments.head.toSlice()
    } else {
      val totalKeyValues =
        segments.foldLeftRecover(0) {
          case (total, segment) =>
            segment.getKeyValueCount() + total
        }

      val aggregator = Slice.newAggregator[Persistent](totalKeyValues)

      segments foreach {
        segment =>
          segment.iterator() foreach aggregator.add
      }

      aggregator.result
    }

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

  def tempMinMaxKeyValues(map: Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Slice[Memory] = {
    for {
      minKey <- map.head().mapS(memory => Memory.Put(memory.key, Slice.Null, None, Time.empty))
      maxKey <- map.last() mapS {
        case fixed: Memory.Fixed =>
          Memory.Put(fixed.key, Slice.Null, None, Time.empty)

        case Memory.Range(fromKey, toKey, _, _) =>
          Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Update(Slice.Null, None, Time.empty))
      }
    } yield
      Slice(minKey, maxKey)
  } getOrElse Slice.of[Memory](0)

  def minMaxKey(map: Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- map.head().mapS(_.key)
      maxKey <- map.last() mapS {
        case fixed: Memory.Fixed =>
          (fixed.key, true)

        case range: Memory.Range =>
          (range.toKey, false)
      }
    } yield (minKey, maxKey._1, maxKey._2)

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
                right: Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    if (busySegments.isEmpty)
      false
    else {
      val assignments =
        SegmentAssigner.assignMinMaxOnlyUnsafe(
          inputSegments = inputSegments,
          targetSegments = appendixSegments
        )

      Segment.overlaps(
        segments1 = busySegments,
        segments2 = assignments
      ).nonEmpty
    }

  def overlapsWithBusySegments(map: Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
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
            SegmentAssigner.assignUnsafe(keyValues = Slice(head), segments = appendixSegments)
          else
            SegmentAssigner.assignUnsafe(keyValues = Slice(head, last), segments = appendixSegments)

        Segment.overlaps(
          segments1 = busySegments,
          segments2 = assignments.keys
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
              nextKeyValue.toMemory

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
}

private[core] trait Segment extends FileSweeperItem with SegmentOption { self =>

  val minKey: Slice[Byte]
  val maxKey: MaxKey[Slice[Byte]]
  val segmentSize: Int
  val nearestPutDeadline: Option[Deadline]
  val minMaxFunctionId: Option[MinMax[Slice[Byte]]]

  def formatId: Byte

  def createdInLevel: Int

  def path: Path

  def isMMAP: Boolean

  def segmentId: Long =
    Effect.numberFileId(path)._1

  def put(newKeyValues: Slice[KeyValue],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config,
          pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment]

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

  def mightContainKey(key: Slice[Byte]): Boolean

  def mightContainFunction(key: Slice[Byte]): Boolean

  def get(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def lower(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def higher(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def toSlice(): Slice[KeyValue]

  def iterator(): Iterator[KeyValue]

  def delete: Unit

  def deleteSegmentsEventually: Unit

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
}
