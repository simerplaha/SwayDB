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

package swaydb.core.segment

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{DBFile, ForceSaveApplier}
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.BlockCache
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.reader.BlockRefReader
import swaydb.core.segment.block.segment.data.{TransientSegment, TransientSegmentSerialiser}
import swaydb.core.segment.block.segment.{SegmentBlockConfig, SegmentBlockOffset}
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.defrag.DefragPersistentSegment
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.segment.ref.{SegmentRef, SegmentRefOption, SegmentRefReader}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util._
import swaydb.core.util.skiplist.SkipListTreeMap
import swaydb.data.MaxKey
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.config.{MMAP, SegmentRefCacheLife}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.effect.{Effect, Extension}

import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

protected case object PersistentSegmentMany extends LazyLogging {

  val formatId: Byte = 127
  val formatIdSlice: Slice[Byte] = Slice(formatId)

  def apply(file: DBFile,
            segmentRefCacheLife: SegmentRefCacheLife,
            segment: TransientSegment.Many)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                            timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore,
                                            keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                            blockCacheSweeper: Option[MemorySweeper.Block],
                                            fileSweeper: FileSweeper,
                                            bufferCleaner: ByteBufferSweeperActor,
                                            segmentIO: SegmentReadIO,
                                            forceSaveApplier: ForceSaveApplier): PersistentSegmentMany = {

    val segmentsCache = new ConcurrentSkipListMap[Slice[Byte], SegmentRef](keyOrder)

    val listSegmentSize = segment.listSegment.segmentSize

    val firstSegmentOffset = segment.fileHeader.size + listSegmentSize

    val cacheBlocksOnCreate = segment.listSegment.sortedIndexUnblockedReader.isDefined

    //iterate Segments and transfer cache if necessary.
    segment
      .segments
      .foldLeft(firstSegmentOffset) {
        case (actualOffset, singleton) =>
          val thisSegmentSize = singleton.segmentSize

          def cacheSegmentRef(blockCache: Option[BlockCache.State]) = {
            val blockRef =
              BlockRefReader(
                file = file,
                start = actualOffset,
                fileSize = thisSegmentSize,
                blockCache = blockCache
              )

            val pathNameOffset = actualOffset - firstSegmentOffset

            val ref =
              SegmentRef(
                path = file.path.resolve(s"ref.$pathNameOffset"),
                minKey = singleton.minKey,
                maxKey = singleton.maxKey,
                nearestPutDeadline = singleton.nearestPutDeadline,
                minMaxFunctionId = singleton.minMaxFunctionId,
                blockRef = blockRef,
                segmentIO = segmentIO,
                updateCount = singleton.updateCount,
                rangeCount = singleton.rangeCount,
                putCount = singleton.putCount,
                putDeadlineCount = singleton.putDeadlineCount,
                keyValueCount = singleton.keyValueCount,
                createdInLevel = singleton.createdInLevel,
                valuesReaderCacheable = singleton.valuesUnblockedReader,
                sortedIndexReaderCacheable = singleton.sortedIndexUnblockedReader,
                hashIndexReaderCacheable = singleton.hashIndexUnblockedReader,
                binarySearchIndexReaderCacheable = singleton.binarySearchUnblockedReader,
                bloomFilterReaderCacheable = singleton.bloomFilterUnblockedReader,
                footerCacheable = singleton.footerUnblocked
              )

            segmentsCache.put(ref.minKey, ref)
          }

          singleton match {
            case remote: TransientSegment.RemoteRef =>
              cacheSegmentRef {
                remote.ref.blockCache() orElse BlockCache.forSearch(maxCacheSizeOrZero = thisSegmentSize, blockSweeper = blockCacheSweeper)
              }

            case _: TransientSegment.One =>
              if (cacheBlocksOnCreate)
                cacheSegmentRef(BlockCache.forSearch(maxCacheSizeOrZero = thisSegmentSize, blockSweeper = blockCacheSweeper))
          }

          actualOffset + thisSegmentSize
      }

    val listSegmentBlockCache = BlockCache.forSearch(maxCacheSizeOrZero = listSegmentSize, blockSweeper = blockCacheSweeper)

    val listSegment =
      if (cacheBlocksOnCreate)
        Some(
          SegmentRef(
            path = file.path,
            minKey = segment.listSegment.minKey,
            maxKey = segment.listSegment.maxKey,
            nearestPutDeadline = segment.listSegment.nearestPutDeadline,
            minMaxFunctionId = segment.listSegment.minMaxFunctionId,
            blockRef =
              BlockRefReader(
                file = file,
                start = segment.fileHeader.size,
                fileSize = listSegmentSize,
                blockCache = listSegmentBlockCache
              ),
            segmentIO = segmentIO,
            updateCount = segment.listSegment.updateCount,
            rangeCount = segment.listSegment.rangeCount,
            putCount = segment.listSegment.putCount,
            putDeadlineCount = segment.listSegment.putDeadlineCount,
            keyValueCount = segment.listSegment.keyValueCount,
            createdInLevel = segment.listSegment.createdInLevel,
            valuesReaderCacheable = segment.listSegment.valuesUnblockedReader,
            sortedIndexReaderCacheable = segment.listSegment.sortedIndexUnblockedReader,
            hashIndexReaderCacheable = segment.listSegment.hashIndexUnblockedReader,
            binarySearchIndexReaderCacheable = segment.listSegment.binarySearchUnblockedReader,
            bloomFilterReaderCacheable = segment.listSegment.bloomFilterUnblockedReader,
            footerCacheable = segment.listSegment.footerUnblocked
          )
        )
      else
        None

    val listSegmentCache =
      Cache.noIO[Unit, SegmentRef](synchronised = true, stored = true, initial = listSegment) {
        (_, _) =>
          initListSegment(
            file = file,
            segmentSize = segment.segmentSize,
            minKey = segment.listSegment.minKey,
            maxKey = segment.listSegment.maxKey,
            updateCount = segment.listSegment.updateCount,
            rangeCount = segment.listSegment.rangeCount,
            putCount = segment.listSegment.putCount,
            putDeadlineCount = segment.listSegment.putDeadlineCount,
            keyValueCount = segment.listSegment.keyValueCount,
            createdInLevel = segment.listSegment.createdInLevel,
            listSegmentBlockCache = listSegmentBlockCache
          )
      }

    PersistentSegmentMany(
      file = file,
      createdInLevel = segment.createdInLevel,
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      minMaxFunctionId = segment.minMaxFunctionId,
      segmentSize = segment.segmentSize,
      nearestPutDeadline = segment.nearestPutDeadline,
      updateCount = segment.updateCount,
      rangeCount = segment.rangeCount,
      putCount = segment.putCount,
      putDeadlineCount = segment.putDeadlineCount,
      keyValueCount = segment.keyValueCount,
      listSegmentCache = listSegmentCache,
      segmentRefCacheLife = segmentRefCacheLife,
      segmentsCache = segmentsCache
    )
  }

  def apply(file: DBFile,
            segmentSize: Int,
            createdInLevel: Int,
            segmentRefCacheLife: SegmentRefCacheLife,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            nearestExpiryDeadline: Option[Deadline],
            updateCount: Int,
            rangeCount: Int,
            putCount: Int,
            putDeadlineCount: Int,
            keyValueCount: Int,
            copiedFrom: Option[PersistentSegmentMany])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                       functionStore: FunctionStore,
                                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                       blockCacheSweeper: Option[MemorySweeper.Block],
                                                       fileSweeper: FileSweeper,
                                                       bufferCleaner: ByteBufferSweeperActor,
                                                       segmentIO: SegmentReadIO,
                                                       forceSaveApplier: ForceSaveApplier): PersistentSegmentMany = {

    val segmentsCache = new ConcurrentSkipListMap[Slice[Byte], SegmentRef](keyOrder)

    copiedFrom foreach {
      copiedFrom =>
        copiedFrom.segmentsCache forEach {
          (offset, oldRef) =>

            val oldRefOffset = oldRef.offset()

            val ref =
              SegmentRef(
                path = file.path.resolve(s"ref.$offset"),
                minKey = oldRef.minKey,
                maxKey = oldRef.maxKey,
                nearestPutDeadline = oldRef.nearestPutDeadline,
                minMaxFunctionId = oldRef.minMaxFunctionId,
                blockRef =
                  BlockRefReader(
                    file = file,
                    start = oldRefOffset.start,
                    fileSize = oldRefOffset.size,
                    blockCache = oldRef.blockCache() orElse BlockCache.forSearch(oldRefOffset.size, blockCacheSweeper)
                  ),
                segmentIO = segmentIO,
                updateCount = oldRef.updateCount,
                rangeCount = oldRef.rangeCount,
                putCount = oldRef.putCount,
                putDeadlineCount = oldRef.putDeadlineCount,
                keyValueCount = oldRef.keyValueCount,
                createdInLevel = oldRef.createdInLevel,
                valuesReaderCacheable = oldRef.segmentBlockCache.cachedValuesSliceReader(),
                sortedIndexReaderCacheable = oldRef.segmentBlockCache.cachedSortedIndexSliceReader(),
                hashIndexReaderCacheable = oldRef.segmentBlockCache.cachedHashIndexSliceReader(),
                binarySearchIndexReaderCacheable = oldRef.segmentBlockCache.cachedBinarySearchIndexSliceReader(),
                bloomFilterReaderCacheable = oldRef.segmentBlockCache.cachedBloomFilterSliceReader(),
                footerCacheable = oldRef.segmentBlockCache.cachedFooter()
              )

            segmentsCache.put(ref.minKey, ref)
        }
    }

    val copiedFromListSegmentCache =
      copiedFrom match {
        case Some(copiedFrom) =>
          copiedFrom.listSegmentCache.get() match {
            case Some(copiedFromListRef) =>
              val copiedFromOffset = copiedFromListRef.offset()

              val ref =
                SegmentRef(
                  path = file.path,
                  minKey = copiedFromListRef.minKey,
                  maxKey = copiedFromListRef.maxKey,
                  nearestPutDeadline = copiedFromListRef.nearestPutDeadline,
                  minMaxFunctionId = copiedFromListRef.minMaxFunctionId,
                  blockRef =
                    BlockRefReader(
                      file = file,
                      start = copiedFromOffset.start,
                      fileSize = copiedFromOffset.size,
                      blockCache = copiedFromListRef.blockCache() orElse BlockCache.forSearch(copiedFromOffset.size, blockCacheSweeper)
                    ),
                  segmentIO = segmentIO,
                  updateCount = copiedFrom.updateCount,
                  rangeCount = copiedFrom.rangeCount,
                  putCount = copiedFrom.putCount,
                  putDeadlineCount = copiedFrom.putDeadlineCount,
                  keyValueCount = copiedFrom.keyValueCount,
                  createdInLevel = copiedFrom.createdInLevel,
                  valuesReaderCacheable = copiedFromListRef.segmentBlockCache.cachedValuesSliceReader(),
                  sortedIndexReaderCacheable = copiedFromListRef.segmentBlockCache.cachedSortedIndexSliceReader(),
                  hashIndexReaderCacheable = copiedFromListRef.segmentBlockCache.cachedHashIndexSliceReader(),
                  binarySearchIndexReaderCacheable = copiedFromListRef.segmentBlockCache.cachedBinarySearchIndexSliceReader(),
                  bloomFilterReaderCacheable = copiedFromListRef.segmentBlockCache.cachedBloomFilterSliceReader(),
                  footerCacheable = copiedFromListRef.segmentBlockCache.cachedFooter()
                )

              Some(ref)


            case None =>
              None
          }

        case None =>
          None
      }

    val listSegmentBlockCache =
      copiedFromListSegmentCache match {
        case Some(copiedFromListSegmentCache) =>
          copiedFromListSegmentCache.blockCache() match {
            case blockCache @ Some(_) =>
              blockCache

            case None =>
              BlockCache.forSearch(copiedFromListSegmentCache.offset().size, blockCacheSweeper)
          }

        case None =>
          None
      }

    val listSegmentCache =
      Cache.noIO[Unit, SegmentRef](synchronised = true, stored = true, initial = copiedFromListSegmentCache) {
        (_, _) =>
          //NOTE - the counts are incorrect here. They are segment level counts instead of listSegment levels.
          //       but listSegment's stats are not used. ListSegment is just for reference.
          initListSegment(
            file = file,
            segmentSize = segmentSize,
            minKey = minKey,
            maxKey = maxKey,
            updateCount = 0,
            rangeCount = 0,
            putCount = 0,
            putDeadlineCount = 0,
            keyValueCount = 0,
            createdInLevel = 0,
            listSegmentBlockCache = listSegmentBlockCache
          )
      }

    new PersistentSegmentMany(
      file = file,
      createdInLevel = createdInLevel,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestPutDeadline = nearestExpiryDeadline,
      updateCount = updateCount,
      rangeCount = rangeCount,
      putCount = putCount,
      putDeadlineCount = putDeadlineCount,
      keyValueCount = keyValueCount,
      listSegmentCache = listSegmentCache,
      segmentRefCacheLife = segmentRefCacheLife,
      segmentsCache = segmentsCache
    )
  }

  /**
   * Used for recovery only - [[swaydb.core.level.tool.AppendixRepairer]] - Not performance optimised.
   *
   * Used when Segment's information is unknown.
   */
  def apply(file: DBFile,
            segmentRefCacheLife: SegmentRefCacheLife)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                      blockCacheSweeper: Option[MemorySweeper.Block],
                                                      fileSweeper: FileSweeper,
                                                      bufferCleaner: ByteBufferSweeperActor,
                                                      segmentIO: SegmentReadIO,
                                                      forceSaveApplier: ForceSaveApplier): PersistentSegmentMany = {

    val fileExtension = Effect.fileExtension(file.path)

    if (fileExtension != Extension.Seg)
      throw new Exception(s"Invalid Segment file extension: $fileExtension")

    val segmentSize = file.fileSize.toInt

    val listSegment: SegmentRef =
      initListSegment(
        file = file,
        segmentSize = segmentSize,
        minKey = null,
        maxKey = null,
        updateCount = 0,
        rangeCount = 0,
        putCount = 0,
        putDeadlineCount = 0,
        keyValueCount = 0,
        createdInLevel = 0,
        listSegmentBlockCache = None,
      )

    val listSegmentFooter = listSegment.getFooter()

    val segmentRefKeyValues =
      listSegment
        .iterator(false)
        .toList

    val segmentRefs =
      parseSkipList(
        file = file,
        segmentSize = segmentSize,
        minKey = null,
        maxKey = null,
        initialiseIteratorsInOneSeek = false
      )

    val lastSegment =
      segmentRefs.last() match {
        case SegmentRef.Null =>
          throw new Exception("Empty List Segment read. List Segment are non-empty lists.")

        case ref: SegmentRef =>
          ref
      }

    val lastKeyValue =
      lastSegment
        .iterator(false)
        .foldLeft(Persistent.Null: PersistentOption) {
          case (_, next) =>
            next
        }

    val maxKey =
      lastKeyValue match {
        case fixed: Persistent.Fixed =>
          MaxKey.Fixed(fixed.key.unslice())

        case range: Persistent.Range =>
          MaxKey.Range(range.fromKey.unslice(), range.toKey.unslice())

        case Persistent.Null =>
          throw new Exception("Empty Segment read. Persisted Segments cannot be empty.")
      }

    val allKeyValues = segmentRefs.values().flatMap(_.iterator(false))

    val deadlineFunctionId = DeadlineAndFunctionId(allKeyValues)

    val minKey = segmentRefKeyValues.head.key.unslice()

    val listSegmentUpdateCount = listSegmentFooter.updateCount
    val listSegmentRangeCount = listSegmentFooter.rangeCount
    val listSegmentPutCount = listSegmentFooter.putCount
    val listSegmentPutDeadlineCount = listSegmentFooter.putDeadlineCount
    val listSegmentKeyValueCount = listSegmentFooter.keyValueCount
    val listSegmentCreatedInLevel = listSegmentFooter.createdInLevel

    val listSegmentCache =
      Cache.noIO[Unit, SegmentRef](synchronised = true, stored = true, initial = None) {
        case (_, _) =>
          initListSegment(
            file = file,
            segmentSize = segmentSize,
            minKey = minKey,
            maxKey = maxKey,
            updateCount = listSegmentUpdateCount,
            rangeCount = listSegmentRangeCount,
            putCount = listSegmentPutCount,
            putDeadlineCount = listSegmentPutDeadlineCount,
            keyValueCount = listSegmentKeyValueCount,
            createdInLevel = listSegmentCreatedInLevel,
            listSegmentBlockCache = None
          )
      }

    PersistentSegmentMany(
      file = file,
      createdInLevel = listSegmentFooter.createdInLevel,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = deadlineFunctionId.minMaxFunctionId.map(_.unslice()),
      segmentSize = file.fileSize.toInt,
      nearestPutDeadline = deadlineFunctionId.nearestDeadline,
      updateCount = segmentRefs.values().foldLeft(0)(_ + _.updateCount),
      rangeCount = segmentRefs.values().foldLeft(0)(_ + _.rangeCount),
      putCount = segmentRefs.values().foldLeft(0)(_ + _.putCount),
      putDeadlineCount = segmentRefs.values().foldLeft(0)(_ + _.putDeadlineCount),
      keyValueCount = segmentRefs.values().foldLeft(0)(_ + _.keyValueCount),
      listSegmentCache = listSegmentCache,
      segmentRefCacheLife = segmentRefCacheLife,
      //above parsed segmentRefs cannot be used here because
      //it's MaxKey.Range's minKey is set to the Segment's minKey
      //instead of the Segment's last range key-values minKey.
      segmentsCache = new ConcurrentSkipListMap[Slice[Byte], SegmentRef](keyOrder)
    )
  }

  private def parseSkipList(file: DBFile,
                            segmentSize: Int,
                            minKey: Slice[Byte],
                            maxKey: MaxKey[Slice[Byte]],
                            initialiseIteratorsInOneSeek: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                   blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                   segmentIO: SegmentReadIO): SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef] = {
    val blockedReader = Reader(file).moveTo(1)
    val listSegmentSize = blockedReader.readUnsignedInt()
    val listSegment = blockedReader.read(listSegmentSize)
    val listSegmentRef = BlockRefReader[SegmentBlockOffset](listSegment)

    val temporarySegmentRef =
      SegmentRef(
        path = file.path,
        minKey = minKey,
        maxKey = maxKey,
        nearestPutDeadline = None,
        minMaxFunctionId = None,
        blockRef = listSegmentRef,
        segmentIO = segmentIO,
        updateCount = 0,
        rangeCount = 0,
        putCount = 0,
        putDeadlineCount = 0,
        keyValueCount = 0,
        createdInLevel = Int.MaxValue,
        valuesReaderCacheable = None,
        sortedIndexReaderCacheable = None,
        hashIndexReaderCacheable = None,
        binarySearchIndexReaderCacheable = None,
        bloomFilterReaderCacheable = None,
        footerCacheable = None
      )

    val skipList = SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef](Slice.Null, SegmentRef.Null)


    //this will also clear all the SegmentRef's
    //            blockCacheMemorySweeper foreach {
    //              cacheMemorySweeper =>
    //                cacheMemorySweeper.add(listSegmentSize, self)
    //            }

    val tailSegmentBytesFromOffset = blockedReader.getPosition
    var previousPath: Path = null
    var previousSegmentRef: SegmentRef = null

    temporarySegmentRef.iterator(initialiseIteratorsInOneSeek) foreach {
      keyValue =>

        val nextSegmentRef =
          keyValue match {
            case range: Persistent.Range =>
              TransientSegmentSerialiser.toSegmentRef(
                file = file,
                firstSegmentStartOffset = tailSegmentBytesFromOffset,
                range = range,
                valuesReaderCacheable = None,
                sortedIndexReaderCacheable = None,
                hashIndexReaderCacheable = None,
                binarySearchIndexReaderCacheable = None,
                bloomFilterReaderCacheable = None,
                footerCacheable = None
              )

            case put: Persistent.Put =>
              TransientSegmentSerialiser.toSegmentRef(
                file = file,
                firstSegmentStartOffset = tailSegmentBytesFromOffset,
                put = put,
                valuesReaderCacheable = None,
                sortedIndexReaderCacheable = None,
                hashIndexReaderCacheable = None,
                binarySearchIndexReaderCacheable = None,
                bloomFilterReaderCacheable = None,
                footerCacheable = None
              )

            case _: Persistent.Fixed =>
              throw new Exception("Non put key-value written to List segment")
          }

        val segmentRef =
          if (previousPath == nextSegmentRef.path)
            previousSegmentRef
          else
            nextSegmentRef

        previousPath = segmentRef.path
        previousSegmentRef = segmentRef

        skipList.put(segmentRef.minKey, segmentRef)
    }

    skipList
  }

  private def initListSegment(file: DBFile,
                              segmentSize: Int,
                              minKey: Slice[Byte],
                              maxKey: MaxKey[Slice[Byte]],
                              updateCount: Int,
                              rangeCount: Int,
                              putCount: Int,
                              putDeadlineCount: Int,
                              keyValueCount: Int,
                              createdInLevel: Int,
                              listSegmentBlockCache: Option[BlockCache.State])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                               timeOrder: TimeOrder[Slice[Byte]],
                                                                               functionStore: FunctionStore,
                                                                               keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                               blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                               segmentIO: SegmentReadIO): SegmentRef = {
    val fileReader = Reader(file).moveTo(1)
    val listSegmentSize = fileReader.readUnsignedInt()

    val listSegmentRef =
      BlockRefReader(
        file = file,
        start = fileReader.getPosition,
        fileSize = listSegmentSize,
        blockCache = listSegmentBlockCache orElse BlockCache.forSearch(maxCacheSizeOrZero = listSegmentSize, blockSweeper = blockCacheMemorySweeper)
      )

    SegmentRef(
      path = file.path,
      minKey = minKey,
      maxKey = maxKey,
      //ListSegment does not store deadline. This is stored at the higher Level.
      nearestPutDeadline = None,
      minMaxFunctionId = None,
      blockRef = listSegmentRef,
      segmentIO = segmentIO,
      updateCount = updateCount,
      rangeCount = rangeCount,
      putCount = putCount,
      putDeadlineCount = putDeadlineCount,
      keyValueCount = keyValueCount,
      createdInLevel = createdInLevel,
      valuesReaderCacheable = None,
      sortedIndexReaderCacheable = None,
      hashIndexReaderCacheable = None,
      binarySearchIndexReaderCacheable = None,
      bloomFilterReaderCacheable = None,
      footerCacheable = None
    )
  }
}

protected case class PersistentSegmentMany(file: DBFile,
                                           createdInLevel: Int,
                                           minKey: Slice[Byte],
                                           maxKey: MaxKey[Slice[Byte]],
                                           minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                           segmentSize: Int,
                                           nearestPutDeadline: Option[Deadline],
                                           updateCount: Int,
                                           rangeCount: Int,
                                           putCount: Int,
                                           putDeadlineCount: Int,
                                           keyValueCount: Int,
                                           listSegmentCache: CacheNoIO[Unit, SegmentRef],
                                           segmentRefCacheLife: SegmentRefCacheLife,
                                           private val segmentsCache: ConcurrentSkipListMap[Slice[Byte], SegmentRef])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                      functionStore: FunctionStore,
                                                                                                                      blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                                                      fileSweeper: FileSweeper,
                                                                                                                      bufferCleaner: ByteBufferSweeperActor,
                                                                                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                                      segmentIO: SegmentReadIO,
                                                                                                                      forceSaveApplier: ForceSaveApplier) extends PersistentSegment with LazyLogging {

  override def formatId: Byte = PersistentSegmentMany.formatId

  private def fetchSegmentRef(persistent: Persistent,
                              listSegment: SegmentRef): SegmentRef = {
    val minKey = TransientSegmentSerialiser.minKey(persistent)
    val segment: SegmentRef = segmentsCache.get(minKey)

    if (segment == null) {
      val firstSegmentStartOffset: Int = 1 + Bytes.sizeOfUnsignedInt(listSegment.segmentSize) + listSegment.segmentSize

      val segment =
        TransientSegmentSerialiser.toSegmentRef(
          file = file,
          firstSegmentStartOffset = firstSegmentStartOffset,
          persistent = persistent,
          valuesReaderCacheable = None,
          sortedIndexReaderCacheable = None,
          hashIndexReaderCacheable = None,
          binarySearchIndexReaderCacheable = None,
          bloomFilterReaderCacheable = None,
          footerCacheable = None
        )

      if (!segmentRefCacheLife.isDiscard) {
        val existingSegment = segmentsCache.putIfAbsent(segment.minKey, segment)

        if (existingSegment == null) {

          if (segmentRefCacheLife.isTemporary) {
            val sweeper = blockCacheSweeper orElse keyValueMemorySweeper
            if (sweeper.isDefined)
              sweeper.get.add(
                key = segment.minKey,
                weight = segment.segmentSize,
                cache = segmentsCache
              )
            else
              logger.error(s"${SegmentRefCacheLife.productPrefix} is ${SegmentRefCacheLife.Temporary.productPrefix} but no sweeper available.")
          }

          segment
        } else {
          existingSegment
        }
      } else {
        segment
      }
    } else {
      segment
    }
  }

  @inline def segmentRefs(initialiseIteratorsInOneSeek: Boolean): Iterator[SegmentRef] =
    new Iterator[SegmentRef] {
      //TODO - do not read sortedIndexBlock if the SegmentRef is already cached in-memory.
      var nextRef: SegmentRef = _
      val listSegment = listSegmentCache.value(())
      val iter = listSegment.iterator(initialiseIteratorsInOneSeek)
      var nextInvoked = false

      @tailrec
      final override def hasNext: Boolean =
        if (iter.hasNext)
          if (nextRef == null) {
            nextRef = fetchSegmentRef(iter.next(), listSegment)
            nextInvoked = false
            true
          } else if (nextInvoked) {
            val nextNextRef = fetchSegmentRef(iter.next(), listSegment)
            if (nextRef == nextNextRef) {
              hasNext
            } else {
              nextRef = nextNextRef
              nextInvoked = false
              true
            }
          } else {
            true
          }
        else if (!nextInvoked && nextRef != null) //if it's the last Segment.
          true
        else
          false

      override def next(): SegmentRef = {
        nextInvoked = true
        nextRef
      }
    }

  def path = file.path

  override def close: Unit = {
    file.close()
    segmentsCache.values().forEach(_.clearAllCaches())
    segmentsCache.clear()
    listSegmentCache.clear()
  }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  def delete(delay: FiniteDuration) = {
    val deadline = delay.fromNow
    if (deadline.isOverdue())
      this.delete
    else
      fileSweeper send FileSweeper.Command.Delete(this, deadline)
  }

  def delete: Unit = {
    logger.trace(s"{}: DELETING FILE", path)
    IO(file.delete()) onLeftSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure.value.exception)
    } map {
      _ =>
        segmentsCache.forEach {
          (_, ref) =>
            ref.clearAllCaches()
        }
    }
  }

  def copyTo(toPath: Path): Path =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegmentOne]]'s parent directory.
   */
  def put(headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          tailGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          newKeyValues: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlockConfig,
          sortedIndexConfig: SortedIndexBlockConfig,
          binarySearchIndexConfig: BinarySearchIndexBlockConfig,
          hashIndexConfig: HashIndexBlockConfig,
          bloomFilterConfig: BloomFilterBlockConfig,
          segmentConfig: SegmentBlockConfig,
          pathsDistributor: PathsDistributor,
          segmentRefCacheLife: SegmentRefCacheLife,
          mmap: MMAP.Segment)(implicit idGenerator: IDGenerator,
                              executionContext: ExecutionContext,
                              compactionIO: CompactionIO.Actor,
                              compactionParallelism: CompactionParallelism): Future[DefIO[PersistentSegmentOption, Iterable[PersistentSegment]]] = {
    implicit val valuesConfigImplicit: ValuesBlockConfig = valuesConfig
    implicit val sortedIndexConfigImplicit: SortedIndexBlockConfig = sortedIndexConfig
    implicit val binarySearchIndexConfigImplicit: BinarySearchIndexBlockConfig = binarySearchIndexConfig
    implicit val hashIndexConfigImplicit: HashIndexBlockConfig = hashIndexConfig
    implicit val bloomFilterConfigImplicit: BloomFilterBlockConfig = bloomFilterConfig
    implicit val segmentConfigImplicit: SegmentBlockConfig = segmentConfig

    DefragPersistentSegment.runMany(
      headGap = headGap,
      tailGap = tailGap,
      segment = this,
      newKeyValues = newKeyValues,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      pathsDistributor = pathsDistributor,
      segmentRefCacheLife = segmentRefCacheLife,
      mmap = mmap
    )
  }

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlockConfig,
              sortedIndexConfig: SortedIndexBlockConfig,
              binarySearchIndexConfig: BinarySearchIndexBlockConfig,
              hashIndexConfig: HashIndexBlockConfig,
              bloomFilterConfig: BloomFilterBlockConfig,
              segmentConfig: SegmentBlockConfig)(implicit idGenerator: IDGenerator,
                                                 ec: ExecutionContext,
                                                 compactionParallelism: CompactionParallelism): Future[DefIO[PersistentSegmentMany, Slice[TransientSegment.OneOrRemoteRefOrMany]]] =
    Segment.refreshForNewLevel(
      keyValues = iterator(segmentConfig.initialiseIteratorsInOneSeek),
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    ) map {
      refreshed =>
        DefIO(
          input = this,
          output = refreshed
        )
    }

  def getFromCache(key: Slice[Byte]): PersistentOption = {
    segmentsCache.forEach {
      (_, ref) =>
        val got = ref.getFromCache(key)
        if (got.isSomeS)
          return got
    }

    Persistent.Null
  }

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean = {
    val listSegment = listSegmentCache.value(())

    listSegment.get(key, threadState) match {
      case _: Persistent =>
        true

      case Persistent.Null =>
        listSegment.lower(key, threadState) match {
          case persistent: Persistent =>
            fetchSegmentRef(persistent, listSegment).mightContainKey(key, threadState)

          case Persistent.Null =>
            false
        }
    }
  }

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  def get(key: Slice[Byte], threadState: ThreadReadState): PersistentOption = {
    val floorOrNull = segmentsCache.floorEntry(key)

    if (floorOrNull != null && SegmentRefReader.contains(key, floorOrNull.getValue)) {
      floorOrNull.getValue.get(key, threadState)
    } else {
      val listSegment = listSegmentCache.value(())

      listSegment.get(key, threadState) match {
        case persistent: Persistent =>
          fetchSegmentRef(persistent, listSegment).get(key, threadState)

        case Persistent.Null =>
          Persistent.Null
      }
    }
  }

  def lower(key: Slice[Byte], threadState: ThreadReadState): PersistentOption = {
    val lowerOrNull = segmentsCache.lowerEntry(key)

    if (lowerOrNull != null && SegmentRefReader.containsLower(key, lowerOrNull.getValue)) {
      lowerOrNull.getValue.lower(key, threadState)
    } else {
      val listSegment = listSegmentCache.value(())

      listSegment.lower(key, threadState) match {
        case persistent: Persistent =>
          fetchSegmentRef(persistent, listSegment).lower(key, threadState)

        case Persistent.Null =>
          Persistent.Null
      }
    }
  }

  private def higherFromListSegment(key: Slice[Byte], threadState: ThreadReadState): PersistentOption = {
    val listSegment = listSegmentCache.value(())

    listSegment.higher(key, threadState) match {
      case segmentKeyValue: Persistent =>
        fetchSegmentRef(segmentKeyValue, listSegment).higher(key, threadState)

      case Persistent.Null =>
        Persistent.Null
    }
  }

  def higher(key: Slice[Byte], threadState: ThreadReadState): PersistentOption = {
    val floorOrNull = segmentsCache.floorEntry(key)

    val higherFromFloorSegment =
      if (floorOrNull != null)
        floorOrNull.getValue.higher(key = key, threadState = threadState)
      else
        Persistent.Null

    if (higherFromFloorSegment.isNoneS)
      higherFromListSegment(key = key, threadState = threadState)
    else
      higherFromFloorSegment
  }

  override def iterator(inOneSeek: Boolean): Iterator[Persistent] =
    segmentRefs(inOneSeek).flatMap(_.iterator(inOneSeek))

  override def isFooterDefined: Boolean =
    segmentsCache.asScala.values.exists(_.isFooterDefined)

  def existsOnDisk: Boolean =
    file.existsOnDisk

  def memory: Boolean =
    false

  def persistent: Boolean =
    true

  def notExistsOnDisk: Boolean =
    !file.existsOnDisk

  def hasBloomFilter: Boolean =
    segmentRefs(false).exists(_.hasBloomFilter)

  def clearCachedKeyValues(): Unit =
    segmentsCache
      .asScala
      .values
      .foreach(_.clearCachedKeyValues())

  def clearAllCaches(): Unit = {
    clearCachedKeyValues()
    segmentsCache.values().forEach(_.clearAllCaches())
    segmentsCache.clear()
    listSegmentCache.clear()
  }

  def isInKeyValueCache(key: Slice[Byte]): Boolean = {
    segmentsCache.forEach {
      (_, ref) =>
        if (ref.isInKeyValueCache(key))
          return true
    }

    false
  }

  def isKeyValueCacheEmpty: Boolean =
    segmentsCache
      .asScala
      .values
      .forall(_.isKeyValueCacheEmpty)

  def areAllCachesEmpty: Boolean =
    segmentsCache.isEmpty && !listSegmentCache.isCached

  def cachedKeyValueSize: Int =
    segmentsCache
      .asScala
      .values
      .foldLeft(0)(_ + _.cachedKeyValueSize)
}
