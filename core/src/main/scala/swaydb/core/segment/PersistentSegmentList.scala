/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.segment

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.cache.Cache
import swaydb.core.data.{KeyValue, Persistent, PersistentOptional}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.format.a.block.segment.list.SegmentListBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{Aggregator, IO}

import scala.concurrent.duration.Deadline

object PersistentSegmentList {
  def apply(file: DBFile,
            createdInLevel: Int,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            segmentSize: Int,
            nearestExpiryDeadline: Option[Deadline],
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                         functionStore: FunctionStore,
                                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                         blockCache: Option[BlockCache.State],
                                                         fileSweeper: FileSweeper.Enabled,
                                                         segmentIO: SegmentIO): PersistentSegment = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val ref =
      SegmentRef(
        path = file.path,
        minKey = minKey,
        maxKey = maxKey,
        blockRef = BlockRefReader(file, segmentSize),
        segmentIO = segmentIO,
        valuesReaderCacheable = valuesReaderCacheable,
        sortedIndexReaderCacheable = sortedIndexReaderCacheable,
        hashIndexReaderCacheable = hashIndexReaderCacheable,
        binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
        bloomFilterReaderCacheable = bloomFilterReaderCacheable,
        footerCacheable = footerCacheable
      )

    new PersistentSegment(
      file = file,
      createdInLevel = createdInLevel,
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestPutDeadline = nearestExpiryDeadline,
      ref = ref
    )
  }
}

private[segment] case class PersistentSegmentList(file: DBFile,
                                                  createdInLevel: Int,
                                                  mmapReads: Boolean,
                                                  mmapWrites: Boolean,
                                                  minKey: Slice[Byte],
                                                  maxKey: MaxKey[Slice[Byte]],
                                                  minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                                  segmentSize: Int,
                                                  nearestPutDeadline: Option[Deadline],
                                                  private[segment] val segments: Cache[Throwable, BlockRefReader[SegmentListBlock.Offset], SegmentRef])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                                        functionStore: FunctionStore,
                                                                                                                                                        blockCache: Option[BlockCache.State],
                                                                                                                                                        fileSweeper: FileSweeper.Enabled,
                                                                                                                                                        keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                                                                        segmentIO: SegmentIO) extends Segment with LazyLogging {

  //  implicit val segmentCacheImplicit: SegmentRef = ref
  //  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  //  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  //  implicit val segmentSearcher: SegmentSearcher = SegmentSearcher

  def path = file.path

  override def close: Unit = {
    file.close()
    //    ref.clearBlockCache()
    ???
  }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  def deleteSegmentsEventually =
    fileSweeper.delete(this)

  def delete: Unit = {
    logger.trace(s"{}: DELETING FILE", path)
    IO(file.delete()) onLeftSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure)
    } map {
      _ =>
        ???
      //        ref.clearBlockCache()
    }
  }

  def copyTo(toPath: Path): Path =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegment]]'s parent directory.
   */
  def put(newKeyValues: Slice[KeyValue],
          minSegmentSize: Int,
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config,
          pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment] = {

    //    val transient: Iterable[TransientSegment] =
    //      SegmentRef.put(
    //        ref = ref,
    //        newKeyValues = newKeyValues,
    //        minSegmentSize = minSegmentSize,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      )
    //
    //    Segment.persistent(
    //      pathsDistributor = pathsDistributor,
    //      mmapReads = mmapReads,
    //      mmapWrites = mmapWrites,
    //      createdInLevel = createdInLevel,
    //      segments = transient
    //    )
    ???
  }

  def refresh(minSegmentSize: Int,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment] = {

    //    val transient: Iterable[TransientSegment] =
    //      SegmentRef.refresh(
    //        ref = ref,
    //        minSegmentSize = minSegmentSize,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      )
    //
    //    Segment.persistent(
    //      pathsDistributor = pathsDistributor,
    //      mmapReads = mmapReads,
    //      mmapWrites = mmapWrites,
    //      createdInLevel = createdInLevel,
    //      segments = transient
    //    )
    ???
  }

  def getSegmentBlockOffset(): SegmentBlock.Offset =
    SegmentBlock.Offset(0, file.fileSize.toInt)

  def getFromCache(key: Slice[Byte]): PersistentOptional =
  //    ref getFromCache key
    ???

  def mightContainKey(key: Slice[Byte]): Boolean =
  //    ref mightContain key
    ???

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  def get(key: Slice[Byte], readState: ThreadReadState): PersistentOptional =
  //    SegmentRef.get(key, readState)
    ???

  def lower(key: Slice[Byte], readState: ThreadReadState): PersistentOptional =
  //    SegmentRef.lower(key, readState)
    ???

  def higher(key: Slice[Byte], readState: ThreadReadState): PersistentOptional =
  //    SegmentRef.higher(key, readState)
    ???

  def getAll[T](aggregator: Aggregator[KeyValue, T]): Unit =
  //    ref getAll aggregator
    ???

  override def getAll(): Slice[KeyValue] =
  //    ref.getAll()
    ???

  override def iterator(): Iterator[KeyValue] =
  //    ref.iterator()
    ???

  override def hasRange: Boolean =
  //    ref.hasRange
    ???

  override def hasPut: Boolean =
  //    ref.hasPut
    ???

  def getKeyValueCount(): Int =
  //    ref.getKeyValueCount()
    ???

  override def isFooterDefined: Boolean =
  //    ref.isFooterDefined
    ???

  def existsOnDisk: Boolean =
    file.existsOnDisk

  def memory: Boolean =
    false

  def persistent: Boolean =
    true

  def notExistsOnDisk: Boolean =
    !file.existsOnDisk

  def hasBloomFilter: Boolean =
  //    ref.hasBloomFilter
    ???

  def clearCachedKeyValues(): Unit =
  //    ref.clearCachedKeyValues()
    ???

  def clearAllCaches(): Unit = {
    clearCachedKeyValues()
    //    ref.clearBlockCache()
    ???
  }

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
  //    ref isInKeyValueCache key
    ???

  def isKeyValueCacheEmpty: Boolean =
  //    ref.isKeyValueCacheEmpty
    ???

  def areAllCachesEmpty: Boolean =
  //    ref.areAllCachesEmpty
    ???

  def cachedKeyValueSize: Int =
  //    ref.cacheSize
    ???
}
