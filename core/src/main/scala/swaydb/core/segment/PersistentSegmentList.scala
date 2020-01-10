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
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.segment.data.{TransientSegment, TransientSegmentConverter}
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util._
import swaydb.data.config.{Dir, IOAction}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.data.{MaxKey, Reserve}
import swaydb.{Aggregator, ForEach, Error, IO}

import scala.concurrent.duration.Deadline

protected object PersistentSegmentList {

  val formatId: Byte = 127

  def apply(file: DBFile,
            segmentSize: Int,
            createdInLevel: Int,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            nearestExpiryDeadline: Option[Deadline],
            segments: Slice[TransientSegment.One],
            initial: Option[SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                   functionStore: FunctionStore,
                                                                                                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                                   blockCache: Option[BlockCache.State],
                                                                                                                   fileSweeper: FileSweeper.Enabled,
                                                                                                                   segmentIO: SegmentIO): PersistentSegmentList = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val fileBlockRef: BlockRefReader[SegmentBlock.Offset] =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = segmentSize
      )

    val segmentsListCache =
      Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Unit, SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef]](
        initial = initial,
        strategy = _ => segmentIO.segmentBlockIO(IOAction.ReadDataOverview).forceCacheOnAccess,
        reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"${file.path}: ${this.getClass.getSimpleName}"))
      ) {
        (initial, self) => //initial set clean up.
          keyValueMemorySweeper foreach {
            cacheMemorySweeper =>
              //            cacheMemorySweeper.add(initial.foldLeft(0), self)
              ???
          }
      } {
        (_, _) =>
          IO {

            val blockedReader: BlockRefReader[SegmentBlock.Offset] = fileBlockRef.copy()
            val segmentSize = blockedReader.readUnsignedInt()
            val segment = fileBlockRef.read(segmentSize)
            val segmentBlockRef = BlockRefReader[SegmentBlock.Offset](segment)

            val segmentRef =
              SegmentRef(
                path = file.path,
                minKey = minKey,
                maxKey = maxKey,
                blockRef = segmentBlockRef,
                segmentIO = segmentIO,
                valuesReaderCacheable = None,
                sortedIndexReaderCacheable = None,
                hashIndexReaderCacheable = None,
                binarySearchIndexReaderCacheable = None,
                bloomFilterReaderCacheable = None,
                footerCacheable = None
              )

            val skipList = SkipList.immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef](Slice.Null, SegmentRef.Null)

            val foreach: ForEach[Persistent] = {
              case range: Persistent.Range =>
                val segmentRef =
                  TransientSegmentConverter.toSegmentRef(
                    path = file.path,
                    reader = blockedReader,
                    range = range,
                    valuesReaderCacheable = None,
                    sortedIndexReaderCacheable = None,
                    hashIndexReaderCacheable = None,
                    binarySearchIndexReaderCacheable = None,
                    bloomFilterReaderCacheable = None,
                    footerCacheable = None
                  )

                skipList.put(minKey, segmentRef)

              case _: Persistent.Put =>
              //ignore. Put is stored so that it's possible to perform binary search but currently binary search is not required.

              case _: Persistent.Fixed =>
                throw new Exception("Non put key-value written to List segment")
            }

            segmentRef getAll foreach

            skipList
          }
      }

    new PersistentSegmentList(
      file = file,
      createdInLevel = createdInLevel,
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestPutDeadline = nearestExpiryDeadline,
      segmentList = segmentsListCache
    )
  }
}

protected case class PersistentSegmentList(file: DBFile,
                                           createdInLevel: Int,
                                           mmapReads: Boolean,
                                           mmapWrites: Boolean,
                                           minKey: Slice[Byte],
                                           maxKey: MaxKey[Slice[Byte]],
                                           minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                           segmentSize: Int,
                                           nearestPutDeadline: Option[Deadline],
                                           private[segment] val segmentList: Cache[Error.Segment, Unit, SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef]])(implicit keyOrder: KeyOrder[Slice[Byte]],
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
