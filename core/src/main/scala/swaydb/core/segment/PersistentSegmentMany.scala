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
import swaydb.core.segment.format.a.block.segment.data.{TransientSegment, TransientSegmentSerialiser}
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util._
import swaydb.data.config.{Dir, IOAction}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.data.{MaxKey, Reserve}
import swaydb.{Error, IO}

import scala.concurrent.duration.Deadline
import scala.jdk.CollectionConverters._

protected object PersistentSegmentMany {

  val formatId: Byte = 127
  val formatIdSlice: Slice[Byte] = Slice(formatId)

  def apply(file: DBFile,
            createdInLevel: Int,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            segment: TransientSegment.Many)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                            timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore,
                                            keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                            blockCache: Option[BlockCache.State],
                                            fileSweeper: FileSweeper.Enabled,
                                            segmentIO: SegmentIO): PersistentSegmentMany = {
    val initial =
      if (segment.segments.isEmpty) {
        None
      } else {
        val skipList = SkipList.immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef](Slice.Null, SegmentRef.Null)
        implicit val blockMemorySweeper = blockCache.map(_.sweeper)

        val firstSegmentOffset =
          segment.headerSize +
            segment.segments.head.segmentSize

        //drop head ignoring the list block.
        segment
          .segments
          .dropHead()
          .foldLeft(firstSegmentOffset) {
            case (offset, one) =>
              val thisSegmentSize = one.segmentSize

              val blockRef =
                BlockRefReader(
                  file = file,
                  start = offset,
                  fileSize = thisSegmentSize
                )

              val ref =
                SegmentRef(
                  path = file.path.resolve(s".ref.$offset"),
                  minKey = one.minKey,
                  maxKey = one.maxKey,
                  blockRef = blockRef,
                  segmentIO = segmentIO,
                  valuesReaderCacheable = one.valuesUnblockedReader,
                  sortedIndexReaderCacheable = one.sortedIndexUnblockedReader,
                  hashIndexReaderCacheable = one.hashIndexUnblockedReader,
                  binarySearchIndexReaderCacheable = one.binarySearchUnblockedReader,
                  bloomFilterReaderCacheable = one.bloomFilterUnblockedReader,
                  footerCacheable = one.footerUnblocked
                )

              skipList.put(one.minKey, ref)

              offset + thisSegmentSize
          }

        Some(skipList)
      }

    PersistentSegmentMany(
      file = file,
      createdInLevel = createdInLevel,
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      minMaxFunctionId = segment.minMaxFunctionId,
      segmentSize = segment.segmentSize,
      nearestExpiryDeadline = segment.nearestDeadline,
      initial = initial
    )
  }

  def apply(file: DBFile,
            segmentSize: Int,
            createdInLevel: Int,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            nearestExpiryDeadline: Option[Deadline],
            initial: Option[SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                   functionStore: FunctionStore,
                                                                                                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                                   blockCache: Option[BlockCache.State],
                                                                                                                   fileSweeper: FileSweeper.Enabled,
                                                                                                                   segmentIO: SegmentIO): PersistentSegmentMany = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val fileBlockRef: BlockRefReader[SegmentBlock.Offset] =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = segmentSize - 1
      )

    val segments =
      Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Unit, SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef]](
        initial = initial,
        strategy = _ => segmentIO.segmentBlockIO(IOAction.ReadDataOverview).forceCacheOnAccess,
        reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"${file.path}: ${this.getClass.getSimpleName}"))
      ) {
        (initial, self) => //initial set clean up.
          blockCacheMemorySweeper foreach {
            cacheMemorySweeper =>
              cacheMemorySweeper.add(initial.size * 500, self)
          }
      } {
        (_, self) =>
          IO {

            val blockedReader: BlockRefReader[SegmentBlock.Offset] = fileBlockRef.copy()
            val listSegmentSize = blockedReader.readUnsignedInt()
            val listSegment = blockedReader.read(listSegmentSize)
            val listSegmentRef = BlockRefReader[SegmentBlock.Offset](listSegment)

            val segmentRef =
              SegmentRef(
                path = file.path,
                minKey = minKey,
                maxKey = maxKey,
                blockRef = listSegmentRef,
                segmentIO = segmentIO,
                valuesReaderCacheable = None,
                sortedIndexReaderCacheable = None,
                hashIndexReaderCacheable = None,
                binarySearchIndexReaderCacheable = None,
                bloomFilterReaderCacheable = None,
                footerCacheable = None
              )

            val skipList = SkipList.immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef](Slice.Null, SegmentRef.Null)


            //this will also clear all the SegmentRef's
            //            blockCacheMemorySweeper foreach {
            //              cacheMemorySweeper =>
            //                cacheMemorySweeper.add(listSegmentSize, self)
            //            }

            val tailSegmentBytesFromOffset = blockedReader.getPosition
            val tailManySegmentsSize = fileBlockRef.size.toInt - tailSegmentBytesFromOffset

            segmentRef.iterator() foreach {
              case range: Persistent.Range =>
                range.unsliceKeys

                val thisSegmentBlockRef =
                  BlockRefReader[SegmentBlock.Offset](
                    ref = fileBlockRef.copy(),
                    start = tailSegmentBytesFromOffset,
                    size = tailManySegmentsSize
                  )

                val segmentRef =
                  TransientSegmentSerialiser.toSegmentRef(
                    path = file.path,
                    reader = thisSegmentBlockRef,
                    range = range,
                    valuesReaderCacheable = None,
                    sortedIndexReaderCacheable = None,
                    hashIndexReaderCacheable = None,
                    binarySearchIndexReaderCacheable = None,
                    bloomFilterReaderCacheable = None,
                    footerCacheable = None
                  )

                skipList.put(segmentRef.minKey, segmentRef)

              case _: Persistent.Put =>
              //ignore. Put is stored so that it's possible to perform binary search but currently binary search is not required.

              case _: Persistent.Fixed =>
                throw new Exception("Non put key-value written to List segment")
            }

            skipList
          }
      }

    new PersistentSegmentMany(
      file = file,
      createdInLevel = createdInLevel,
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestPutDeadline = nearestExpiryDeadline,
      segments = segments
    )
  }
}

protected case class PersistentSegmentMany(file: DBFile,
                                           createdInLevel: Int,
                                           mmapReads: Boolean,
                                           mmapWrites: Boolean,
                                           minKey: Slice[Byte],
                                           maxKey: MaxKey[Slice[Byte]],
                                           minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                           segmentSize: Int,
                                           nearestPutDeadline: Option[Deadline],
                                           private[segment] val segments: Cache[Error.Segment, Unit, SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                                                                            timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                                                                            functionStore: FunctionStore,
                                                                                                                                                                                            blockCache: Option[BlockCache.State],
                                                                                                                                                                                            fileSweeper: FileSweeper.Enabled,
                                                                                                                                                                                            keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                                                                                                            segmentIO: SegmentIO) extends PersistentSegment with LazyLogging {

  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val segmentSearcher: SegmentSearcher = SegmentSearcher

  override def formatId: Byte = PersistentSegmentMany.formatId

  private def skipList: SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef] =
    segments
      .value(())
      .get

  private def segmentRefs: Iterable[SegmentRef] =
    skipList
      .values()
      .asScala

  def path = file.path

  override def close: Unit = {
    file.close()
    segments.clear()
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
        segments.clear()
    }
  }

  def copyTo(toPath: Path): Path =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegmentOne]]'s parent directory.
   */
  def put(newKeyValues: Slice[KeyValue],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config,
          pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[PersistentSegment] = {

    val transient: Iterable[TransientSegment] =
      SegmentRef.put(
        oldKeyValuesCount = getKeyValueCount(),
        oldKeyValues = iterator(),
        newKeyValues = newKeyValues,
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
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      createdInLevel = createdInLevel,
      segments = transient
    )
  }

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[PersistentSegment] = {

    val transient: Iterable[TransientSegment] =
      SegmentRef.refreshForNewLevel(
        keyValues = iterator(),
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
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      createdInLevel = createdInLevel,
      segments = transient
    )
  }

  def getFromCache(key: Slice[Byte]): PersistentOptional =
    skipList
      .floor(key)
      .flatMapSomeS(Persistent.Null: PersistentOptional)(_.getFromCache(key))

  def mightContainKey(key: Slice[Byte]): Boolean =
    skipList
      .floor(key)
      .existsS(_.mightContain(key))

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  def get(key: Slice[Byte], threadState: ThreadReadState): PersistentOptional =
    skipList
      .floor(key)
      .flatMapSomeS(Persistent.Null: PersistentOptional) {
        implicit ref =>
          SegmentRef.get(
            key = key,
            threadState = threadState
          )
      }

  def lower(key: Slice[Byte], threadState: ThreadReadState): PersistentOptional =
    skipList
      .floor(key)
      .flatMapSomeS(Persistent.Null: PersistentOptional) {
        implicit ref =>
          SegmentRef.lower(
            key = key,
            threadState = threadState
          )
      }

  def higher(key: Slice[Byte], threadState: ThreadReadState): PersistentOptional =
    skipList
      .floor(key)
      .flatMapSomeS(Persistent.Null: PersistentOptional) {
        implicit ref =>
          SegmentRef.higher(
            key = key,
            threadState = threadState
          )
      }

  override def toSlice(): Slice[Persistent] =
    Segment.getAllKeyValuesRef(segmentRefs)

  override def iterator(): Iterator[Persistent] =
    segmentRefs.foldLeft(Iterator.empty[Persistent]) {
      case (iterator, segment) =>
        iterator ++ segment.iterator()
    }

  override def hasRange: Boolean =
    segmentRefs.exists(_.hasRange)

  override def hasPut: Boolean =
    segmentRefs.exists(_.hasPut)

  def getKeyValueCount(): Int =
    segmentRefs.foldLeft(0)(_ + _.getKeyValueCount())

  override def isFooterDefined: Boolean =
    segmentRefs.exists(_.isFooterDefined)

  def existsOnDisk: Boolean =
    file.existsOnDisk

  def memory: Boolean =
    false

  def persistent: Boolean =
    true

  def notExistsOnDisk: Boolean =
    !file.existsOnDisk

  def hasBloomFilter: Boolean =
    segmentRefs.exists(_.hasBloomFilter)

  def clearCachedKeyValues(): Unit =
    segmentRefs.foreach(_.clearCachedKeyValues())

  def clearAllCaches(): Unit = {
    clearCachedKeyValues()
    segmentRefs.foreach(_.clearBlockCache())
  }

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    skipList
      .floor(key)
      .forallS(_.isInKeyValueCache(key))

  def isKeyValueCacheEmpty: Boolean =
    segmentRefs.forall(_.isKeyValueCacheEmpty)

  def areAllCachesEmpty: Boolean =
    segmentRefs.forall(_.areAllCachesEmpty)

  def cachedKeyValueSize: Int =
    segmentRefs.foldLeft(0)(_ + _.cacheSize)
}
