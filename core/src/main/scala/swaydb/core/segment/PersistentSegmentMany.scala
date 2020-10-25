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
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile, Effect, ForceSaveApplier}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.segment.data.{TransientSegment, TransientSegmentSerialiser}
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util._
import swaydb.core.util.skiplist.SkipListTreeMap
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.data.compaction.ParallelMerge.SegmentParallelism
import swaydb.data.config.{Dir, IOAction}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.{MaxKey, Reserve}
import swaydb.{Error, IO}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}

protected case object PersistentSegmentMany {

  val formatId: Byte = 127
  val formatIdSlice: Slice[Byte] = Slice(formatId)

  def apply(file: DBFile,
            createdInLevel: Int,
            segment: TransientSegment.Many)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                            timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore,
                                            keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                            blockCache: Option[BlockCache.State],
                                            fileSweeper: FileSweeper,
                                            bufferCleaner: ByteBufferSweeperActor,
                                            segmentIO: SegmentIO,
                                            forceSaveApplier: ForceSaveApplier): PersistentSegmentMany = {
    val initial =
      if (segment.segments.isEmpty) {
        None
      } else {
        val skipList = SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef](Slice.Null, SegmentRef.Null)
        implicit val blockMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

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
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      minMaxFunctionId = segment.minMaxFunctionId,
      segmentSize = segment.segmentSize,
      nearestExpiryDeadline = segment.nearestPutDeadline,
      initial = initial
    )
  }

  def apply(file: DBFile,
            segmentSize: Int,
            createdInLevel: Int,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            nearestExpiryDeadline: Option[Deadline],
            initial: Option[SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                            timeOrder: TimeOrder[Slice[Byte]],
                                                                                                            functionStore: FunctionStore,
                                                                                                            keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                            blockCache: Option[BlockCache.State],
                                                                                                            fileSweeper: FileSweeper,
                                                                                                            bufferCleaner: ByteBufferSweeperActor,
                                                                                                            segmentIO: SegmentIO,
                                                                                                            forceSaveApplier: ForceSaveApplier): PersistentSegmentMany = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val fileBlockRef: BlockRefReader[SegmentBlock.Offset] =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = segmentSize - 1
      )

    val segments =
      Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Unit, SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef]](
        initial = initial,
        strategy = _ => segmentIO.segmentBlockIO(IOAction.ReadDataOverview).forceCacheOnAccess,
        reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"${file.path}: ${this.productPrefix}"))
      )() {
        (_, _) =>
          IO {
            parseSkipList(
              file = file,
              minKey = minKey,
              maxKey = maxKey,
              fileBlockRef = fileBlockRef
            )
          }
      }

    new PersistentSegmentMany(
      file = file,
      createdInLevel = createdInLevel,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestPutDeadline = nearestExpiryDeadline,
      segmentsCache = segments
    )
  }

  /**
   * Used for recovery only - [[swaydb.core.level.tool.AppendixRepairer]] - Not performance optimised.
   *
   * Used when Segment's information is unknown.
   */
  def apply(file: DBFile)(implicit keyOrder: KeyOrder[Slice[Byte]],
                          timeOrder: TimeOrder[Slice[Byte]],
                          functionStore: FunctionStore,
                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                          blockCacheMemorySweeper: Option[MemorySweeper.Block],
                          blockCache: Option[BlockCache.State],
                          fileSweeper: FileSweeper,
                          bufferCleaner: ByteBufferSweeperActor,
                          segmentIO: SegmentIO,
                          forceSaveApplier: ForceSaveApplier): PersistentSegmentMany = {

    val fileExtension = Effect.fileExtension(file.path)

    if (fileExtension != Extension.Seg)
      throw new Exception(s"Invalid Segment file extension: $fileExtension")

    val fileBlockRef: BlockRefReader[SegmentBlock.Offset] =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = file.fileSize.toInt - 1
      )

    val listSegment =
      parseListSegment(
        file = file,
        minKey = null,
        maxKey = null,
        fileBlockRef = fileBlockRef
      )

    val footer = listSegment.getFooter()

    val segmentRefKeyValues =
      listSegment
        .iterator()
        .toList

    val deadlineFunctionId = DeadlineAndFunctionId(segmentRefKeyValues)

    val segmentRefs =
      parseSkipList(
        file = file,
        minKey = null,
        maxKey = null,
        fileBlockRef = fileBlockRef
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
        .iterator()
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

    PersistentSegmentMany(
      file = file,
      segmentSize = file.fileSize.toInt,
      createdInLevel = footer.createdInLevel,
      minKey = segmentRefKeyValues.head.key.unslice(),
      maxKey = maxKey,
      minMaxFunctionId = deadlineFunctionId.minMaxFunctionId.map(_.unslice()),
      nearestExpiryDeadline = deadlineFunctionId.nearestDeadline,
      //above parsed segmentRefs cannot be used here because
      //it's MaxKey.Range's minKey is set to the Segment's minKey
      //instead of the Segment's last range key-values minKey.
      initial = None
    )
  }

  private def parseSkipList(file: DBFile,
                            minKey: Slice[Byte],
                            maxKey: MaxKey[Slice[Byte]],
                            fileBlockRef: BlockRefReader[SegmentBlock.Offset])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                               keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                               blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                               segmentIO: SegmentIO): SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef] = {
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

    val skipList = SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef](Slice.Null, SegmentRef.Null)


    //this will also clear all the SegmentRef's
    //            blockCacheMemorySweeper foreach {
    //              cacheMemorySweeper =>
    //                cacheMemorySweeper.add(listSegmentSize, self)
    //            }

    val tailSegmentBytesFromOffset = blockedReader.getPosition
    val tailManySegmentsSize = fileBlockRef.size.toInt - tailSegmentBytesFromOffset

    var previousPath: Path = null
    var previousSegmentRef: SegmentRef = null

    segmentRef.iterator() foreach {
      keyValue =>
        val thisSegmentBlockRef =
          BlockRefReader[SegmentBlock.Offset](
            ref = fileBlockRef.copy(),
            start = tailSegmentBytesFromOffset,
            size = tailManySegmentsSize
          )

        val nextSegmentRef =
          keyValue match {
            case range: Persistent.Range =>
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

            case put: Persistent.Put =>
              TransientSegmentSerialiser.toSegmentRef(
                path = file.path,
                reader = thisSegmentBlockRef,
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

  private def parseListSegment(file: DBFile,
                               minKey: Slice[Byte],
                               maxKey: MaxKey[Slice[Byte]],
                               fileBlockRef: BlockRefReader[SegmentBlock.Offset])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                  blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                                  segmentIO: SegmentIO): SegmentRef = {
    val blockedReader: BlockRefReader[SegmentBlock.Offset] = fileBlockRef.copy()
    val listSegmentSize = blockedReader.readUnsignedInt()
    val listSegment = blockedReader.read(listSegmentSize)
    val listSegmentRef = BlockRefReader[SegmentBlock.Offset](listSegment)

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
  }

}

protected case class PersistentSegmentMany(file: DBFile,
                                           createdInLevel: Int,
                                           minKey: Slice[Byte],
                                           maxKey: MaxKey[Slice[Byte]],
                                           minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                           segmentSize: Int,
                                           nearestPutDeadline: Option[Deadline],
                                           private[segment] val segmentsCache: Cache[Error.Segment, Unit, SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                                                                          functionStore: FunctionStore,
                                                                                                                                                                                          blockCache: Option[BlockCache.State],
                                                                                                                                                                                          fileSweeper: FileSweeper,
                                                                                                                                                                                          bufferCleaner: ByteBufferSweeperActor,
                                                                                                                                                                                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                                                                                                          segmentIO: SegmentIO,
                                                                                                                                                                                          forceSaveApplier: ForceSaveApplier) extends PersistentSegment with LazyLogging {

  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val segmentSearcher: SegmentSearcher = SegmentSearcher

  override def formatId: Byte = PersistentSegmentMany.formatId

  private def segments: SkipListTreeMap[SliceOption[Byte], SegmentRefOption, Slice[Byte], SegmentRef] =
    segmentsCache
      .value(())
      .get

  private val segmentRefsCache: CacheNoIO[Unit, Iterable[SegmentRef]] =
    Cache.noIO[Unit, Iterable[SegmentRef]](synchronised = true, stored = true, None) {
      (_, _) =>
        val uniqueRefs = ListBuffer.empty[SegmentRef]

        var previousPath: Path = null

        segments
          .values()
          .foreach {
            nextSegmentRef =>
              if (nextSegmentRef.path != previousPath) {
                uniqueRefs += nextSegmentRef
                previousPath = nextSegmentRef.path
              }
          }

        uniqueRefs
    }

  def segmentRefs: Iterable[SegmentRef] =
    segmentRefsCache.value(())

  def path = file.path

  override def close: Unit = {
    file.close()
    segmentsCache.clear()
    segmentRefsCache.clear()
  }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  def delete(delay: FiniteDuration) =
    fileSweeper send FileSweeper.Command.Delete(this, delay.fromNow)

  def delete: Unit = {
    logger.trace(s"{}: DELETING FILE", path)
    IO(file.delete()) onLeftSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure.value.exception)
    } map {
      _ =>
        segmentsCache.clear()
    }
  }

  def copyTo(toPath: Path): Path =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegmentOne]]'s parent directory.
   */
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
                                                                                                           executionContext: ExecutionContext): SegmentPutResult[Slice[PersistentSegment]] = {
    val transient: Iterable[TransientSegment] =
      SegmentRef.mergeWrite(
        oldKeyValuesCount = getKeyValueCount(),
        oldKeyValues = iterator(),
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
      mmap = segmentConfig.mmap,
      createdInLevel = createdInLevel,
      transient = transient
    )
  }

  def getFromCache(key: Slice[Byte]): PersistentOption =
    segments
      .floor(key)
      .flatMapSomeS(Persistent.Null: PersistentOption)(_.getFromCache(key))

  def mightContainKey(key: Slice[Byte]): Boolean =
    segments
      .floor(key)
      .existsS(_.mightContain(key))

  /**
   * [[PersistentSegmentMany]] is not aware of [[minMaxFunctionId]].
   * It should be deferred to [[SegmentRef]].
   */
  override def mightContainFunction(key: Slice[Byte]): Boolean =
    segmentRefs exists (_.mightContain(key))

  def get(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    segments
      .floor(key)
      .flatMapSomeS(Persistent.Null: PersistentOption) {
        implicit ref =>
          SegmentRef.get(
            key = key,
            threadState = threadState
          )
      }

  def lower(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    segments
      .lower(key)
      .flatMapSomeS(Persistent.Null: PersistentOption) {
        implicit ref =>
          SegmentRef.lower(
            key = key,
            threadState = threadState
          )
      }

  private def higherFromHigherSegment(key: Slice[Byte],
                                      floorSegment: SegmentRefOption,
                                      threadState: ThreadReadState): PersistentOption =
    segments
      .higher(key)
      .flatMapSomeS(Persistent.Null: PersistentOption) {
        implicit higherSegment =>
          if (floorSegment.existsS(_.path == higherSegment.path))
            Persistent.Null
          else
            SegmentRef.higher(
              key = key,
              threadState = threadState
            )
      }

  def higher(key: Slice[Byte], threadState: ThreadReadState): PersistentOption = {
    val floorSegment = segments.floor(key)

    floorSegment
      .flatMapSomeS(Persistent.Null: PersistentOption) {
        implicit ref =>
          SegmentRef.higher(
            key = key,
            threadState = threadState
          )
      }
      .orElseS {
        higherFromHigherSegment(
          key = key,
          floorSegment = floorSegment,
          threadState = threadState
        )
      }
  }

  override def iterator(): Iterator[Persistent] =
    segmentRefs
      .iterator
      .flatMap(_.iterator())

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
    segmentsCache.clear()
    segmentRefsCache.clear()
    segmentRefs.foreach(_.clearBlockCache())
  }

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    segments
      .floor(key)
      .forallS(_.isInKeyValueCache(key))

  def isKeyValueCacheEmpty: Boolean =
    segmentRefs.forall(_.isKeyValueCacheEmpty)

  def areAllCachesEmpty: Boolean =
    segmentRefs.forall(_.areAllCachesEmpty)

  def cachedKeyValueSize: Int =
    segmentRefs.foldLeft(0)(_ + _.cacheSize)
}
