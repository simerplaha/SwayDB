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

package swaydb.core.segment

import java.nio.file.Path
import java.util.function.Consumer

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.Memory
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.JavaConverters._
import scala.concurrent.duration.Deadline

private[segment] case class MemorySegment(path: Path,
                                          segmentId: Long,
                                          minKey: Slice[Byte],
                                          maxKey: MaxKey[Slice[Byte]],
                                          minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                          segmentSize: Int,
                                          _hasRange: Boolean,
                                          _hasPut: Boolean,
                                          _createdInLevel: Int,
                                          private[segment] val skipList: SkipList.Concurrent[Slice[Byte], Memory],
                                          bloomFilterReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                                          nearestExpiryDeadline: Option[Deadline])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                   functionStore: FunctionStore,
                                                                                   fileSweeper: FileSweeper.Enabled) extends Segment with LazyLogging {

  @volatile private var deleted = false

  import keyOrder._

  override def put(newKeyValues: Slice[KeyValue.ReadOnly],
                   minSegmentSize: Long,
                   removeDeletes: Boolean,
                   createdInLevel: Int,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config,
                   targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator): IO[swaydb.Error.Segment, Slice[Segment]] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      getAll() flatMap {
        currentKeyValues =>
          SegmentMerger.merge(
            newKeyValues = newKeyValues,
            oldKeyValues = currentKeyValues,
            minSegmentSize = minSegmentSize,
            isLastLevel = removeDeletes,
            forInMemory = true,
            createdInLevel = createdInLevel,
            valuesConfig = valuesConfig,
            sortedIndexConfig = sortedIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            hashIndexConfig = hashIndexConfig,
            bloomFilterConfig = bloomFilterConfig
          ) flatMap {
            splits =>
              splits.mapIO[Segment](
                block =
                  keyValues => {
                    val segmentId = idGenerator.nextID
                    Segment.memory(
                      path = targetPaths.next.resolve(IDGenerator.segmentId(segmentId)),
                      segmentId = segmentId,
                      createdInLevel = createdInLevel,
                      keyValues = keyValues
                    )
                  },

                recover =
                  (segments: Slice[Segment], _: IO.Left[swaydb.Error.Segment, Iterable[Segment]]) =>
                    segments foreach {
                      segmentToDelete =>
                        segmentToDelete.delete onLeftSideEffect {
                          exception =>
                            logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
                        }
                    }
              )
          }
      }

  override def refresh(minSegmentSize: Long,
                       removeDeletes: Boolean,
                       createdInLevel: Int,
                       valuesConfig: ValuesBlock.Config,
                       sortedIndexConfig: SortedIndexBlock.Config,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                       hashIndexConfig: HashIndexBlock.Config,
                       bloomFilterConfig: BloomFilterBlock.Config,
                       segmentConfig: SegmentBlock.Config,
                       targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator): IO[swaydb.Error.Segment, Slice[Segment]] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      getAll() flatMap {
        currentKeyValues =>
          SegmentMerger.split(
            keyValues = currentKeyValues,
            minSegmentSize = minSegmentSize,
            isLastLevel = removeDeletes,
            forInMemory = true,
            createdInLevel = createdInLevel,
            valuesConfig = valuesConfig,
            sortedIndexConfig = sortedIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            hashIndexConfig = hashIndexConfig,
            bloomFilterConfig = bloomFilterConfig
          ) flatMap {
            splits =>
              splits.mapIO[Segment](
                block =
                  keyValues => {
                    val segmentId = idGenerator.nextID
                    Segment.memory(
                      path = targetPaths.next.resolve(IDGenerator.segmentId(segmentId)),
                      segmentId = segmentId,
                      createdInLevel = createdInLevel,
                      keyValues = keyValues
                    )
                  },

                recover =
                  (segments: Slice[Segment], _: IO.Left[swaydb.Error.Segment, Iterable[Segment]]) =>
                    segments foreach {
                      segmentToDelete =>
                        segmentToDelete.delete onLeftSideEffect {
                          exception =>
                            logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
                        }
                    }
              )
          }
      }

  override def getFromCache(key: Slice[Byte]): Option[KeyValue.ReadOnly] =
    skipList.get(key)

  override def get(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory]] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      mightContainKey(key, rangeCheck = true) flatMap {
        mightContain =>
          if (mightContain)
            maxKey match {
              case MaxKey.Fixed(maxKey) if key > maxKey =>
                IO.none

              case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
                IO.none

              case _ =>
                if (_hasRange)
                  skipList.floor(key) match {
                    case Some(range: Memory.Range) if range contains key =>
                      IO.Right(Some(range))

                    case _ =>
                      IO.Right(skipList.get(key))
                  }
                else
                  IO.Right(skipList.get(key))
            }
          else
            IO.none
      }

  def mightContainKey(key: Slice[Byte], rangeCheck: Boolean): IO[swaydb.Error.Segment, Boolean] =
    if (rangeCheck && _hasRange)
      IO.`true`
    else
      bloomFilterReader map {
        reader =>
          BloomFilterBlock.mightContain(
            key = key,
            reader = reader.copy()
          )
      } getOrElse IO.`true`

  def mightContainKey(key: Slice[Byte]): IO[swaydb.Error.Segment, Boolean] =
    mightContainKey(key = key, rangeCheck = false)

  override def mightContainFunction(key: Slice[Byte]): IO[swaydb.Error.Segment, Boolean] =
    IO {
      minMaxFunctionId.exists {
        minMaxFunctionId =>
          MinMax.contains(
            key = key,
            minMax = minMaxFunctionId
          )(FunctionStore.order)
      }
    }

  override def lower(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory]] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      IO.Right(skipList.lower(key))

  def floorHigherHint(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Slice[Byte]]] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      hasPut map {
        hasPut =>
          if (hasPut)
            if (key < minKey)
              Some(minKey)
            else if (key < maxKey.maxKey)
              Some(key)
            else
              None
          else
            None
      }

  override def higher(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory]] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else if (_hasRange)
      skipList.floor(key) match {
        case Some(floor) =>
          floor match {
            case floorRange: Memory.Range if floorRange contains key =>
              IO.Right(Some(floorRange))

            case _ =>
              IO.Right(skipList.higher(key))
          }

        case None =>
          IO.Right(skipList.higher(key))
      }
    else
      IO.Right(skipList.higher(key))

  override def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      IO {
        val slice = addTo getOrElse Slice.create[KeyValue.ReadOnly](skipList.size)
        skipList.values() forEach {
          new Consumer[Memory] {
            override def accept(value: Memory): Unit =
              slice add value
          }
        }
        slice
      }

  override def delete: IO[swaydb.Error.Segment, Unit] = {
    //cache should not be cleared.
    logger.trace(s"{}: DELETING FILE", path)
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      IO.Right {
        deleted = true
      }
  }

  override val close: IO[swaydb.Error.Segment, Unit] =
    IO.unit

  override def getKeyValueCount(): IO[swaydb.Error.Segment, Int] =
    if (deleted)
      IO.Left(swaydb.Error.NoSuchFile(path): swaydb.Error.Segment)
    else
      IO.Right(skipList.size)

  override def isOpen: Boolean =
    !deleted

  override def isFileDefined: Boolean =
    !deleted

  override def memory: Boolean =
    true

  override def persistent: Boolean =
    false

  override def existsOnDisk: Boolean =
    false

  override def hasRange: IO[swaydb.Error.Segment, Boolean] =
    IO(_hasRange)

  override def hasPut: IO[swaydb.Error.Segment, Boolean] =
    IO(_hasPut)

  override def isFooterDefined: Boolean =
    !deleted

  override def deleteSegmentsEventually: Unit =
    fileSweeper.delete(this)

  override def createdInLevel: IO[swaydb.Error.Segment, Int] =
    IO(_createdInLevel)

  override def hasBloomFilter: IO[swaydb.Error.Segment, Boolean] =
    IO(bloomFilterReader.isDefined)

  override def clearCachedKeyValues(): Unit =
    ()

  override def clearAllCaches(): Unit =
    ()

  override def isInKeyValueCache(key: Slice[Byte]): Boolean =
    skipList contains key

  override def isKeyValueCacheEmpty: Boolean =
    skipList.isEmpty

  def areAllCachesEmpty: Boolean =
    isKeyValueCacheEmpty

  override def cachedKeyValueSize: Int =
    skipList.size
}
