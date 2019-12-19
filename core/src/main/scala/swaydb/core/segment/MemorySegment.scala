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
import swaydb.core.actor.FileSweeper
import swaydb.core.data.{Memory, _}
import swaydb.core.function.FunctionStore
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.merge.{KeyValueMergeBuilder, SegmentMerger}
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[segment] case class MemorySegment(path: Path,
                                          segmentId: Long,
                                          minKey: Slice[Byte],
                                          maxKey: MaxKey[Slice[Byte]],
                                          minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                          segmentSize: Int,
                                          hasRange: Boolean,
                                          hasPut: Boolean,
                                          createdInLevel: Int,
                                          private[segment] val skipList: SkipList.Immutable[Slice[Byte], Memory],
                                          bloomFilterReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                                          nearestExpiryDeadline: Option[Deadline])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                   functionStore: FunctionStore,
                                                                                   fileSweeper: FileSweeper.Enabled) extends Segment with LazyLogging {

  @volatile private var deleted = false

  import keyOrder._

  override def put(newKeyValues: Slice[KeyValue],
                   minSegmentSize: Int,
                   removeDeletes: Boolean,
                   createdInLevel: Int,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config,
                   targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator): Slice[Segment] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val currentKeyValues = getAll()

      val builder = KeyValueMergeBuilder.memory()

      SegmentMerger.merge(
        newKeyValues = newKeyValues,
        oldKeyValues = currentKeyValues,
        builder = builder,
        isLastLevel = removeDeletes
      )

      //      splits.mapRecover[Segment](
      //        block =
      //          keyValues => {
      //            val segmentId = idGenerator.nextID
      //            Segment.memory(
      //              path = targetPaths.next.resolve(IDGenerator.segmentId(segmentId)),
      //              segmentId = segmentId,
      //              createdInLevel = createdInLevel,
      //              keyValues = keyValues
      //            )
      //          },
      //
      //        recover =
      //          (segments: Slice[Segment], _: Throwable) =>
      //            segments foreach {
      //              segmentToDelete =>
      //                IO(segmentToDelete.delete) onLeftSideEffect {
      //                  exception =>
      //                    logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
      //                }
      //            }
      //      )
      ???
    }

  override def refresh(minSegmentSize: Int,
                       removeDeletes: Boolean,
                       createdInLevel: Int,
                       valuesConfig: ValuesBlock.Config,
                       sortedIndexConfig: SortedIndexBlock.Config,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                       hashIndexConfig: HashIndexBlock.Config,
                       bloomFilterConfig: BloomFilterBlock.Config,
                       segmentConfig: SegmentBlock.Config,
                       targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator): Slice[Segment] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val currentKeyValues = getAll()

      //      val splits =
      //        SegmentMerger.split(
      //          keyValues = currentKeyValues,
      //          minSegmentSize = minSegmentSize,
      //          isLastLevel = removeDeletes,
      //          forInMemory = true,
      //          createdInLevel = createdInLevel,
      //          valuesConfig = valuesConfig,
      //          sortedIndexConfig = sortedIndexConfig,
      //          binarySearchIndexConfig = binarySearchIndexConfig,
      //          hashIndexConfig = hashIndexConfig,
      //          bloomFilterConfig = bloomFilterConfig
      //        )
      //
      //      splits.mapRecover[Segment](
      //        block =
      //          keyValues => {
      //            val segmentId = idGenerator.nextID
      //            Segment.memory(
      //              path = targetPaths.next.resolve(IDGenerator.segmentId(segmentId)),
      //              segmentId = segmentId,
      //              createdInLevel = createdInLevel,
      //              keyValues = keyValues
      //            )
      //          },
      //
      //        recover =
      //          (segments: Slice[Segment], _: Throwable) =>
      //            segments foreach {
      //              segmentToDelete =>
      //                IO(segmentToDelete.delete) onLeftSideEffect {
      //                  exception =>
      //                    logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
      //                }
      //            }
      //      )
      ???
    }

  override def getFromCache(key: Slice[Byte]): Option[KeyValue] =
    skipList.get(key)

  override def get(key: Slice[Byte], readState: ReadState): Option[Memory] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val mightContain = mightContainKey(key, rangeCheck = true)
      if (mightContain)
        maxKey match {
          case MaxKey.Fixed(maxKey) if key > maxKey =>
            None

          case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
            None

          case _ =>
            if (hasRange)
              skipList.floor(key) match {
                case Some(range: Memory.Range) if range contains key =>
                  Some(range)

                case _ =>
                  skipList.get(key)
              }
            else
              skipList.get(key)
        }
      else
        None
    }

  def mightContainKey(key: Slice[Byte], rangeCheck: Boolean): Boolean =
    rangeCheck && hasRange ||
      bloomFilterReader.forall {
        reader =>
          BloomFilterBlock.mightContain(
            key = key,
            reader = reader.copy()
          )
      }

  def mightContainKey(key: Slice[Byte]): Boolean =
    mightContainKey(key = key, rangeCheck = false)

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId.exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  override def lower(key: Slice[Byte],
                     readState: ReadState): Option[Memory] =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      skipList.lower(key)

  def floorHigherHint(key: Slice[Byte]): Option[Slice[Byte]] =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else if (hasPut)
      if (key < minKey)
        Some(minKey)
      else if (key < maxKey.maxKey)
        Some(key)
      else
        None
    else
      None

  override def higher(key: Slice[Byte],
                      readState: ReadState): Option[Memory] =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else if (hasRange)
      skipList.floor(key) match {
        case Some(floor) =>
          floor match {
            case floorRange: Memory.Range if floorRange contains key =>
              Some(floorRange)

            case _ =>
              skipList.higher(key)
          }

        case None =>
          skipList.higher(key)
      }
    else
      skipList.higher(key)

  override def getAll(addTo: Option[Slice[KeyValue]] = None): Slice[KeyValue] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val slice = addTo getOrElse Slice.create[KeyValue](skipList.size)
      skipList.values() forEach {
        new Consumer[Memory] {
          override def accept(value: Memory): Unit =
            slice add value
        }
      }
      slice
    }

  override def delete: Unit = {
    //cache should not be cleared.
    logger.trace(s"{}: DELETING FILE", path)
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      deleted = true
  }

  override val close: Unit =
    ()

  override def getKeyValueCount(): Int =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      skipList.size

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

  override def isFooterDefined: Boolean =
    !deleted

  override def deleteSegmentsEventually: Unit =
    fileSweeper.delete(this)

  override def hasBloomFilter: Boolean =
    bloomFilterReader.isDefined

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
