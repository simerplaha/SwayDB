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

package swaydb.core.segment.format.a.block.segment.list

import java.nio.file.Path
import java.util.concurrent.TimeUnit

import swaydb.core.actor.MemorySweeper
import swaydb.core.function.FunctionStore
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.segment.format.a.block.segment.{SegmentBlock, TransientSegment}
import swaydb.core.segment.{SegmentIO, SegmentRef, SegmentRefOptional}
import swaydb.core.util.Options._
import swaydb.core.util.{Bytes, FiniteDurations, MinMax, SkipList}
import swaydb.data.MaxKey
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.data.util.ByteSizeOf

import scala.concurrent.duration.Deadline

object SegmentListBlock {
  val blockName = this.getClass.getSimpleName.dropRight(1)

  val formatId: Byte = 0.toByte

  object Config {
    def default =
      Config(
        blockIO = IOStrategy.synchronisedStoredIfCompressed
      )
  }

  case class Config(blockIO: IOAction => IOStrategy)

  case class Offset(start: Int, size: Int) extends BlockOffset

  class State(val bytes: Slice[Byte],
              val createdInLevel: Int,
              val numberOfRanges: Int,
              val minKey: Slice[Byte],
              val maxKey: MaxKey[Slice[Byte]],
              val minMaxFunctionId: Option[MinMax[Slice[Byte]]],
              val nearestDeadline: Option[Deadline])

  def writeAndClose(createdInLevel: Int,
                    segments: Iterable[TransientSegment]): State = {
    var minKey: Slice[Byte] = null
    var maxKey: MaxKey[Slice[Byte]] = null
    var segmentSize: Int = 0
    var nearestDeadline: Option[Deadline] = None
    var minMaxFunctionId: Option[MinMax[Slice[Byte]]] = None
    var numberOfRanges: Int = 0

    var totalBytesRequired = 0

    totalBytesRequired +=
      ByteSizeOf.byte + //formatId
        ByteSizeOf.int + //createdInLevel
        ByteSizeOf.int + //number of ranges
        ByteSizeOf.int + //segments count
        ByteSizeOf.long //deadline

    var maxMinMaxFunctionId = 0

    segments foreach {
      segment =>
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            totalBytesRequired += ByteSizeOf.byte + Bytes.sizeOfUnsignedInt(maxKey.size) + maxKey.size

          case MaxKey.Range(fromKey, maxToKey) =>
            val mergedSize = fromKey.size + maxToKey.size
            totalBytesRequired += ByteSizeOf.byte + Bytes.sizeOfUnsignedInt(mergedSize) + mergedSize
        }

        //maximum sized functionId
        segment.minMaxFunctionId match {
          case Some(minMax) =>
            val functionIdSize =
              Bytes.sizeOfUnsignedInt(minMax.min.size) +
                minMax.min.size +
                Bytes.sizeOfUnsignedInt(minMax.max.valueOrElse(_.size, 0)) +
                minMax.max.valueOrElse(_.size, 0)

            maxMinMaxFunctionId = maxMinMaxFunctionId max functionIdSize

          case None =>
            maxMinMaxFunctionId = maxMinMaxFunctionId max 1
        }
    }

    totalBytesRequired += maxMinMaxFunctionId

    val bytes = Slice.create[Byte](totalBytesRequired)
    bytes moveWritePosition ByteSizeOf.int

    bytes add this.formatId
    bytes addUnsignedInt createdInLevel
    bytes addUnsignedInt segments.size

    segments foreach {
      segment =>
        if (minKey == null) minKey = segment.minKey
        maxKey = segment.maxKey
        segmentSize += segment.segmentSize
        nearestDeadline = FiniteDurations.getNearestDeadline(nearestDeadline, segment.nearestDeadline)
        minMaxFunctionId = MinMax.minMax(segment.minMaxFunctionId, minMaxFunctionId)(FunctionStore.order)

        numberOfRanges += segment.sortedIndexClosedState.rangeCount

        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            bytes add Bytes.zero
            bytes addUnsignedInt maxKey.size
            bytes addAll maxKey

          case MaxKey.Range(fromKey, maxToKey) =>
            val rangeKey = Bytes.compressJoin(fromKey, maxToKey)

            bytes add Bytes.one
            bytes addUnsignedInt rangeKey.size
            bytes addAll rangeKey
        }

        bytes addUnsignedInt segment.segmentSize
    }

    bytes addUnsignedLong nearestDeadline.valueOrElse(_.time.toNanos, 0L)

    minMaxFunctionId match {
      case Some(minMaxFunctionId) =>
        bytes addUnsignedInt minMaxFunctionId.min.size
        bytes addAll minMaxFunctionId.min
        minMaxFunctionId.max match {
          case Some(max) =>
            bytes addUnsignedInt max.size
            bytes addAll max

          case None =>
            bytes addUnsignedInt 0
        }

      case None =>
        bytes addUnsignedInt 0
    }

    bytes addUnsignedInt numberOfRanges

    bytes moveWritePosition 0
    bytes addInt bytes.size

    new State(
      bytes = bytes,
      createdInLevel = createdInLevel,
      numberOfRanges = numberOfRanges,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      nearestDeadline = nearestDeadline
    )
  }

  //all these functions are wrapper with a try catch block with value only to make it easier to read.
  def read(path: Path,
           rootRef: BlockRefReader[BlockOffset])(implicit segmentIO: SegmentIO,
                                                 keyOrder: KeyOrder[Slice[Byte]],
                                                 blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                 keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentListBlock = {
    val blockSize = rootRef.readUnsignedInt()
    val bytes = rootRef read blockSize
    val reader = Reader(bytes)

    val formatId = reader.get()
    if (formatId != this.formatId)
      throw new Exception(s"Invalid PersistentSegment formatId: $formatId")
    val createdInLevel = reader.readUnsignedInt()
    val segmentsCount = reader.readUnsignedInt()

    val skipList = SkipList.immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef](Slice.Null, SegmentRef.Null)
    var currentSegmentOffset = 0

    var i = 0
    while (i < segmentsCount) {
      val minKeyLength = reader.readUnsignedInt()
      val minKey = reader.read(minKeyLength).unslice()
      val maxKeyId = reader.readUnsignedInt()
      val maxKeyLength = reader.readUnsignedInt()
      val maxKeyBytes = reader.read(maxKeyLength).unslice()

      val maxKey =
        if (maxKeyId == 1) {
          MaxKey.Fixed(maxKeyBytes)
        } else {
          val (fromKey, toKey) = Bytes.decompressJoin(maxKeyBytes)
          MaxKey.Range(fromKey, toKey)
        }

      val segmentSize = reader.readUnsignedInt()
      val segmentRef = BlockRefReader[SegmentBlock.Offset](rootRef, currentSegmentOffset)
      currentSegmentOffset += segmentSize

      val ref =
        SegmentRef(
          path = path,
          minKey = minKey,
          maxKey = maxKey,
          blockRef = segmentRef,
          segmentIO = segmentIO,
          valuesReaderCacheable = None,
          sortedIndexReaderCacheable = None,
          hashIndexReaderCacheable = None,
          binarySearchIndexReaderCacheable = None,
          bloomFilterReaderCacheable = None,
          footerCacheable = None
        )

      skipList.put(minKey, ref)
      i += 1
    }

    val nearestExpiryDeadline = {
      val deadlineNanos = reader.readUnsignedLong()
      if (deadlineNanos == 0)
        None
      else
        Some(Deadline((deadlineNanos, TimeUnit.NANOSECONDS)))
    }

    val minMaxFunctionId = {
      val minIdSize = reader.readUnsignedInt()
      if (minIdSize == 0)
        None
      else {
        val minId = reader.read(minIdSize)
        val maxIdSize = reader.readUnsignedInt()
        val maxId = if (maxIdSize == 0) None else Some(reader.read(maxIdSize))
        Some(MinMax(minId, maxId))
      }
    }

    val numberOfRanges = reader.readUnsignedInt()

    new SegmentListBlock(
      segmentsCount = segmentsCount,
      createdInLevel = createdInLevel,
      numberOfRanges = numberOfRanges,
      minMaxFunctionId = minMaxFunctionId,
      nearestDeadline = nearestExpiryDeadline,
      skipList = skipList
    )
  }
}

class SegmentListBlock(val segmentsCount: Int,
                       val createdInLevel: Int,
                       val numberOfRanges: Int,
                       val minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                       val nearestDeadline: Option[Deadline],
                       val skipList: SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef]) {
  def hasRange: Boolean =
    numberOfRanges > 0
}
