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

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.io.file.{BlockCache, Effect, ForceSaveApplier}
import swaydb.data.util.Options._
import swaydb.core.util.{BlockCacheFileIDGenerator, Bytes, Extension, MinMax}
import swaydb.data.MaxKey
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.ByteSizeOf

import scala.concurrent.duration.Deadline

private[core] sealed trait SegmentSerialiser {

  def write(value: Segment, bytes: Slice[Byte]): Unit

  def read(reader: ReaderBase,
           mmapSegment: MMAP.Segment,
           checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 functionStore: FunctionStore,
                                 keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                 fileSweeper: FileSweeperActor,
                                 bufferCleaner: ByteBufferSweeperActor,
                                 blockCache: Option[BlockCache.State],
                                 forceSaveApplier: ForceSaveApplier,
                                 segmentIO: SegmentIO): Segment

  def bytesRequired(value: Segment): Int

}

private[core] object SegmentSerialiser {

  object FormatA extends SegmentSerialiser {
    val formatId: Byte = 0.toByte

    override def write(segment: Segment, bytes: Slice[Byte]): Unit = {
      val segmentPath = Slice(segment.path.toString.getBytes(StandardCharsets.UTF_8))

      val (maxKeyId, maxKeyBytes) =
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            (1, maxKey)

          case MaxKey.Range(fromKey, maxToKey) =>
            (2, Bytes.compressJoin(fromKey, maxToKey))
        }

      bytes
        .add(this.formatId)
        .add(segment.formatId)
        .addUnsignedInt(segmentPath.size)
        .addBytes(segmentPath)
        .addUnsignedInt(segment.createdInLevel)
        .addUnsignedInt(segment.segmentSize)
        .addUnsignedInt(segment.minKey.size)
        .addAll(segment.minKey)
        .addUnsignedInt(maxKeyId)
        .addUnsignedInt(maxKeyBytes.size)
        .addAll(maxKeyBytes)
        .addUnsignedLong(segment.nearestPutDeadline.valueOrElse(_.time.toNanos, 0L))

      segment.minMaxFunctionId match {
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
    }

    def read(reader: ReaderBase,
             mmapSegment: MMAP.Segment,
             checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                   timeOrder: TimeOrder[Slice[Byte]],
                                   functionStore: FunctionStore,
                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                   fileSweeper: FileSweeperActor,
                                   bufferCleaner: ByteBufferSweeperActor,
                                   blockCache: Option[BlockCache.State],
                                   forceSaveApplier: ForceSaveApplier,
                                   segmentIO: SegmentIO): Segment = {

      val formatId = reader.get() //formatId

      if (formatId != this.formatId)
        throw new Exception(s"Invalid serialised Segment formatId: $formatId")

      val segmentFormatId = reader.get()
      val segmentPathLength = reader.readUnsignedInt()
      val segmentPathBytes = reader.read(segmentPathLength).unslice()
      val segmentPath = Paths.get(new String(segmentPathBytes.toArray, StandardCharsets.UTF_8))
      val createdInLevel = reader.readUnsignedInt()
      val segmentSize = reader.readUnsignedInt()
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

      val fileType = Effect.numberFileId(segmentPath)._2

      if (fileType != Extension.Seg)
        throw new Exception(s"File is not a Segment. Path: $segmentPath")

      Segment(
        path = segmentPath,
        formatId = segmentFormatId,
        createdInLevel = createdInLevel,
        blockCacheFileId = BlockCacheFileIDGenerator.nextID,
        mmap = mmapSegment,
        minKey = minKey,
        maxKey = maxKey,
        segmentSize = segmentSize,
        minMaxFunctionId = minMaxFunctionId,
        nearestExpiryDeadline = nearestExpiryDeadline,
        copiedFrom = None,
        checkExists = checkExists
      )
    }

    override def bytesRequired(segment: Segment): Int = {
      val segmentPath = segment.path.toString.getBytes(StandardCharsets.UTF_8)

      val (maxKeyId, maxKeyBytes) =
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            (1, maxKey)
          case MaxKey.Range(fromKey, maxToKey) =>
            (2, Bytes.compressJoin(fromKey, maxToKey))
        }

      val minMaxFunctionIdBytesRequires =
        segment.minMaxFunctionId match {
          case Some(minMax) =>
            Bytes.sizeOfUnsignedInt(minMax.min.size) +
              minMax.min.size +
              Bytes.sizeOfUnsignedInt(minMax.max.valueOrElse(_.size, 0)) +
              minMax.max.valueOrElse(_.size, 0)

          case None =>
            1
        }

      ByteSizeOf.byte + //formatId
        ByteSizeOf.byte + //segmentFormatId
        Bytes.sizeOfUnsignedInt(segmentPath.length) +
        segmentPath.length +
        Bytes.sizeOfUnsignedInt(segment.createdInLevel) +
        Bytes.sizeOfUnsignedInt(segment.segmentSize) +
        Bytes.sizeOfUnsignedInt(segment.minKey.size) +
        segment.minKey.size +
        Bytes.sizeOfUnsignedInt(maxKeyId) +
        Bytes.sizeOfUnsignedInt(maxKeyBytes.size) +
        maxKeyBytes.size +
        Bytes.sizeOfUnsignedLong(segment.nearestPutDeadline.valueOrElse(_.time.toNanos, 0L)) +
        minMaxFunctionIdBytesRequires
    }
  }
}
