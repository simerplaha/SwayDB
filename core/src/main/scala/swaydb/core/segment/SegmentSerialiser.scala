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

import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{Effect, ForceSaveApplier}
import swaydb.core.map.serializer.ValueSerializer.MinMaxSerialiser
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.util.{Bytes, Extension}
import swaydb.data.MaxKey
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.ByteSizeOf
import swaydb.data.util.Options._

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Deadline

private[core] sealed trait SegmentSerialiser {

  def write(value: Segment,
            bytes: Slice[Byte]): Unit

  def read(reader: ReaderBase[Byte],
           mmapSegment: MMAP.Segment,
           segmentRefCacheWeight: Int,
           checkExists: Boolean,
           removeDeletes: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                   timeOrder: TimeOrder[Slice[Byte]],
                                   functionStore: FunctionStore,
                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                   fileSweeper: FileSweeper,
                                   bufferCleaner: ByteBufferSweeperActor,
                                   blockCacheSweeper: Option[MemorySweeper.Block],
                                   forceSaveApplier: ForceSaveApplier,
                                   segmentIO: SegmentReadIO): Segment

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
        .addUnsignedInt(segment.updateCount)
        .addUnsignedInt(segment.rangeCount)
        .addUnsignedInt(segment.putCount)
        .addUnsignedInt(segment.putDeadlineCount)
        .addUnsignedInt(segment.keyValueCount)
        .addUnsignedInt(segmentPath.size)
        .addAll(segmentPath)
        .addUnsignedInt(segment.createdInLevel)
        .addUnsignedInt(segment.segmentSize)
        .addUnsignedInt(segment.minKey.size)
        .addAll(segment.minKey)
        .addUnsignedInt(maxKeyId)
        .addUnsignedInt(maxKeyBytes.size)
        .addAll(maxKeyBytes)
        .addUnsignedLong(segment.nearestPutDeadline.valueOrElse(_.time.toNanos, 0L))

      MinMaxSerialiser.write(segment.minMaxFunctionId, bytes)
    }

    def read(reader: ReaderBase[Byte],
             mmapSegment: MMAP.Segment,
             segmentRefCacheWeight: Int,
             checkExists: Boolean,
             removeDeletes: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                     timeOrder: TimeOrder[Slice[Byte]],
                                     functionStore: FunctionStore,
                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                     fileSweeper: FileSweeper,
                                     bufferCleaner: ByteBufferSweeperActor,
                                     blockCacheSweeper: Option[MemorySweeper.Block],
                                     forceSaveApplier: ForceSaveApplier,
                                     segmentIO: SegmentReadIO): Segment = {

      val formatId = reader.get() //formatId

      if (formatId != this.formatId)
        throw new Exception(s"Invalid serialised Segment formatId: $formatId")

      val segmentFormatId = reader.get()

      val updateCount = reader.readUnsignedInt()
      val rangeCount = reader.readUnsignedInt()
      val putCount = reader.readUnsignedInt()
      val putDeadlineCount = reader.readUnsignedInt()
      val keyValueCount = reader.readUnsignedInt()

      val segmentPathLength = reader.readUnsignedInt()
      val segmentPathBytes = reader.read(segmentPathLength).unslice()
      val segmentPath = Paths.get(new String(segmentPathBytes.toArray[Byte], StandardCharsets.UTF_8))
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

      val minMaxFunctionId = MinMaxSerialiser.read(reader)

      val fileType = Effect.numberFileId(segmentPath)._2

      if (fileType != Extension.Seg)
        throw new Exception(s"File is not a Segment. Path: $segmentPath")

      Segment(
        path = segmentPath,
        formatId = segmentFormatId,
        createdInLevel = createdInLevel,
        segmentRefCacheWeight = segmentRefCacheWeight,
        mmap = mmapSegment,
        minKey = minKey,
        maxKey = maxKey,
        segmentSize = segmentSize,
        minMaxFunctionId = minMaxFunctionId,
        updateCount = updateCount,
        rangeCount = rangeCount,
        putCount = putCount,
        putDeadlineCount = putDeadlineCount,
        keyValueCount = keyValueCount,
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
        MinMaxSerialiser.bytesRequired(segment.minMaxFunctionId)

      ByteSizeOf.byte + //formatId
        ByteSizeOf.byte + //segmentFormatId
        ByteSizeOf.varInt + //updateCount
        ByteSizeOf.varInt + //rangeCount
        ByteSizeOf.varInt + //putCount
        ByteSizeOf.varInt + //putDeadlineCount
        ByteSizeOf.varInt + //keyValueCount
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
