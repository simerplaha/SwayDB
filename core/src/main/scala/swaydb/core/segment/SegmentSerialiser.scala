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

import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.log.serializer.ValueSerializer.MinMaxSerialiser
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.Bytes
import swaydb.data.MaxKey
import swaydb.data.config.{MMAP, SegmentRefCacheLife}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.effect.{Effect, Extension}
import swaydb.utils.ByteSizeOf
import swaydb.utils.Options.OptionsImplicits

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Deadline

private[core] sealed trait SegmentSerialiser {

  def write(value: Segment,
            bytes: Slice[Byte]): Unit

  def read(reader: ReaderBase[Byte],
           mmapSegment: MMAP.Segment,
           segmentRefCacheLife: SegmentRefCacheLife,
           checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
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
             segmentRefCacheLife: SegmentRefCacheLife,
             checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
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
        segmentRefCacheLife = segmentRefCacheLife,
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
        Bytes.sizeOfUnsignedInt(segment.updateCount) + //updateCount
        Bytes.sizeOfUnsignedInt(segment.rangeCount) + //rangeCount
        Bytes.sizeOfUnsignedInt(segment.putCount) + //putCount
        Bytes.sizeOfUnsignedInt(segment.putDeadlineCount) + //putDeadlineCount
        Bytes.sizeOfUnsignedInt(segment.keyValueCount) + //keyValueCount
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
