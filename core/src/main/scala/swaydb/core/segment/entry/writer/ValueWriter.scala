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

package swaydb.core.segment.entry.writer

import swaydb.core.data.Memory
import swaydb.core.segment.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.slice.{Slice, SliceOption}
import swaydb.utils.Options

private[segment] trait ValueWriter {
  def write[T <: Memory](current: T,
                         entryId: BaseEntryId.Time,
                         builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                       keyWriter: KeyWriter,
                                                       deadlineWriter: DeadlineWriter): Unit
}

private[segment] object ValueWriter extends ValueWriter {

  def write[T <: Memory](current: T,
                         entryId: BaseEntryId.Time,
                         builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                       keyWriter: KeyWriter,
                                                       deadlineWriter: DeadlineWriter): Unit =
    if (current.value.forallC(_.isEmpty))
      noValue(
        current = current,
        entryId = entryId,
        builder = builder
      )
    else
      builder.previous match {
        case previous: Memory =>
          compress(
            current = current,
            previous = previous,
            entryId = entryId,
            builder = builder
          )

        case Memory.Null =>
          uncompressed(
            current = current,
            currentValue = current.value,
            entryId = entryId,
            builder = builder
          )
      }

  private def compress[T <: Memory](current: T,
                                    previous: Memory,
                                    entryId: BaseEntryId.Time,
                                    builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                  keyWriter: KeyWriter,
                                                                  deadlineWriter: DeadlineWriter): Unit =
    if (builder.compressDuplicateValues) //check if values are the same.
      duplicateValue(
        current = current,
        previous = previous,
        entryId = entryId,
        builder = builder
      ) getOrElse {
        partialCompress(
          current = current,
          previous = previous,
          entryId = entryId,
          builder = builder
        )
      }
    else
      partialCompress(
        current = current,
        previous = previous,
        builder = builder,
        entryId = entryId
      )

  private def partialCompress[T <: Memory](current: T,
                                           previous: Memory,
                                           entryId: BaseEntryId.Time,
                                           builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                         keyWriter: KeyWriter,
                                                                         deadlineWriter: DeadlineWriter): Unit =
    if (builder.enablePrefixCompressionForCurrentWrite && !builder.prefixCompressKeysOnly)
      (Memory.compressibleValue(current), Memory.compressibleValue(previous)) match {
        case (currentValue: Slice[Byte], previousValue: Slice[Byte]) =>
          partialCompress(
            current = current,
            entryId = entryId,
            currentValue = currentValue,
            builder = builder,
            previousValue = previousValue
          )

        case (_: Slice[Byte], Slice.Null) =>
          uncompressed(
            current = current,
            currentValue = current.value,
            entryId = entryId,
            builder = builder
          )

        case (Slice.Null, _) =>
          noValue(
            current = current,
            entryId = entryId,
            builder = builder
          )
      }
    else
      uncompressed(
        current = current,
        currentValue = current.value,
        entryId = entryId,
        builder = builder
      )

  private def uncompressed[T <: Memory](current: T,
                                        currentValue: SliceOption[Byte],
                                        entryId: BaseEntryId.Time,
                                        builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                      keyWriter: KeyWriter,
                                                                      deadlineWriter: DeadlineWriter): Unit = {
    //if previous does not exists write full offsets and then write deadline.
    val currentValueSize = currentValue.valueOrElseC(_.size, 0)
    val currentValueOffset = builder.nextStartValueOffset

    builder.startValueOffset = currentValueOffset
    builder.endValueOffset = currentValueOffset + currentValueSize - 1

    deadlineWriter.write(
      current = current,
      builder = builder,
      deadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed
    )

    builder
      .bytes
      .addUnsignedInt(currentValueOffset)
      .addUnsignedInt(currentValueSize)
  }

  //if there is no value then write deadline.
  //since there is no value, offsets will continue from previous key-values offset.
  private def noValue[T <: Memory](current: T,
                                   entryId: BaseEntryId.Time,
                                   builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                 keyWriter: KeyWriter,
                                                                 deadlineWriter: DeadlineWriter): Unit =
    deadlineWriter.write(
      current = current,
      deadlineId = entryId.noValue,
      builder = builder
    )

  private def duplicateValue[T <: Memory](current: T,
                                          previous: Memory,
                                          entryId: BaseEntryId.Time,
                                          builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                        keyWriter: KeyWriter,
                                                                        deadlineWriter: DeadlineWriter): Option[Unit] =
    Options.when(Memory.hasSameValue(previous, current)) { //no need to serialised values and then compare. Simply compare the value objects and check for equality.
      if (builder.enablePrefixCompressionForCurrentWrite && !builder.prefixCompressKeysOnly) {
        //values are the same, no need to write offset & length, jump straight to deadline.
        builder.setSegmentHasPrefixCompression()
        builder.isValueFullyCompressed = true

        deadlineWriter.write(
          current = current,
          builder = builder,
          deadlineId = entryId.valueFullyCompressed.valueOffsetFullyCompressed.valueLengthFullyCompressed
        )
      } else {
        //use previous values offset but write the offset and length information.

        //this need to reset to true after value is written to values block.
        builder.isValueFullyCompressed = true

        deadlineWriter.write(
          current = current,
          deadlineId = entryId.valueFullyCompressed.valueOffsetUncompressed.valueLengthUncompressed,
          builder = builder
        )

        builder
          .bytes
          .addUnsignedInt(builder.startValueOffset)
          .addUnsignedInt(builder.endValueOffset - builder.startValueOffset + 1)
      }

      Options.unit
    }

  private def partialCompress[T <: Memory](current: T,
                                           entryId: BaseEntryId.Time,
                                           currentValue: Slice[Byte],
                                           builder: EntryWriter.Builder,
                                           previousValue: Slice[Byte])(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                       keyWriter: KeyWriter,
                                                                       deadlineWriter: DeadlineWriter): Unit =
    compressValueOffset( //if the values are not the same, write compressed offset, length and then deadline.
      current = current,
      builder = builder,
      entryId = entryId,
      currentValue = currentValue,
      previousValue = previousValue
    ) getOrElse {
      compressValueLength(
        current = current,
        entryId = entryId,
        currentValue = currentValue,
        previousValue = previousValue,
        builder = builder
      )
    }

  private def compressValueOffset[T <: Memory](current: T,
                                               builder: EntryWriter.Builder,
                                               entryId: BaseEntryId.Time,
                                               currentValue: Slice[Byte],
                                               previousValue: Slice[Byte])(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                           keyWriter: KeyWriter,
                                                                           deadlineWriter: DeadlineWriter): Option[Unit] = {
    val currentValueOffset = builder.nextStartValueOffset
    Bytes.compress(Slice.writeInt[Byte](builder.startValueOffset), Slice.writeInt[Byte](currentValueOffset), 1) map {
      case (valueOffsetCommonBytes, valueOffsetRemainingBytes) =>
        val valueOffsetId =
          if (valueOffsetCommonBytes == 1)
            entryId.valueUncompressed.valueOffsetOneCompressed
          else if (valueOffsetCommonBytes == 2)
            entryId.valueUncompressed.valueOffsetTwoCompressed
          else if (valueOffsetCommonBytes == 3)
            entryId.valueUncompressed.valueOffsetThreeCompressed
          else
            throw new Exception(s"Fatal exception: valueOffsetCommonBytes = $valueOffsetCommonBytes")

        Bytes.compress(Slice.writeInt[Byte](previousValue.size), Slice.writeInt[Byte](currentValue.size), 1) match {
          case Some((valueLengthCommonBytes, valueLengthRemainingBytes)) =>
            val valueLengthId =
              if (valueLengthCommonBytes == 1)
                valueOffsetId.valueLengthOneCompressed
              else if (valueLengthCommonBytes == 2)
                valueOffsetId.valueLengthTwoCompressed
              else if (valueLengthCommonBytes == 3)
                valueOffsetId.valueLengthThreeCompressed
              else if (valueLengthCommonBytes == 4)
                valueOffsetId.valueLengthFullyCompressed
              else
                throw new Exception(s"Fatal exception: valueLengthCommonBytes = $valueLengthCommonBytes")

            builder.setSegmentHasPrefixCompression()
            builder.startValueOffset = currentValueOffset
            builder.endValueOffset = currentValueOffset + currentValue.size - 1

            deadlineWriter.write(
              current = current,
              deadlineId = valueLengthId,
              builder = builder
            )

            builder
              .bytes
              .addAll(valueOffsetRemainingBytes)
              .addAll(valueLengthRemainingBytes)

          case None =>
            //if unable to compress valueLengthBytes then write compressed valueOffset with fully valueLength bytes.
            val currentUnsignedValueLengthBytes = Slice.writeUnsignedInt[Byte](currentValue.size)

            builder.setSegmentHasPrefixCompression()
            builder.startValueOffset = currentValueOffset
            builder.endValueOffset = currentValueOffset + currentValue.size - 1

            deadlineWriter.write(
              current = current,
              deadlineId = valueOffsetId.valueLengthUncompressed,
              builder = builder
            )

            builder
              .bytes
              .addAll(valueOffsetRemainingBytes)
              .addAll(currentUnsignedValueLengthBytes)
        }
    }
  }

  //if unable to compress valueOffsetBytes, try compressing value length valueLength bytes.
  private def compressValueLength[T <: Memory](current: T,
                                               entryId: BaseEntryId.Time,
                                               currentValue: Slice[Byte],
                                               previousValue: Slice[Byte],
                                               builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                             keyWriter: KeyWriter,
                                                                             deadlineWriter: DeadlineWriter): Unit =
    Bytes.compress(previous = Slice.writeInt[Byte](previousValue.size), next = Slice.writeInt[Byte](currentValue.size), minimumCommonBytes = 1) match {
      case Some((valueLengthCommonBytes, valueLengthRemainingBytes)) =>
        val valueLengthId =
          if (valueLengthCommonBytes == 1)
            entryId.valueUncompressed.valueOffsetUncompressed.valueLengthOneCompressed
          else if (valueLengthCommonBytes == 2)
            entryId.valueUncompressed.valueOffsetUncompressed.valueLengthTwoCompressed
          else if (valueLengthCommonBytes == 3)
            entryId.valueUncompressed.valueOffsetUncompressed.valueLengthThreeCompressed
          else if (valueLengthCommonBytes == 4)
            entryId.valueUncompressed.valueOffsetUncompressed.valueLengthFullyCompressed
          else
            throw new Exception(s"Fatal exception: valueLengthCommonBytes = $valueLengthCommonBytes")

        val currentValueOffset = builder.nextStartValueOffset

        builder.setSegmentHasPrefixCompression()
        builder.startValueOffset = currentValueOffset
        builder.endValueOffset = currentValueOffset + currentValue.size - 1

        deadlineWriter.write(
          current = current,
          deadlineId = valueLengthId,
          builder = builder
        )

        builder
          .bytes
          .addUnsignedInt(currentValueOffset)
          .addAll(valueLengthRemainingBytes)

      case None =>
        //unable to compress valueOffset and valueLength bytes, write them as full bytes.
        val currentValueOffset = builder.nextStartValueOffset

        builder.startValueOffset = currentValueOffset
        builder.endValueOffset = currentValueOffset + currentValue.size - 1

        deadlineWriter.write(
          current = current,
          deadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
          builder = builder
        )

        builder
          .bytes
          .addUnsignedInt(currentValueOffset)
          .addUnsignedInt(currentValue.size)
    }
}
