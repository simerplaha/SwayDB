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

package swaydb.core.segment.format.a.entry.writer

import swaydb.IO
import swaydb.core.data.Memory
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.util.{Bytes, Options}
import swaydb.data.slice.Slice

private[writer] object ValueWriter {

  def write[T](current: Memory,
               entryId: BaseEntryId.Time,
               builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit =
    if (current.value.forall(_.isEmpty))
      noValue(
        current = current,
        entryId = entryId,
        builder = builder
      )
    else
      builder.previous match {
        case Some(previous) =>
          compress(
            current = current,
            previous = previous,
            entryId = entryId,
            builder = builder
          )

        case None =>
          uncompressed(
            current = current,
            currentValue = current.value,
            entryId = entryId,
            builder = builder
          )
      }

  private def compress[T](current: Memory,
                          previous: Memory,
                          entryId: BaseEntryId.Time,
                          builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit =
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

  private def partialCompress[T](current: Memory,
                                 previous: Memory,
                                 entryId: BaseEntryId.Time,
                                 builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit =
    if (builder.enablePrefixCompression && !builder.prefixCompressKeysOnly)
      (Memory.compressibleValue(current), Memory.compressibleValue(previous)) match {
        case (Some(currentValue), Some(previousValue)) =>
          partialCompress(
            current = current,
            entryId = entryId,
            currentValue = currentValue,
            builder = builder,
            previousValue = previousValue
          )

        case (Some(_), None) =>
          uncompressed(
            current = current,
            currentValue = current.value,
            entryId = entryId,
            builder = builder
          )

        case (None, _) =>
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

  private def uncompressed(current: Memory,
                           currentValue: Option[Slice[Byte]],
                           entryId: BaseEntryId.Time,
                           builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[_]): Unit = {
    //if previous does not exists write full offsets and then write deadline.
    val currentValueSize = currentValue.foldLeft(0)(_ + _.size)
    val currentValueOffset = builder.nextStartValueOffset

    builder.startValueOffset = currentValueOffset
    builder.endValueOffset = currentValueOffset + currentValueSize - 1

    DeadlineWriter.write(
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
  private def noValue(current: Memory,
                      entryId: BaseEntryId.Time,
                      builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[_]): Unit =
    DeadlineWriter.write(
      current = current,
      deadlineId = entryId.noValue,
      builder = builder
    )

  private def duplicateValue(current: Memory,
                             previous: Memory,
                             entryId: BaseEntryId.Time,
                             builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[_]): Option[Unit] =
    Options.when(Memory.hasSameValue(previous, current)) { //no need to serialised values and then compare. Simply compare the value objects and check for equality.
      if (builder.enablePrefixCompression && !builder.prefixCompressKeysOnly) {
        //values are the same, no need to write offset & length, jump straight to deadline.
        builder.setSegmentHasPrefixCompression()
        builder.isValueFullyCompressed = true

        DeadlineWriter.write(
          current = current,
          builder = builder,
          deadlineId = entryId.valueFullyCompressed.valueOffsetFullyCompressed.valueLengthFullyCompressed
        )
      } else {
        //use previous values offset but write the offset and length information.

        //this need to reset to true after value is written to values block.
        builder.isValueFullyCompressed = true

        DeadlineWriter.write(
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

  private def partialCompress(current: Memory,
                              entryId: BaseEntryId.Time,
                              currentValue: Slice[Byte],
                              builder: EntryWriter.Builder,
                              previousValue: Slice[Byte])(implicit binder: MemoryToKeyValueIdBinder[_]): Unit =
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

  private def compressValueOffset(current: Memory,
                                  builder: EntryWriter.Builder,
                                  entryId: BaseEntryId.Time,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte])(implicit binder: MemoryToKeyValueIdBinder[_]): Option[Unit] = {
    val currentValueOffset = builder.nextStartValueOffset
    Bytes.compress(Slice.writeInt(builder.startValueOffset), Slice.writeInt(currentValueOffset), 1) map {
      case (valueOffsetCommonBytes, valueOffsetRemainingBytes) =>
        val valueOffsetId =
          if (valueOffsetCommonBytes == 1)
            entryId.valueUncompressed.valueOffsetOneCompressed
          else if (valueOffsetCommonBytes == 2)
            entryId.valueUncompressed.valueOffsetTwoCompressed
          else if (valueOffsetCommonBytes == 3)
            entryId.valueUncompressed.valueOffsetThreeCompressed
          else
            throw IO.throwable(s"Fatal exception: valueOffsetCommonBytes = $valueOffsetCommonBytes")

        Bytes.compress(Slice.writeInt(previousValue.size), Slice.writeInt(currentValue.size), 1) match {
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
                throw IO.throwable(s"Fatal exception: valueLengthCommonBytes = $valueLengthCommonBytes")

            builder.setSegmentHasPrefixCompression()
            builder.startValueOffset = currentValueOffset
            builder.endValueOffset = currentValueOffset + currentValue.size - 1

            DeadlineWriter.write(
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
            val currentUnsignedValueLengthBytes = Slice.writeUnsignedInt(currentValue.size)

            builder.setSegmentHasPrefixCompression()
            builder.startValueOffset = currentValueOffset
            builder.endValueOffset = currentValueOffset + currentValue.size - 1

            DeadlineWriter.write(
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
  private def compressValueLength(current: Memory,
                                  entryId: BaseEntryId.Time,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte],
                                  builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[_]): Unit =
    Bytes.compress(previous = Slice.writeInt(previousValue.size), next = Slice.writeInt(currentValue.size), minimumCommonBytes = 1) match {
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
            throw IO.throwable(s"Fatal exception: valueLengthCommonBytes = $valueLengthCommonBytes")

        val currentValueOffset = builder.nextStartValueOffset

        builder.setSegmentHasPrefixCompression()
        builder.startValueOffset = currentValueOffset
        builder.endValueOffset = currentValueOffset + currentValue.size - 1

        DeadlineWriter.write(
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

        DeadlineWriter.write(
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
