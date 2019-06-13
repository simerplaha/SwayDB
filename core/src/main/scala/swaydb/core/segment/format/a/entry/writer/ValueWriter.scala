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

import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.entry.id.{EntryId, TransientToEntryId}
import swaydb.core.util.Bytes
import swaydb.core.util.Bytes._
import swaydb.data.slice.Slice

private[writer] object ValueWriter {

  def write(current: KeyValue.WriteOnly,
            enablePrefixCompression: Boolean,
            compressDuplicateValues: Boolean,
            entryId: EntryId.Time,
            plusSize: Int)(implicit id: TransientToEntryId[_]): EntryWriter.Result =
    current.value map {
      currentValue: Slice[Byte] =>
        current.previous map {
          previous: KeyValue.WriteOnly =>
            compressWithPrevious(
              current = current,
              currentValue = currentValue,
              previous = previous,
              enablePrefixCompression = enablePrefixCompression,
              compressDuplicateValues = compressDuplicateValues,
              entryId = entryId,
              plusSize = plusSize
            )
        } getOrElse {
          noPreviousValue(
            current = current,
            entryId = entryId,
            plusSize = plusSize,
            currentValue = currentValue,
            enablePrefixCompression = enablePrefixCompression
          )
        }
    } getOrElse {
      noValue(
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression
      )
    }

  private def compressWithPrevious(current: KeyValue.WriteOnly,
                                   currentValue: Slice[Byte],
                                   previous: KeyValue.WriteOnly,
                                   compressDuplicateValues: Boolean,
                                   enablePrefixCompression: Boolean,
                                   entryId: EntryId.Time,
                                   plusSize: Int)(implicit id: TransientToEntryId[_]): EntryWriter.Result =
  //if value is empty byte slice, return None instead of empty Slice. We do not store empty byte arrays.
    previous.value.flatMap(value => if (value.isEmpty) None else Some(value)) flatMap {
      previousValue => {
        if (compressDuplicateValues) //check if values are the same.
          compressExact(
            previous = previous,
            currentValue = currentValue,
            previousValue = previousValue,
            current = current,
            entryId = entryId,
            plusSize = plusSize,
            enablePrefixCompression = enablePrefixCompression
          )
        else
          None
      } orElse {
        if (enablePrefixCompression)
          Some(
            partialCompressWithPrevious(
              current = current,
              entryId = entryId,
              plusSize = plusSize,
              currentValue = currentValue,
              previous = previous,
              previousValue = previousValue,
              enablePrefixCompression = enablePrefixCompression
            )
          )
        else
          None
      }
    } getOrElse {
      uncompressed(
        current = current,
        currentValue = currentValue,
        previous = previous,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression
      )
    }

  private def uncompressed(current: KeyValue.WriteOnly,
                           currentValue: Slice[Byte],
                           previous: KeyValue.WriteOnly,
                           entryId: EntryId.Time,
                           plusSize: Int,
                           enablePrefixCompression: Boolean)(implicit id: TransientToEntryId[_]): EntryWriter.Result = {
    //if previous does not exists write full offsets and then write deadline.
    val currentValueOffset = previous.nextStartValueOffsetPosition
    val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(currentValueOffset)
    val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(currentValue.size)
    val indexEntryBytes =
      DeadlineWriter.write(
        current = current.deadline,
        previous = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
        plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size,
        enablePrefixCompression = enablePrefixCompression
      ).addAll(currentValueOffsetUnsignedBytes)
        .addAll(currentValueLengthUnsignedBytes)

    EntryWriter.Result(
      indexBytes = indexEntryBytes,
      valueBytes = Some(currentValue),
      valueStartOffset = currentValueOffset,
      valueEndOffset = currentValueOffset + currentValue.size - 1
    )
  }

  private def compressExact(previous: KeyValue.WriteOnly,
                            currentValue: Slice[Byte],
                            previousValue: Slice[Byte],
                            current: KeyValue.WriteOnly,
                            entryId: EntryId.Time,
                            plusSize: Int,
                            enablePrefixCompression: Boolean)(implicit id: TransientToEntryId[_]): Option[EntryWriter.Result] =
  //eliminate exact values only. Value size should also be the same.
    Bytes.compressExact(
      previous = previousValue,
      next = currentValue
    ) map {
      _ =>
        //values are the same, no need to write offset & length, jump straight to deadline.
        val indexEntry =
          DeadlineWriter.write(
            current = current.deadline,
            previous = current.previous.flatMap(_.deadline),
            getDeadlineId = entryId.valueFullyCompressed,
            plusSize = plusSize,
            enablePrefixCompression = enablePrefixCompression
          )
        EntryWriter.Result(
          indexBytes = indexEntry,
          valueBytes = None,
          valueStartOffset = previous.currentStartValueOffsetPosition,
          valueEndOffset = previous.currentEndValueOffsetPosition
        )
    }

  private def partialCompressWithPrevious(current: KeyValue.WriteOnly,
                                          entryId: EntryId.Time,
                                          plusSize: Int,
                                          currentValue: Slice[Byte],
                                          previous: KeyValue.WriteOnly,
                                          previousValue: Slice[Byte],
                                          enablePrefixCompression: Boolean)(implicit id: TransientToEntryId[_]): EntryWriter.Result = {
    //if the values are not the same, write compressed offset, length and then deadline.
    val currentValueOffset = previous.nextStartValueOffsetPosition
    val currentValueOffsetBytes = Slice.writeInt(currentValueOffset)
    compressValueOffset(
      current = current,
      previous = previous,
      currentValueOffsetBytes = currentValueOffsetBytes,
      entryId = entryId,
      plusSize = plusSize,
      currentValue = currentValue,
      previousValue = previousValue,
      currentValueOffset = currentValueOffset,
      enablePrefixCompression = enablePrefixCompression
    ) getOrElse {
      compressValueLength(
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        currentValue = currentValue,
        previousValue = previousValue,
        currentValueOffset = currentValueOffset,
        enablePrefixCompression = enablePrefixCompression
      )
    }
  }

  private def compressValueLength(current: KeyValue.WriteOnly,
                                  entryId: EntryId.Time,
                                  plusSize: Int,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte],
                                  currentValueOffset: Int,
                                  enablePrefixCompression: Boolean)(implicit id: TransientToEntryId[_]): EntryWriter.Result =
  //if unable to compress valueOffsetBytes, try compressing value length valueLength bytes.
    compress(Slice.writeInt(previousValue.size), Slice.writeInt(currentValue.size), 1) map {
      case (valueLengthCommonBytes, valueLengthRemainingBytes) =>
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

        val currentUnsignedValueOffsetBytes = Slice.writeIntUnsigned(currentValueOffset)
        val indexEntryBytes =
          DeadlineWriter.write(
            current = current.deadline,
            previous = current.previous.flatMap(_.deadline),
            getDeadlineId = valueLengthId,
            plusSize = plusSize + currentUnsignedValueOffsetBytes.size + valueLengthRemainingBytes.size,
            enablePrefixCompression = enablePrefixCompression
          ).addAll(currentUnsignedValueOffsetBytes)
            .addAll(valueLengthRemainingBytes)

        EntryWriter.Result(
          indexBytes = indexEntryBytes,
          valueBytes = Some(currentValue),
          valueStartOffset = currentValueOffset,
          valueEndOffset = currentValueOffset + currentValue.size - 1
        )
    } getOrElse {
      //unable to compress valueOffset and valueLength bytes, write them as full bytes.

      val currentUnsignedValueOffsetBytes = Slice.writeIntUnsigned(currentValueOffset)
      val currentUnsignedValueLengthBytes = Slice.writeIntUnsigned(currentValue.size)
      val indexEntryBytes =
        DeadlineWriter.write(
          current = current.deadline,
          previous = current.previous.flatMap(_.deadline),
          getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
          plusSize = plusSize + currentUnsignedValueOffsetBytes.size + currentUnsignedValueLengthBytes.size,
          enablePrefixCompression = enablePrefixCompression
        ).addAll(currentUnsignedValueOffsetBytes)
          .addAll(currentUnsignedValueLengthBytes)

      EntryWriter.Result(
        indexBytes = indexEntryBytes,
        valueBytes = Some(currentValue),
        valueStartOffset = currentValueOffset,
        valueEndOffset = currentValueOffset + currentValue.size - 1
      )
    }

  private def compressValueOffset(current: KeyValue.WriteOnly,
                                  previous: KeyValue.WriteOnly,
                                  currentValueOffsetBytes: Slice[Byte],
                                  entryId: EntryId.Time,
                                  plusSize: Int,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte],
                                  currentValueOffset: Int,
                                  enablePrefixCompression: Boolean)(implicit id: TransientToEntryId[_]): Option[EntryWriter.Result] =
    compress(Slice.writeInt(previous.currentStartValueOffsetPosition), currentValueOffsetBytes, 1) map {
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

        compress(Slice.writeInt(previousValue.size), Slice.writeInt(currentValue.size), 1) map {
          case (valueLengthCommonBytes, valueLengthRemainingBytes) =>
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

            val indexEntryBytes =
              DeadlineWriter.write(
                current = current.deadline,
                previous = current.previous.flatMap(_.deadline),
                getDeadlineId = valueLengthId,
                plusSize = plusSize + valueOffsetRemainingBytes.size + valueLengthRemainingBytes.size,
                enablePrefixCompression = enablePrefixCompression
              ).addAll(valueOffsetRemainingBytes)
                .addAll(valueLengthRemainingBytes)

            EntryWriter.Result(
              indexBytes = indexEntryBytes,
              valueBytes = Some(currentValue),
              valueStartOffset = currentValueOffset,
              valueEndOffset = currentValueOffset + currentValue.size - 1
            )
        } getOrElse {
          //if unable to compress valueLengthBytes then write compressed valueOffset with fully valueLength bytes.
          val currentUnsignedValueLengthBytes = Slice.writeIntUnsigned(currentValue.size)
          val indexEntryBytes =
            DeadlineWriter.write(
              current = current.deadline,
              previous = current.previous.flatMap(_.deadline),
              getDeadlineId = valueOffsetId.valueLengthUncompressed,
              plusSize = plusSize + valueOffsetRemainingBytes.size + currentUnsignedValueLengthBytes.size,
              enablePrefixCompression = enablePrefixCompression
            ).addAll(valueOffsetRemainingBytes)
              .addAll(currentUnsignedValueLengthBytes)

          EntryWriter.Result(
            indexBytes = indexEntryBytes,
            valueBytes = Some(currentValue),
            valueStartOffset = currentValueOffset,
            valueEndOffset = currentValueOffset + currentValue.size - 1
          )
        }
    }

  private def noValue(current: KeyValue.WriteOnly,
                      entryId: EntryId.Time,
                      plusSize: Int,
                      enablePrefixCompression: Boolean)(implicit id: TransientToEntryId[_]): EntryWriter.Result = {
    //if there is no value then write deadline.
    val indexEntryBytes =
      DeadlineWriter.write(
        current = current.deadline,
        previous = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.noValue,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression
      )
    //since there is no value, offsets will continue from previous key-values offset.
    EntryWriter.Result(
      indexBytes = indexEntryBytes,
      valueBytes = None,
      valueStartOffset = current.previous.map(_.currentStartValueOffsetPosition).getOrElse(0),
      valueEndOffset = current.previous.map(_.currentEndValueOffsetPosition).getOrElse(0)
    )
  }

  private def noPreviousValue(current: KeyValue.WriteOnly,
                              entryId: EntryId.Time,
                              plusSize: Int,
                              currentValue: Slice[Byte],
                              enablePrefixCompression: Boolean)(implicit id: TransientToEntryId[_]): EntryWriter.Result = {
    //if previous does not exists write offset as the first value in the Transient chain.
    val currentValueOffset = 0
    val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(currentValueOffset)
    val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(currentValue.size)

    val indexEntryBytes =
      DeadlineWriter.write(
        current = current.deadline,
        enablePrefixCompression = enablePrefixCompression,
        previous = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
        plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size
      ).addAll(currentValueOffsetUnsignedBytes)
        .addAll(currentValueLengthUnsignedBytes)

    EntryWriter.Result(
      indexEntryBytes,
      Some(currentValue),
      currentValueOffset,
      currentValue.size - 1
    )
  }
}
