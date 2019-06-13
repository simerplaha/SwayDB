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

object ValueWriter {

  def write(current: KeyValue.WriteOnly,
            compressDuplicateValues: Boolean,
            entryId: EntryId.Time,
            plusSize: Int)(implicit id: TransientToEntryId[_]): (Slice[Byte], Option[Slice[Byte]], Int, Int) =
    current.value map {
      currentValue: Slice[Byte] =>
        current.previous map {
          previous: KeyValue.WriteOnly =>
            compressWithPrevious(
              current = current,
              currentValue = currentValue,
              previous = previous,
              compressDuplicateValues = compressDuplicateValues,
              entryId = entryId,
              plusSize = plusSize)
        } getOrElse {
          writeNoPreviousValue(
            current = current,
            entryId = entryId,
            plusSize = plusSize,
            currentValue = currentValue
          )
        }
    } getOrElse {
      writeNoValue(
        current = current,
        entryId = entryId,
        plusSize = plusSize
      )
    }

  def compressWithPrevious(current: KeyValue.WriteOnly,
                           currentValue: Slice[Byte],
                           previous: KeyValue.WriteOnly,
                           compressDuplicateValues: Boolean,
                           entryId: EntryId.Time,
                           plusSize: Int)(implicit id: TransientToEntryId[_]) =
  //if value is empty byte slice, return None instead of empty Slice. We do not store empty byte arrays.
    previous.value.flatMap(value => if (value.isEmpty) None else Some(value)) map {
      previousValue => {
        if (compressDuplicateValues) //check if values are the same.
          compressExact(
            previous = previous,
            currentValue = currentValue,
            previousValue = previousValue,
            current = current,
            entryId = entryId,
            plusSize = plusSize
          )
        else
          None
      } getOrElse {
        partialCompressWithPrevious(
          current = current,
          entryId = entryId,
          plusSize = plusSize,
          currentValue = currentValue,
          previous = previous,
          previousValue = previousValue
        )
      }
    } getOrElse {
      writeUncompressed(
        current = current,
        currentValue = currentValue,
        previous = previous,
        entryId = entryId,
        plusSize = plusSize
      )
    }

  def writeUncompressed(current: KeyValue.WriteOnly,
                        currentValue: Slice[Byte],
                        previous: KeyValue.WriteOnly,
                        entryId: EntryId.Time,
                        plusSize: Int)(implicit id: TransientToEntryId[_]) = {
    //if previous does not exists write full offsets and then write deadline.
    val currentValueOffset = previous.nextStartValueOffsetPosition
    val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(currentValueOffset)
    val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(currentValue.size)
    val indexEntryBytes =
      DeadlineWriter.write(
        current = current.deadline,
        previous = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
        plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size
      ).addAll(currentValueOffsetUnsignedBytes)
        .addAll(currentValueLengthUnsignedBytes)

    (indexEntryBytes, Some(currentValue), currentValueOffset, currentValueOffset + currentValue.size - 1)
  }

  def compressExact(previous: KeyValue.WriteOnly,
                    currentValue: Slice[Byte],
                    previousValue: Slice[Byte],
                    current: KeyValue.WriteOnly,
                    entryId: EntryId.Time,
                    plusSize: Int)(implicit id: TransientToEntryId[_]): Option[(Slice[Byte], None.type, Int, Int)] = {
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
            plusSize = plusSize
          )
        (indexEntry, None, previous.currentStartValueOffsetPosition, previous.currentEndValueOffsetPosition)
    }
  }

  def partialCompressWithPrevious(current: KeyValue.WriteOnly,
                                  entryId: EntryId.Time,
                                  plusSize: Int,
                                  currentValue: Slice[Byte],
                                  previous: KeyValue.WriteOnly,
                                  previousValue: Slice[Byte])(implicit id: TransientToEntryId[_]) = {
    //if the values are not the same, write compressed offset, length and then deadline.
    val currentValueOffset = previous.nextStartValueOffsetPosition
    val currentValueOffsetBytes = Slice.writeInt(currentValueOffset)
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
                plusSize = plusSize + valueOffsetRemainingBytes.size + valueLengthRemainingBytes.size
              ).addAll(valueOffsetRemainingBytes)
                .addAll(valueLengthRemainingBytes)

            (indexEntryBytes, Some(currentValue), currentValueOffset, currentValueOffset + currentValue.size - 1)
        } getOrElse {
          //if unable to compress valueLengthBytes then write compressed valueOffset with fully valueLength bytes.
          val currentUnsignedValueLengthBytes = Slice.writeIntUnsigned(currentValue.size)
          val indexEntryBytes =
            DeadlineWriter.write(
              current = current.deadline,
              previous = current.previous.flatMap(_.deadline),
              getDeadlineId = valueOffsetId.valueLengthUncompressed,
              plusSize = plusSize + valueOffsetRemainingBytes.size + currentUnsignedValueLengthBytes.size
            ).addAll(valueOffsetRemainingBytes)
              .addAll(currentUnsignedValueLengthBytes)

          (indexEntryBytes, Some(currentValue), currentValueOffset, currentValueOffset + currentValue.size - 1)
        }
    } getOrElse {
      //if unable to compress valueOffsetBytes, compressed valueLength bytes.
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
              plusSize = plusSize + currentUnsignedValueOffsetBytes.size + valueLengthRemainingBytes.size
            ).addAll(currentUnsignedValueOffsetBytes)
              .addAll(valueLengthRemainingBytes)

          (indexEntryBytes, Some(currentValue), currentValueOffset, currentValueOffset + currentValue.size - 1)
      } getOrElse {
        //unable to compress valueOffset and valueLength bytes, write them as full bytes.

        val currentUnsignedValueOffsetBytes = Slice.writeIntUnsigned(currentValueOffset)
        val currentUnsignedValueLengthBytes = Slice.writeIntUnsigned(currentValue.size)
        val indexEntryBytes =
          DeadlineWriter.write(
            current = current.deadline,
            previous = current.previous.flatMap(_.deadline),
            getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
            plusSize = plusSize + currentUnsignedValueOffsetBytes.size + currentUnsignedValueLengthBytes.size
          ).addAll(currentUnsignedValueOffsetBytes)
            .addAll(currentUnsignedValueLengthBytes)

        (indexEntryBytes, Some(currentValue), currentValueOffset, currentValueOffset + currentValue.size - 1)
      }
    }
  }

  def writeNoValue(current: KeyValue.WriteOnly,
                   entryId: EntryId.Time,
                   plusSize: Int)(implicit id: TransientToEntryId[_]) = {
    //if there is no value then write deadline.
    val indexEntryBytes =
      DeadlineWriter.write(
        current = current.deadline,
        previous = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.noValue,
        plusSize = plusSize
      )
    //since there is no value, offsets will continue from previous key-values offset.
    (indexEntryBytes, None, current.previous.map(_.currentStartValueOffsetPosition).getOrElse(0), current.previous.map(_.currentEndValueOffsetPosition).getOrElse(0))
  }

  def writeNoPreviousValue(current: KeyValue.WriteOnly,
                           entryId: EntryId.Time,
                           plusSize: Int,
                           currentValue: Slice[Byte])(implicit id: TransientToEntryId[_]) = {
    //if previous does not exists write offset as the first value in the Transient chain.
    val currentValueOffset = 0
    val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(currentValueOffset)
    val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(currentValue.size)

    val indexEntryBytes =
      DeadlineWriter.write(
        current = current.deadline,
        previous = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
        plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size
      ).addAll(currentValueOffsetUnsignedBytes)
        .addAll(currentValueLengthUnsignedBytes)

    (indexEntryBytes, Some(currentValue), currentValueOffset, currentValue.size - 1)
  }
}
