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

import swaydb.core.data.Transient
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice

private[writer] object ValueWriter {

  def write[T](current: Transient,
               enablePrefixCompression: Boolean,
               compressDuplicateValues: Boolean,
               entryId: BaseEntryId.Time,
               plusSize: Int,
               isKeyUncompressed: Boolean,
               hasPrefixCompressed: Boolean,
               adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): EntryWriter.WriteResult =
    if (Transient.hasNoValue(current))
      noValue(
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      )
    else
      current.previous map {
        previous =>
          compress(
            current = current,
            previous = previous,
            enablePrefixCompression = enablePrefixCompression,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId,
            plusSize = plusSize,
            isKeyUncompressed = isKeyUncompressed,
            hasPrefixCompressed = hasPrefixCompressed,
            adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
          )
      } getOrElse {
        uncompressed(
          current = current,
          currentValues = current.values,
          entryId = entryId,
          plusSize = plusSize,
          enablePrefixCompression = enablePrefixCompression,
          isKeyUncompressed = isKeyUncompressed,
          hasPrefixCompressed = hasPrefixCompressed,
          adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
        )
      }

  private def compress[T](current: Transient,
                          previous: Transient,
                          enablePrefixCompression: Boolean,
                          compressDuplicateValues: Boolean,
                          entryId: BaseEntryId.Time,
                          plusSize: Int,
                          isKeyUncompressed: Boolean,
                          hasPrefixCompressed: Boolean,
                          adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): EntryWriter.WriteResult =
    if (compressDuplicateValues) //check if values are the same.
      duplicateValue(
        previous = previous,
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      ) getOrElse {
        partialCompress(
          previous = previous,
          current = current,
          entryId = entryId,
          plusSize = plusSize,
          enablePrefixCompression = enablePrefixCompression,
          isKeyUncompressed = isKeyUncompressed,
          hasPrefixCompressed = hasPrefixCompressed,
          adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
        )
      }
    else
      partialCompress(
        previous = previous,
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      )

  private def partialCompress[T](current: Transient,
                                 previous: Transient,
                                 enablePrefixCompression: Boolean,
                                 entryId: BaseEntryId.Time,
                                 plusSize: Int,
                                 isKeyUncompressed: Boolean,
                                 hasPrefixCompressed: Boolean,
                                 adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): EntryWriter.WriteResult =
    if (enablePrefixCompression)
      (Transient.compressibleValue(current), Transient.compressibleValue(previous)) match {
        case (Some(currentValue), Some(previousValue)) =>
          partialCompress(
            current = current,
            entryId = entryId,
            plusSize = plusSize,
            currentValue = currentValue,
            previous = previous,
            previousValue = previousValue,
            isKeyUncompressed = isKeyUncompressed,
            hasPrefixCompressed = hasPrefixCompressed,
            adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
          )

        case (Some(_), None) =>
          uncompressed(
            current = current,
            currentValues = current.values,
            entryId = entryId,
            plusSize = plusSize,
            enablePrefixCompression = enablePrefixCompression,
            isKeyUncompressed = isKeyUncompressed,
            hasPrefixCompressed = hasPrefixCompressed,
            adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
          )

        case (None, _) =>
          noValue(
            current = current,
            entryId = entryId,
            plusSize = plusSize,
            enablePrefixCompression = enablePrefixCompression,
            isKeyUncompressed = isKeyUncompressed,
            hasPrefixCompressed = hasPrefixCompressed,
            adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
          )
      }
    else
      uncompressed(
        current = current,
        currentValues = current.values,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      )

  private def uncompressed(current: Transient,
                           currentValues: Slice[Slice[Byte]],
                           entryId: BaseEntryId.Time,
                           plusSize: Int,
                           enablePrefixCompression: Boolean,
                           isKeyUncompressed: Boolean,
                           hasPrefixCompressed: Boolean,
                           adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): EntryWriter.WriteResult = {
    //if previous does not exists write full offsets and then write deadline.
    val currentValueSize = currentValues.foldLeft(0)(_ + _.size)
    val currentValueOffset = current.previous.map(_.nextStartValueOffsetPosition) getOrElse 0
    val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(currentValueOffset)
    val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(currentValueSize)

    val (indexEntryBytes, isPrefixCompressed) =
      DeadlineWriter.write(
        currentDeadline = current.deadline,
        previousDeadline = current.previous.flatMap(_.deadline),
        deadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
        enablePrefixCompression = enablePrefixCompression,
        plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size,
        isKeyCompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      )

    indexEntryBytes
      .addAll(currentValueOffsetUnsignedBytes)
      .addAll(currentValueLengthUnsignedBytes)

    EntryWriter.WriteResult(
      indexBytes = indexEntryBytes,
      valueBytes = currentValues,
      valueStartOffset = currentValueOffset,
      valueEndOffset = currentValueOffset + currentValueSize - 1,
      thisKeyValueAccessIndexPosition = 0,
      isPrefixCompressed = isPrefixCompressed
    )
  }

  private def noValue(current: Transient,
                      entryId: BaseEntryId.Time,
                      plusSize: Int,
                      enablePrefixCompression: Boolean,
                      isKeyUncompressed: Boolean,
                      hasPrefixCompressed: Boolean,
                      adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): EntryWriter.WriteResult = {
    //if there is no value then write deadline.
    val (indexEntryBytes, isPrefixCompressed) =
      DeadlineWriter.write(
        currentDeadline = current.deadline,
        previousDeadline = current.previous.flatMap(_.deadline),
        deadlineId = entryId.noValue,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyCompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      )
    //since there is no value, offsets will continue from previous key-values offset.
    EntryWriter.WriteResult(
      indexBytes = indexEntryBytes,
      valueBytes = Slice.emptyEmptyBytes,
      valueStartOffset = current.previous.map(_.currentStartValueOffsetPosition).getOrElse(0),
      valueEndOffset = current.previous.map(_.currentEndValueOffsetPosition).getOrElse(0),
      thisKeyValueAccessIndexPosition = 0,
      isPrefixCompressed = isPrefixCompressed
    )
  }

  private def duplicateValue(current: Transient,
                             previous: Transient,
                             entryId: BaseEntryId.Time,
                             plusSize: Int,
                             enablePrefixCompression: Boolean,
                             isKeyUncompressed: Boolean,
                             hasPrefixCompressed: Boolean,
                             adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[EntryWriter.WriteResult] =
    if (Transient.hasSameValue(previous, current)) { //no need to serialised values and then compare. Simply compare the value objects and check for equality.
      val (indexEntry, isPrefixCompressed) =
        if (enablePrefixCompression) {
          //values are the same, no need to write offset & length, jump straight to deadline.
          DeadlineWriter.write(
            currentDeadline = current.deadline,
            previousDeadline = current.previous.flatMap(_.deadline),
            deadlineId = entryId.valueFullyCompressed.valueOffsetFullyCompressed.valueLengthFullyCompressed,
            plusSize = plusSize,
            enablePrefixCompression = enablePrefixCompression,
            isKeyCompressed = isKeyUncompressed,
            hasPrefixCompressed = true,
            adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
          )
        } else {
          //use previous values offset but write the offset and length information.
          val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(previous.currentStartValueOffsetPosition)
          val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(previous.currentEndValueOffsetPosition - previous.currentStartValueOffsetPosition + 1)

          val (indexEntryBytes, isPrefixCompressed) =
            DeadlineWriter.write(
              currentDeadline = current.deadline,
              previousDeadline = current.previous.flatMap(_.deadline),
              deadlineId = entryId.valueFullyCompressed.valueOffsetUncompressed.valueLengthUncompressed,
              plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size,
              enablePrefixCompression = enablePrefixCompression,
              isKeyCompressed = isKeyUncompressed,
              hasPrefixCompressed = hasPrefixCompressed,
              adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
            )

          indexEntryBytes
            .addAll(currentValueOffsetUnsignedBytes)
            .addAll(currentValueLengthUnsignedBytes)

          (indexEntryBytes, isPrefixCompressed)
        }

      Some(
        EntryWriter.WriteResult(
          indexBytes = indexEntry,
          valueBytes = Slice.emptyEmptyBytes,
          valueStartOffset = previous.currentStartValueOffsetPosition,
          valueEndOffset = previous.currentEndValueOffsetPosition,
          thisKeyValueAccessIndexPosition = 0,
          isPrefixCompressed = isPrefixCompressed
        )
      )
    }
    else
      None

  private def partialCompress(current: Transient,
                              entryId: BaseEntryId.Time,
                              plusSize: Int,
                              currentValue: Slice[Byte],
                              previous: Transient,
                              previousValue: Slice[Byte],
                              isKeyUncompressed: Boolean,
                              hasPrefixCompressed: Boolean,
                              adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): EntryWriter.WriteResult = {
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
      isKeyUncompressed = isKeyUncompressed,
      adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
    ) getOrElse {
      compressValueLength(
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        currentValue = currentValue,
        previousValue = previousValue,
        currentValueOffset = currentValueOffset,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      )
    }
  }

  private def compressValueLength(current: Transient,
                                  entryId: BaseEntryId.Time,
                                  plusSize: Int,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte],
                                  currentValueOffset: Int,
                                  isKeyUncompressed: Boolean,
                                  hasPrefixCompressed: Boolean,
                                  adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): EntryWriter.WriteResult =
  //if unable to compress valueOffsetBytes, try compressing value length valueLength bytes.
    Bytes.compress(Slice.writeInt(previousValue.size), Slice.writeInt(currentValue.size), 1) map {
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
        val (indexEntryBytes, isPrefixCompressed) =
          DeadlineWriter.write(
            currentDeadline = current.deadline,
            previousDeadline = current.previous.flatMap(_.deadline),
            deadlineId = valueLengthId,
            plusSize = plusSize + currentUnsignedValueOffsetBytes.size + valueLengthRemainingBytes.size,
            enablePrefixCompression = true,
            isKeyCompressed = isKeyUncompressed,
            hasPrefixCompressed = true,
            adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
          )
        indexEntryBytes
          .addAll(currentUnsignedValueOffsetBytes)
          .addAll(valueLengthRemainingBytes)

        EntryWriter.WriteResult(
          indexBytes = indexEntryBytes,
          valueBytes = Slice(currentValue),
          valueStartOffset = currentValueOffset,
          valueEndOffset = currentValueOffset + currentValue.size - 1,
          thisKeyValueAccessIndexPosition = 0,
          isPrefixCompressed = isPrefixCompressed
        )
    } getOrElse {
      //unable to compress valueOffset and valueLength bytes, write them as full bytes.
      val currentUnsignedValueOffsetBytes = Slice.writeIntUnsigned(currentValueOffset)
      val currentUnsignedValueLengthBytes = Slice.writeIntUnsigned(currentValue.size)
      val (indexEntryBytes, isPrefixCompressed) =
        DeadlineWriter.write(
          currentDeadline = current.deadline,
          previousDeadline = current.previous.flatMap(_.deadline),
          deadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
          plusSize = plusSize + currentUnsignedValueOffsetBytes.size + currentUnsignedValueLengthBytes.size,
          enablePrefixCompression = true,
          isKeyCompressed = isKeyUncompressed,
          hasPrefixCompressed = hasPrefixCompressed,
          adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
        )

      indexEntryBytes
        .addAll(currentUnsignedValueOffsetBytes)
        .addAll(currentUnsignedValueLengthBytes)

      EntryWriter.WriteResult(
        indexBytes = indexEntryBytes,
        valueBytes = Slice(currentValue),
        valueStartOffset = currentValueOffset,
        valueEndOffset = currentValueOffset + currentValue.size - 1,
        thisKeyValueAccessIndexPosition = 0,
        isPrefixCompressed = isPrefixCompressed
      )
    }

  private def compressValueOffset(current: Transient,
                                  previous: Transient,
                                  currentValueOffsetBytes: Slice[Byte],
                                  entryId: BaseEntryId.Time,
                                  plusSize: Int,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte],
                                  currentValueOffset: Int,
                                  isKeyUncompressed: Boolean,
                                  adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[EntryWriter.WriteResult] =
    Bytes.compress(Slice.writeInt(previous.currentStartValueOffsetPosition), currentValueOffsetBytes, 1) map {
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

        Bytes.compress(Slice.writeInt(previousValue.size), Slice.writeInt(currentValue.size), 1) map {
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

            val (indexEntryBytes, _) =
              DeadlineWriter.write(
                currentDeadline = current.deadline,
                previousDeadline = current.previous.flatMap(_.deadline),
                deadlineId = valueLengthId,
                plusSize = plusSize + valueOffsetRemainingBytes.size + valueLengthRemainingBytes.size,
                enablePrefixCompression = true,
                isKeyCompressed = isKeyUncompressed,
                hasPrefixCompressed = true,
                adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
              )

            indexEntryBytes
              .addAll(valueOffsetRemainingBytes)
              .addAll(valueLengthRemainingBytes)

            EntryWriter.WriteResult(
              indexBytes = indexEntryBytes,
              valueBytes = Slice(currentValue),
              valueStartOffset = currentValueOffset,
              valueEndOffset = currentValueOffset + currentValue.size - 1,
              thisKeyValueAccessIndexPosition = 0,
              isPrefixCompressed = true
            )
        } getOrElse {
          //if unable to compress valueLengthBytes then write compressed valueOffset with fully valueLength bytes.
          val currentUnsignedValueLengthBytes = Slice.writeIntUnsigned(currentValue.size)
          val (indexEntryBytes, isPrefixCompressed) =
            DeadlineWriter.write(
              currentDeadline = current.deadline,
              previousDeadline = current.previous.flatMap(_.deadline),
              deadlineId = valueOffsetId.valueLengthUncompressed,
              plusSize = plusSize + valueOffsetRemainingBytes.size + currentUnsignedValueLengthBytes.size,
              enablePrefixCompression = true,
              isKeyCompressed = isKeyUncompressed,
              hasPrefixCompressed = true,
              adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
            )

          indexEntryBytes
            .addAll(valueOffsetRemainingBytes)
            .addAll(currentUnsignedValueLengthBytes)

          EntryWriter.WriteResult(
            indexBytes = indexEntryBytes,
            valueBytes = Slice(currentValue),
            valueStartOffset = currentValueOffset,
            valueEndOffset = currentValueOffset + currentValue.size - 1,
            thisKeyValueAccessIndexPosition = 0,
            isPrefixCompressed = isPrefixCompressed
          )
        }
    }
}
