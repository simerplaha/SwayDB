/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
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

import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.data.KeyValue.WriteOnly
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.core.util.Bytes._
import swaydb.data.slice.Slice

private[writer] object ValueWriter {

  def compressibleValue(keyValue: KeyValue.WriteOnly): Option[Slice[Byte]] =
    keyValue match {
      case transient: Transient.SegmentResponse =>
        //if value is empty byte slice, return None instead of empty Slice. We do not store empty byte arrays.
        if (transient.value.exists(_.isEmpty))
          None
        else
          transient.value
      case _: Transient.Group =>
        None
    }

  def write[T](current: KeyValue.WriteOnly,
               enablePrefixCompression: Boolean,
               compressDuplicateValues: Boolean,
               entryId: BaseEntryId.Time,
               plusSize: Int,
               isKeyUncompressed: Boolean,
               hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): KeyValueWriter.Result =
    current match {
      case current: Transient.SegmentResponse =>
        (compressibleValue(current), current.previous.flatMap(compressibleValue)) match {
          case (Some(currentValue), Some(previousValue)) =>
            compressWithPrevious(
              current = current,
              previousValue = previousValue,
              currentValue = currentValue,
              previous = current.previous.get,
              compressDuplicateValues = compressDuplicateValues,
              enablePrefixCompression = enablePrefixCompression,
              entryId = entryId,
              plusSize = plusSize,
              isKeyUncompressed = isKeyUncompressed,
              hasPrefixCompressed = hasPrefixCompressed
            )

          case (Some(currentValue), None) =>
            uncompressed(
              current = current,
              entryId = entryId,
              plusSize = plusSize,
              currentValue = Slice(currentValue),
              enablePrefixCompression = enablePrefixCompression,
              isKeyUncompressed = isKeyUncompressed,
              hasPrefixCompressed = hasPrefixCompressed
            )

          case (None, Some(_)) | (None, None) =>
            noValue(
              current = current,
              entryId = entryId,
              plusSize = plusSize,
              enablePrefixCompression = enablePrefixCompression,
              isKeyUncompressed = isKeyUncompressed,
              hasPrefixCompressed = hasPrefixCompressed
            )
        }

      case group: Transient.Group =>
        uncompressed(
          current = current,
          currentValue = group.result.segmentBytes,
          entryId = entryId,
          plusSize = plusSize,
          enablePrefixCompression = enablePrefixCompression,
          isKeyUncompressed = isKeyUncompressed,
          hasPrefixCompressed = hasPrefixCompressed
        )
    }

  private def uncompressed(current: KeyValue.WriteOnly,
                           currentValue: Slice[Slice[Byte]],
                           entryId: BaseEntryId.Time,
                           plusSize: Int,
                           enablePrefixCompression: Boolean,
                           isKeyUncompressed: Boolean,
                           hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): KeyValueWriter.Result = {
    //if previous does not exists write full offsets and then write deadline.
    val currentValueSize = currentValue.foldLeft(0)(_ + _.written)

    val currentValueOffset = current.previous.map(_.nextStartValueOffsetPosition) getOrElse 0
    val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(currentValueOffset)
    val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(currentValueSize)

    val (indexEntryBytes, isPrefixCompressed) =
      DeadlineWriter.write(
        currentDeadline = current.deadline,
        previousDeadline = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
        enablePrefixCompression = enablePrefixCompression,
        plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size,
        isKeyCompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed
      )

    indexEntryBytes
      .addAll(currentValueOffsetUnsignedBytes)
      .addAll(currentValueLengthUnsignedBytes)

    KeyValueWriter.Result(
      indexBytes = indexEntryBytes,
      valueBytes = currentValue,
      valueStartOffset = currentValueOffset,
      valueEndOffset = currentValueOffset + currentValueSize - 1,
      isPrefixCompressed = isPrefixCompressed
    )
  }

  private def compressWithPrevious(current: KeyValue.WriteOnly,
                                   previousValue: Slice[Byte],
                                   currentValue: Slice[Byte],
                                   previous: KeyValue.WriteOnly,
                                   compressDuplicateValues: Boolean,
                                   enablePrefixCompression: Boolean,
                                   entryId: BaseEntryId.Time,
                                   plusSize: Int,
                                   isKeyUncompressed: Boolean,
                                   hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): KeyValueWriter.Result = {
    if (compressDuplicateValues) //check if values are the same.
      compressExact(
        previous = previous,
        currentValue = currentValue,
        previousValue = previousValue,
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed
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
          isKeyUncompressed = isKeyUncompressed,
          hasPrefixCompressed = hasPrefixCompressed
        )
      )
    else
      None
  } getOrElse {
    uncompressed(
      current = current,
      currentValue = Slice(currentValue),
      entryId = entryId,
      plusSize = plusSize,
      enablePrefixCompression = enablePrefixCompression,
      isKeyUncompressed = isKeyUncompressed,
      hasPrefixCompressed = hasPrefixCompressed
    )
  }

  private def compressExact(previous: KeyValue.WriteOnly,
                            currentValue: Slice[Byte],
                            previousValue: Slice[Byte],
                            current: KeyValue.WriteOnly,
                            entryId: BaseEntryId.Time,
                            plusSize: Int,
                            enablePrefixCompression: Boolean,
                            isKeyUncompressed: Boolean,
                            hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[KeyValueWriter.Result] =
  //todo if prefix compression is disable then write offsets with
  //eliminate exact values only. Value size should also be the same.
    Bytes.compressExact(
      previous = previousValue,
      next = currentValue
    ) map {
      _ =>
        val (indexEntry, isPrefixCompressed) =
          if (enablePrefixCompression) {
            //values are the same, no need to write offset & length, jump straight to deadline.
            DeadlineWriter.write(
              currentDeadline = current.deadline,
              previousDeadline = current.previous.flatMap(_.deadline),
              getDeadlineId = entryId.valueFullyCompressed.valueOffsetFullyCompressed.valueLengthFullyCompressed,
              plusSize = plusSize,
              enablePrefixCompression = enablePrefixCompression,
              isKeyCompressed = isKeyUncompressed,
              hasPrefixCompressed = true
            )
          } else {
            //use previous values offset but write the offset and length information.
            val currentValueOffsetUnsignedBytes = Slice.writeIntUnsigned(previous.currentStartValueOffsetPosition)
            val currentValueLengthUnsignedBytes = Slice.writeIntUnsigned(currentValue.size)

            val (indexEntryBytes, isPrefixCompressed) =
              DeadlineWriter.write(
                currentDeadline = current.deadline,
                previousDeadline = current.previous.flatMap(_.deadline),
                getDeadlineId = entryId.valueFullyCompressed.valueOffsetUncompressed.valueLengthUncompressed,
                plusSize = plusSize + currentValueOffsetUnsignedBytes.size + currentValueLengthUnsignedBytes.size,
                enablePrefixCompression = enablePrefixCompression,
                isKeyCompressed = isKeyUncompressed,
                hasPrefixCompressed = hasPrefixCompressed
              )

            indexEntryBytes
              .addAll(currentValueOffsetUnsignedBytes)
              .addAll(currentValueLengthUnsignedBytes)

            (indexEntryBytes, isPrefixCompressed)
          }

        KeyValueWriter.Result(
          indexBytes = indexEntry,
          valueBytes = Slice.emptyEmptyBytes,
          valueStartOffset = previous.currentStartValueOffsetPosition,
          valueEndOffset = previous.currentEndValueOffsetPosition,
          isPrefixCompressed = isPrefixCompressed
        )
    }

  private def partialCompressWithPrevious(current: KeyValue.WriteOnly,
                                          entryId: BaseEntryId.Time,
                                          plusSize: Int,
                                          currentValue: Slice[Byte],
                                          previous: KeyValue.WriteOnly,
                                          previousValue: Slice[Byte],
                                          isKeyUncompressed: Boolean,
                                          hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): KeyValueWriter.Result = {
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
      isKeyUncompressed = isKeyUncompressed
    ) getOrElse {
      compressValueLength(
        current = current,
        entryId = entryId,
        plusSize = plusSize,
        currentValue = currentValue,
        previousValue = previousValue,
        currentValueOffset = currentValueOffset,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed
      )
    }
  }

  private def compressValueLength(current: KeyValue.WriteOnly,
                                  entryId: BaseEntryId.Time,
                                  plusSize: Int,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte],
                                  currentValueOffset: Int,
                                  isKeyUncompressed: Boolean,
                                  hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): KeyValueWriter.Result =
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
        val (indexEntryBytes, isPrefixCompressed) =
          DeadlineWriter.write(
            currentDeadline = current.deadline,
            previousDeadline = current.previous.flatMap(_.deadline),
            getDeadlineId = valueLengthId,
            plusSize = plusSize + currentUnsignedValueOffsetBytes.size + valueLengthRemainingBytes.size,
            enablePrefixCompression = true,
            isKeyCompressed = isKeyUncompressed,
            hasPrefixCompressed = true
          )
        indexEntryBytes
          .addAll(currentUnsignedValueOffsetBytes)
          .addAll(valueLengthRemainingBytes)

        KeyValueWriter.Result(
          indexBytes = indexEntryBytes,
          valueBytes = Slice(currentValue),
          valueStartOffset = currentValueOffset,
          valueEndOffset = currentValueOffset + currentValue.size - 1,
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
          getDeadlineId = entryId.valueUncompressed.valueOffsetUncompressed.valueLengthUncompressed,
          plusSize = plusSize + currentUnsignedValueOffsetBytes.size + currentUnsignedValueLengthBytes.size,
          enablePrefixCompression = true,
          isKeyCompressed = isKeyUncompressed,
          hasPrefixCompressed = hasPrefixCompressed
        )

      indexEntryBytes
        .addAll(currentUnsignedValueOffsetBytes)
        .addAll(currentUnsignedValueLengthBytes)

      KeyValueWriter.Result(
        indexBytes = indexEntryBytes,
        valueBytes = Slice(currentValue),
        valueStartOffset = currentValueOffset,
        valueEndOffset = currentValueOffset + currentValue.size - 1,
        isPrefixCompressed = isPrefixCompressed
      )
    }

  private def compressValueOffset(current: KeyValue.WriteOnly,
                                  previous: KeyValue.WriteOnly,
                                  currentValueOffsetBytes: Slice[Byte],
                                  entryId: BaseEntryId.Time,
                                  plusSize: Int,
                                  currentValue: Slice[Byte],
                                  previousValue: Slice[Byte],
                                  currentValueOffset: Int,
                                  isKeyUncompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[KeyValueWriter.Result] =
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

            val (indexEntryBytes, _) =
              DeadlineWriter.write(
                currentDeadline = current.deadline,
                previousDeadline = current.previous.flatMap(_.deadline),
                getDeadlineId = valueLengthId,
                plusSize = plusSize + valueOffsetRemainingBytes.size + valueLengthRemainingBytes.size,
                enablePrefixCompression = true,
                isKeyCompressed = isKeyUncompressed,
                hasPrefixCompressed = true
              )

            indexEntryBytes
              .addAll(valueOffsetRemainingBytes)
              .addAll(valueLengthRemainingBytes)

            KeyValueWriter.Result(
              indexBytes = indexEntryBytes,
              valueBytes = Slice(currentValue),
              valueStartOffset = currentValueOffset,
              valueEndOffset = currentValueOffset + currentValue.size - 1,
              isPrefixCompressed = true
            )
        } getOrElse {
          //if unable to compress valueLengthBytes then write compressed valueOffset with fully valueLength bytes.
          val currentUnsignedValueLengthBytes = Slice.writeIntUnsigned(currentValue.size)
          val (indexEntryBytes, isPrefixCompressed) =
            DeadlineWriter.write(
              currentDeadline = current.deadline,
              previousDeadline = current.previous.flatMap(_.deadline),
              getDeadlineId = valueOffsetId.valueLengthUncompressed,
              plusSize = plusSize + valueOffsetRemainingBytes.size + currentUnsignedValueLengthBytes.size,
              enablePrefixCompression = true,
              isKeyCompressed = isKeyUncompressed,
              hasPrefixCompressed = true
            )

          indexEntryBytes
            .addAll(valueOffsetRemainingBytes)
            .addAll(currentUnsignedValueLengthBytes)

          KeyValueWriter.Result(
            indexBytes = indexEntryBytes,
            valueBytes = Slice(currentValue),
            valueStartOffset = currentValueOffset,
            valueEndOffset = currentValueOffset + currentValue.size - 1,
            isPrefixCompressed = isPrefixCompressed
          )
        }
    }

  private def noValue(current: KeyValue.WriteOnly,
                      entryId: BaseEntryId.Time,
                      plusSize: Int,
                      enablePrefixCompression: Boolean,
                      isKeyUncompressed: Boolean,
                      hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): KeyValueWriter.Result = {
    //if there is no value then write deadline.
    val (indexEntryBytes, isPrefixCompressed) =
      DeadlineWriter.write(
        currentDeadline = current.deadline,
        previousDeadline = current.previous.flatMap(_.deadline),
        getDeadlineId = entryId.noValue,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyCompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed
      )
    //since there is no value, offsets will continue from previous key-values offset.
    KeyValueWriter.Result(
      indexBytes = indexEntryBytes,
      valueBytes = Slice.emptyEmptyBytes,
      valueStartOffset = current.previous.map(_.currentStartValueOffsetPosition).getOrElse(0),
      valueEndOffset = current.previous.map(_.currentEndValueOffsetPosition).getOrElse(0),
      isPrefixCompressed = isPrefixCompressed
    )
  }
}
