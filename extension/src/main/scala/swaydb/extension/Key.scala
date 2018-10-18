/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.extension

import swaydb.TypedOrdering
import swaydb.core.io.reader.Reader
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteUtil
import swaydb.order.KeyOrder
import swaydb.serializers.Serializer

import scala.util.Try

sealed trait Key[+K] {
  def parentMapKeys: Seq[K]
}

object Key {

  case class Start[K](parentMapKeys: Seq[K]) extends Key[K]
  case class EntriesStart[K](parentMapKeys: Seq[K]) extends Key[K]
  case class Entry[K](parentMapKeys: Seq[K], dataKey: K) extends Key[K]
  case class EntriesEnd[K](parentMapKeys: Seq[K]) extends Key[K]
  case class SubMapsStart[K](parentMapKeys: Seq[K]) extends Key[K]
  case class SubMap[K](parentMapKeys: Seq[K], subMapKey: K) extends Key[K]
  case class SubMapsEnd[K](parentMapKeys: Seq[K]) extends Key[K]
  case class End[K](parentMapKeys: Seq[K]) extends Key[K]

  //formatId for the serializers. Each key is prepended with this formatId.
  private val formatId: Byte = 0.toByte
  //starting of the Map
  private val start: Byte = 1.toByte
  //ids for entries block
  private val entriesStart: Byte = 2.toByte
  private val entry: Byte = 3.toByte
  private val entriesEnd: Byte = 10.toByte
  //ids for subMaps block
  private val subMapsStart: Byte = 11.toByte
  private val subMap: Byte = 12.toByte
  private val subMapsEnd: Byte = 20.toByte
  //leave enough space to allow for adding other data like mapSize etc.
  private val end: Byte = 100.toByte

  private[swaydb] def writeKeys[K](keys: Seq[K], keySerializer: Serializer[K]): Slice[Byte] = {
    if (keys.isEmpty)
      Slice.emptyBytes
    else {
      val slices = keys map keySerializer.write
      val size = slices.foldLeft(0) {
        case (size, bytes) =>
          size + ByteUtil.sizeOf(bytes.size) + bytes.size
      }
      val slice = Slice.create[Byte](size)
      slices foreach {
        keySlice =>
          slice addIntUnsigned keySlice.size
          slice addAll keySlice
      }
      slice
    }
  }

  private def readKeys[K](keys: Slice[Byte], keySerializer: Serializer[K]): Try[Seq[K]] = {
    def readOne(reader: Reader): Try[Option[K]] =
      reader
        .readIntUnsigned()
        .flatMap(reader.read)
        .map {
          bytes =>
            if (bytes.isEmpty)
              None
            else
              Some(keySerializer.read(bytes))
        }

    Reader(keys).foldLeftTry(Seq.empty[K]) {
      case (keys, reader) =>
        readOne(reader) map {
          key =>
            key map {
              key =>
                keys :+ key
            } getOrElse {
              keys
            }
        }
    }
  }

  /**
    * Serializer implementation for [[Key]] types.
    *
    * Formats:
    * [[Start]] - formatId|mapKey.size|mapKey|dataType
    * [[Entry]] - formatId|mapKey.size|mapKey|dataType|dataKey
    * [[End]]   - formatId|mapKey.size|mapKey|dataType
    *
    * mapKey   - the unique id of the Map.
    * dataType - the type of [[Key]] which can be either one of [[start]], [[entry]] or [[end]]
    * dataKey  - the entry key for the Map.
    */
  implicit def serializer[K](implicit keySerializer: Serializer[K]): Serializer[Key[K]] =
    new Serializer[Key[K]] {
      override def write(data: Key[K]): Slice[Byte] =
        data match {
          case Key.Start(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.start)

          case Key.EntriesStart(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.entriesStart)

          case Key.Entry(mapKeys, dataKey) =>
            val mapKeyBytes = writeKeys(mapKeys, keySerializer)
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.create[Byte](1 + ByteUtil.sizeOf(mapKeyBytes.size) + mapKeyBytes.size + 1 + dataKeyBytes.size)
              .add(formatId)
              .addIntUnsigned(mapKeyBytes.size)
              .addAll(mapKeyBytes)
              .add(Key.entry)
              .addAll(dataKeyBytes)

          case Key.EntriesEnd(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.entriesEnd)

          case Key.SubMapsStart(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.subMapsStart)

          case Key.SubMap(mapKeys, subMapKey) =>
            val mapKeyBytes = writeKeys(mapKeys, keySerializer)
            val dataKeyBytes = keySerializer.write(subMapKey)

            Slice.create[Byte](1 + ByteUtil.sizeOf(mapKeyBytes.size) + mapKeyBytes.size + 1 + dataKeyBytes.size)
              .add(formatId)
              .addIntUnsigned(mapKeyBytes.size)
              .addAll(mapKeyBytes)
              .add(Key.subMap)
              .addAll(dataKeyBytes)

          case Key.SubMapsEnd(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.subMapsEnd)

          case Key.End(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.end)
        }

      override def read(data: Slice[Byte]): Key[K] = {
        val reader = data.createReader()
        reader.skip(1) //skip formatId
        val keyBytes = reader.read(reader.readIntUnsigned())
        val keys = readKeys(keyBytes, keySerializer).get
        val dataType = reader.get()
        if (dataType == Key.start)
          Key.Start(keys)
        else if (dataType == Key.entriesStart)
          Key.EntriesStart(keys)
        else if (dataType == Key.entry)
          Key.Entry(keys, keySerializer.read(reader.readRemaining()))
        else if (dataType == Key.entriesEnd)
          Key.EntriesEnd(keys)
        else if (dataType == Key.subMapsStart)
          Key.SubMapsStart(keys)
        else if (dataType == Key.subMap)
          Key.SubMap(keys, keySerializer.read(reader.readRemaining()))
        else if (dataType == Key.subMapsEnd)
          Key.SubMapsEnd(keys)
        else if (dataType == Key.end)
          Key.End(keys)
        else {
          throw new Exception(s"Invalid dataType: $dataType")
        }
      }
    }

  /**
    * Implements un-typed ordering for performance. This ordering can also be implemented used [[TypedOrdering]].
    * See documentation at http://www.swaydb.io/custom-key-ordering/
    *
    * Creates dual ordering on [[Key.parentMapKeys]]. Orders mapKey using the [[KeyOrder.default]] order
    * and applies custom ordering on the user provided keys.
    */
  def ordering(customOrder: Ordering[Slice[Byte]]) =
    new Ordering[Slice[Byte]] {
      def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
        val readerLeft = a.createReader()
        readerLeft.skip(1) //skip formatId
        val keySizeLeft = readerLeft.readIntUnsigned()
        readerLeft.skip(keySizeLeft)
        val dataTypeLeft = readerLeft.get()

        val readerRight = a.createReader()
        readerRight.skip(1) //skip formatId
        val keySizeRight = readerRight.readIntUnsigned() //read the keySize integer
        readerRight.skip(keySizeRight) //skip key size
        val dataTypeRight = readerRight.get() //read the data type to apply ordering (default or custom)

        //use default sorting if the keys are pointer keys
        if (dataTypeLeft == Key.start || dataTypeLeft == Key.end || dataTypeLeft == Key.subMapsStart || dataTypeLeft == Key.subMapsEnd || dataTypeLeft == Key.entriesStart || dataTypeLeft == Key.entriesEnd ||
          dataTypeRight == Key.start || dataTypeRight == Key.end || dataTypeRight == Key.subMapsStart || dataTypeRight == Key.subMapsEnd || dataTypeRight == Key.entriesStart || dataTypeRight == Key.entriesEnd) {
          KeyOrder.default.compare(a, b)
        } else if (dataTypeLeft == Key.entry || dataTypeLeft == Key.subMap) {
          val tableBytesLeft = a.take(1 + ByteUtil.sizeOf(keySizeLeft) + keySizeLeft + 1)
          val tableBytesRight = b.take(1 + ByteUtil.sizeOf(keySizeRight) + keySizeRight + 1)

          val defaultOrderResult = KeyOrder.default.compare(tableBytesLeft, tableBytesRight)
          if (defaultOrderResult == 0)
            customOrder.compare(a.drop(tableBytesLeft.size), b.drop(tableBytesRight.size))
          else
            defaultOrderResult
        } else {
          throw new Exception(s"Invalid key with prefix byte ${a.head}")
        }
      }
    }
}