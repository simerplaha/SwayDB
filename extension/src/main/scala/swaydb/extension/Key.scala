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

import swaydb.KeyOrderTyped
import swaydb.core.io.reader.Reader
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteUtil
import swaydb.data.order.KeyOrder
import swaydb.serializers.Serializer

import scala.util.Try

sealed trait Key[+K] {
  def parentMapKeys: Seq[K]
}
object Key {

  //map start
  case class MapStart[K](parentMapKeys: Seq[K]) extends Key[K]

  case class MapEntriesStart[K](parentMapKeys: Seq[K]) extends Key[K]
  case class MapEntry[K](parentMapKeys: Seq[K], dataKey: K) extends Key[K]
  case class MapEntriesEnd[K](parentMapKeys: Seq[K]) extends Key[K]

  case class SubMapsStart[K](parentMapKeys: Seq[K]) extends Key[K]
  case class SubMap[K](parentMapKeys: Seq[K], subMapKey: K) extends Key[K]
  case class SubMapsEnd[K](parentMapKeys: Seq[K]) extends Key[K]

  case class MapEnd[K](parentMapKeys: Seq[K]) extends Key[K]

  //formatId for the serializers. Each key is prepended with this formatId.
  private val formatId: Byte = 0

  //starting of the Map
  private val mapStart: Byte = 1
  //ids for entries block
  private val mapEntriesStart: Byte = 2
  private val mapEntry: Byte = 3
  private val mapEntriesEnd: Byte = 10
  //ids for subMaps block
  private val subMapsStart: Byte = 11
  private val subMap: Byte = 12
  private val subMapsEnd: Byte = 20
  //leave enough space to allow for adding other data like mapSize etc.
  private val mapEnd: Byte = 127 //keep this to map so that there is enough space in the map to add more data types.
  //actual queues's data is outside the map

  private[swaydb] def writeKeys[K](keys: Seq[K],
                                   keySerializer: Serializer[K]): Slice[Byte] =
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
    * [[MapStart]] - formatId|mapKey.size|mapKey|dataType
    * [[MapEntry]] - formatId|mapKey.size|mapKey|dataType|dataKey
    * [[MapEnd]]   - formatId|mapKey.size|mapKey|dataType
    *
    * mapKey   - the unique id of the Map.
    * dataType - the type of [[Key]] which can be either one of [[mapStart]], [[mapEntry]] or [[mapEnd]]
    * dataKey  - the entry key for the Map.
    */
  implicit def serializer[K](implicit keySerializer: Serializer[K]): Serializer[Key[K]] =
    new Serializer[Key[K]] {
      override def write(data: Key[K]): Slice[Byte] =
        data match {
          case Key.MapStart(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.mapStart)

          case Key.MapEntriesStart(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.mapEntriesStart)

          case Key.MapEntry(mapKeys, dataKey) =>
            val mapKeyBytes = writeKeys(mapKeys, keySerializer)
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.create[Byte](1 + ByteUtil.sizeOf(mapKeyBytes.size) + mapKeyBytes.size + 1 + dataKeyBytes.size)
              .add(formatId)
              .addIntUnsigned(mapKeyBytes.size)
              .addAll(mapKeyBytes)
              .add(Key.mapEntry)
              .addAll(dataKeyBytes)

          case Key.MapEntriesEnd(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.mapEntriesEnd)

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

          case Key.MapEnd(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, keySerializer)

            Slice.create[Byte](1 + ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Key.mapEnd)
        }

      override def read(data: Slice[Byte]): Key[K] = {
        val reader = data.createReader()
        reader.skip(1) //skip formatId
        val keyBytes = reader.read(reader.readIntUnsigned())
        val keys = readKeys(keyBytes, keySerializer).get
        val dataType = reader.get()
        if (dataType == Key.mapStart)
          Key.MapStart(keys)
        else if (dataType == Key.mapEntriesStart)
          Key.MapEntriesStart(keys)
        else if (dataType == Key.mapEntry)
          Key.MapEntry(keys, keySerializer.read(reader.readRemaining()))
        else if (dataType == Key.mapEntriesEnd)
          Key.MapEntriesEnd(keys)
        else if (dataType == Key.subMapsStart)
          Key.SubMapsStart(keys)
        else if (dataType == Key.subMap)
          Key.SubMap(keys, keySerializer.read(reader.readRemaining()))
        else if (dataType == Key.subMapsEnd)
          Key.SubMapsEnd(keys)
        else if (dataType == Key.mapEnd)
          Key.MapEnd(keys)
        else {
          throw new Exception(s"Invalid dataType: $dataType")
        }
      }
    }

  /**
    * Implements un-typed ordering for performance. This ordering can also be implemented used [[KeyOrderTyped]].
    * See documentation at http://www.swaydb.io/custom-key-ordering/
    *
    * Creates dual ordering on [[Key.parentMapKeys]]. Orders mapKey using the [[KeyOrder.default]] order
    * and applies custom ordering on the user provided keys.
    */
  def ordering(customOrder: KeyOrder[Slice[Byte]]) =
    new KeyOrder[Slice[Byte]] {
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
        if (dataTypeLeft == Key.mapStart || dataTypeLeft == Key.mapEnd || dataTypeLeft == Key.subMapsStart || dataTypeLeft == Key.subMapsEnd || dataTypeLeft == Key.mapEntriesStart || dataTypeLeft == Key.mapEntriesEnd ||
          dataTypeRight == Key.mapStart || dataTypeRight == Key.mapEnd || dataTypeRight == Key.subMapsStart || dataTypeRight == Key.subMapsEnd || dataTypeRight == Key.mapEntriesStart || dataTypeRight == Key.mapEntriesEnd) {
          KeyOrder.default.compare(a, b)
        } else if (dataTypeLeft == Key.mapEntry || dataTypeLeft == Key.subMap) {
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
