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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb

import swaydb.core.io.reader.Reader
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.collection.mutable.ListBuffer

protected sealed trait MultiMapKey[+T, +K] {
  def parentKey: Iterable[T]
}

protected object MultiMapKey {

  //map start
  case class MapStart[+T](parentKey: Iterable[T]) extends MultiMapKey[T, Nothing]

  case class MapEntriesStart[+T](parentKey: Iterable[T]) extends MultiMapKey[T, Nothing]
  case class MapEntry[+T, +K](parentKey: Iterable[T], dataKey: K) extends MultiMapKey[T, K]
  case class MapEntriesEnd[+T](parentKey: Iterable[T]) extends MultiMapKey[T, Nothing]

  case class SubMapsStart[+T](parentKey: Iterable[T]) extends MultiMapKey[T, Nothing]
  case class SubMap[+T](parentKey: Iterable[T], subMapKey: T) extends MultiMapKey[T, Nothing]
  case class SubMapsEnd[+T](parentKey: Iterable[T]) extends MultiMapKey[T, Nothing]

  case class MapEnd[+T](parentKey: Iterable[T]) extends MultiMapKey[T, Nothing]

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

  protected def writeKeys[T](keys: Iterable[T],
                             keySerializer: Serializer[T]): Slice[Byte] =
    if (keys.isEmpty)
      Slice.emptyBytes
    else {
      val slices = keys map keySerializer.write

      val size = slices.foldLeft(0) {
        case (size, bytes) =>
          size + Bytes.sizeOfUnsignedInt(bytes.size) + bytes.size
      }

      val slice = Slice.create[Byte](size)
      slices foreach {
        keySlice =>
          slice addUnsignedInt keySlice.size
          slice addAll keySlice
      }

      slice
    }

  private def readKeys[T](keys: Slice[Byte], keySerializer: Serializer[T]): Iterable[T] =
    Reader(keys).foldLeft(ListBuffer.empty[T]) {
      case (keys, reader) =>
        val tailKeyBytes = reader.read(reader.readUnsignedInt())
        keys :+ keySerializer.read(tailKeyBytes)
    }

  /**
   * Serializer implementation for [[MultiMapKey]] types.
   *
   * Formats:
   * [[MapStart]] - formatId|mapKey.size|mapKey|dataType
   * [[MapEntry]] - formatId|mapKey.size|mapKey|dataType|dataKey
   * [[MapEnd]]   - formatId|mapKey.size|mapKey|dataType
   *
   * mapKey   - the unique id of the Map.
   * dataType - the type of [[MultiMapKey]] which can be either one of [[mapStart]], [[mapEntry]] or [[mapEnd]]
   * dataKey  - the entry key for the Map.
   */
  implicit def serializer[T, K](implicit keySerializer: Serializer[K],
                                tableSerializer: Serializer[T]): Serializer[MultiMapKey[T, K]] =
    new Serializer[MultiMapKey[T, K]] {
      override def write(data: MultiMapKey[T, K]): Slice[Byte] =
        data match {
          case MultiMapKey.MapStart(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, tableSerializer)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addUnsignedInt(keyBytes.size)
              .addAll(keyBytes)
              .add(MultiMapKey.mapStart)

          case MultiMapKey.MapEntriesStart(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, tableSerializer)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addUnsignedInt(keyBytes.size)
              .addAll(keyBytes)
              .add(MultiMapKey.mapEntriesStart)

          case MultiMapKey.MapEntry(mapKeys, dataKey) =>
            val mapKeyBytes = writeKeys(mapKeys, tableSerializer)
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(mapKeyBytes.size) + mapKeyBytes.size + 1 + dataKeyBytes.size)
              .add(formatId)
              .addUnsignedInt(mapKeyBytes.size)
              .addAll(mapKeyBytes)
              .add(MultiMapKey.mapEntry)
              .addAll(dataKeyBytes)

          case MultiMapKey.MapEntriesEnd(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, tableSerializer)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addUnsignedInt(keyBytes.size)
              .addAll(keyBytes)
              .add(MultiMapKey.mapEntriesEnd)

          case MultiMapKey.SubMapsStart(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, tableSerializer)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addUnsignedInt(keyBytes.size)
              .addAll(keyBytes)
              .add(MultiMapKey.subMapsStart)

          case MultiMapKey.SubMap(mapKeys, subMapKey) =>
            val mapKeyBytes = writeKeys(mapKeys, tableSerializer)
            val dataKeyBytes = tableSerializer.write(subMapKey)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(mapKeyBytes.size) + mapKeyBytes.size + 1 + dataKeyBytes.size)
              .add(formatId)
              .addUnsignedInt(mapKeyBytes.size)
              .addAll(mapKeyBytes)
              .add(MultiMapKey.subMap)
              .addAll(dataKeyBytes)

          case MultiMapKey.SubMapsEnd(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, tableSerializer)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addUnsignedInt(keyBytes.size)
              .addAll(keyBytes)
              .add(MultiMapKey.subMapsEnd)

          case MultiMapKey.MapEnd(mapKeys) =>
            val keyBytes = writeKeys(mapKeys, tableSerializer)

            Slice.create[Byte](1 + Bytes.sizeOfUnsignedInt(keyBytes.size) + keyBytes.size + 1)
              .add(formatId)
              .addUnsignedInt(keyBytes.size)
              .addAll(keyBytes)
              .add(MultiMapKey.mapEnd)
        }

      override def read(data: Slice[Byte]): MultiMapKey[T, K] = {
        val reader = Reader(slice = data, position = 1)
        val keyBytes = reader.read(reader.readUnsignedInt())
        val keys = readKeys(keyBytes, tableSerializer)
        val dataType = reader.get()
        if (dataType == MultiMapKey.mapStart)
          MultiMapKey.MapStart(keys)
        else if (dataType == MultiMapKey.mapEntriesStart)
          MultiMapKey.MapEntriesStart(keys)
        else if (dataType == MultiMapKey.mapEntry)
          MultiMapKey.MapEntry(keys, keySerializer.read(reader.readRemaining()))
        else if (dataType == MultiMapKey.mapEntriesEnd)
          MultiMapKey.MapEntriesEnd(keys)
        else if (dataType == MultiMapKey.subMapsStart)
          MultiMapKey.SubMapsStart(keys)
        else if (dataType == MultiMapKey.subMap)
          MultiMapKey.SubMap(keys, tableSerializer.read(reader.readRemaining()))
        else if (dataType == MultiMapKey.subMapsEnd)
          MultiMapKey.SubMapsEnd(keys)
        else if (dataType == MultiMapKey.mapEnd)
          MultiMapKey.MapEnd(keys)
        else
          throw IO.throwable(s"Invalid dataType: $dataType")
      }
    }

  /**
   * Implements un-typed ordering for performance. This ordering can also be implemented using types.
   * See documentation at http://www.swaydb.io/custom-key-ordering/
   *
   * Creates dual ordering on [[MultiMapKey.parentKey]]. Orders mapKey using the [[KeyOrder.default]] order
   * and applies custom ordering on the user provided keys.
   */
  def ordering(customOrder: KeyOrder[Slice[Byte]]) =
    new KeyOrder[Slice[Byte]] {
      def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
        val readerLeft = Reader(slice = a, position = 1)
        val keySizeLeft = readerLeft.readUnsignedInt()
        readerLeft.skip(keySizeLeft)
        val dataTypeLeft = readerLeft.get()

        val readerRight = Reader(slice = a, position = 1)
        val keySizeRight = readerRight.readUnsignedInt() //read the keySize integer
        readerRight.skip(keySizeRight) //skip key size
        val dataTypeRight = readerRight.get() //read the data type to apply ordering (default or custom)

        //use default sorting if the keys are pointer keys
        if (dataTypeLeft == MultiMapKey.mapStart || dataTypeLeft == MultiMapKey.mapEnd || dataTypeLeft == MultiMapKey.subMapsStart || dataTypeLeft == MultiMapKey.subMapsEnd || dataTypeLeft == MultiMapKey.mapEntriesStart || dataTypeLeft == MultiMapKey.mapEntriesEnd ||
          dataTypeRight == MultiMapKey.mapStart || dataTypeRight == MultiMapKey.mapEnd || dataTypeRight == MultiMapKey.subMapsStart || dataTypeRight == MultiMapKey.subMapsEnd || dataTypeRight == MultiMapKey.mapEntriesStart || dataTypeRight == MultiMapKey.mapEntriesEnd) {
          KeyOrder.default.compare(a, b)
        } else if (dataTypeLeft == MultiMapKey.mapEntry || dataTypeLeft == MultiMapKey.subMap) {
          val tableBytesLeft = a.take(1 + Bytes.sizeOfUnsignedInt(keySizeLeft) + keySizeLeft + 1)
          val tableBytesRight = b.take(1 + Bytes.sizeOfUnsignedInt(keySizeRight) + keySizeRight + 1)

          val defaultOrderResult = KeyOrder.default.compare(tableBytesLeft, tableBytesRight)
          if (defaultOrderResult == 0)
            customOrder.compare(a.drop(tableBytesLeft.size), b.drop(tableBytesRight.size))
          else
            defaultOrderResult
        } else {
          throw IO.throwable(s"Invalid key with prefix byte ${a.head}")
        }
      }
    }
}