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

protected sealed trait MultiMapKey[+M, +K] {
  def mapId: Long
}

protected object MultiMapKey {

  //map start
  case class MapStart(mapId: Long) extends MultiMapKey[Nothing, Nothing]

  case class MapEntriesStart(mapId: Long) extends MultiMapKey[Nothing, Nothing]
  case class MapEntry[+K](mapId: Long, dataKey: K) extends MultiMapKey[Nothing, K]
  case class MapEntriesEnd(mapId: Long) extends MultiMapKey[Nothing, Nothing]

  case class SubMapsStart(mapId: Long) extends MultiMapKey[Nothing, Nothing]
  case class SubMap[+M](mapId: Long, subMapKey: M, subMapId: Long) extends MultiMapKey[M, Nothing]
  case class SubMapsEnd(mapId: Long) extends MultiMapKey[Nothing, Nothing]

  case class MapEnd(mapId: Long) extends MultiMapKey[Nothing, Nothing]

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
          case MultiMapKey.MapStart(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.mapStart)

          case MultiMapKey.MapEntriesStart(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.mapEntriesStart)

          case MultiMapKey.MapEntry(mapId, dataKey) =>
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + dataKeyBytes.size + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.mapEntry)
              .addAll(dataKeyBytes)

          case MultiMapKey.MapEntriesEnd(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.mapEntriesEnd)

          case MultiMapKey.SubMapsStart(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.subMapsStart)

          case MultiMapKey.SubMap(mapId, subMapKey, subMapId) =>
            val dataKeyBytes = tableSerializer.write(subMapKey)

            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + Bytes.sizeOfUnsignedInt(dataKeyBytes.size) + dataKeyBytes.size + Bytes.sizeOfUnsignedLong(subMapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.subMap)
              .addUnsignedInt(dataKeyBytes.size)
              .addAll(dataKeyBytes)
              .addUnsignedLong(subMapId)

          case MultiMapKey.SubMapsEnd(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.subMapsEnd)

          case MultiMapKey.MapEnd(mapId) =>

            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiMapKey.mapEnd)
        }

      override def read(data: Slice[Byte]): MultiMapKey[T, K] = {
        val reader = Reader(slice = data)
        val mapId = reader.readUnsignedLong()
        val dataType = reader.get()
        if (dataType == MultiMapKey.mapStart)
          MultiMapKey.MapStart(mapId)
        else if (dataType == MultiMapKey.mapEntriesStart)
          MultiMapKey.MapEntriesStart(mapId)
        else if (dataType == MultiMapKey.mapEntry)
          MultiMapKey.MapEntry(mapId, keySerializer.read(reader.readRemaining()))
        else if (dataType == MultiMapKey.mapEntriesEnd)
          MultiMapKey.MapEntriesEnd(mapId)
        else if (dataType == MultiMapKey.subMapsStart)
          MultiMapKey.SubMapsStart(mapId)
        else if (dataType == MultiMapKey.subMap)
          MultiMapKey.SubMap(
            mapId = mapId,
            subMapKey = tableSerializer.read(reader.read(reader.readUnsignedInt())),
            subMapId = reader.readUnsignedLong()
          )
        else if (dataType == MultiMapKey.subMapsEnd)
          MultiMapKey.SubMapsEnd(mapId)
        else if (dataType == MultiMapKey.mapEnd)
          MultiMapKey.MapEnd(mapId)
        else
          throw IO.throwable(s"Invalid dataType: $dataType")
      }
    }

  /**
   * Implements un-typed ordering for performance. This ordering can also be implemented using types.
   * See documentation at http://www.swaydb.io/custom-key-ordering/
   *
   * Creates dual ordering on [[MultiMapKey.mapId]]. Orders mapKey using the [[KeyOrder.default]] order
   * and applies custom ordering on the user provided keys.
   */
  def ordering(customOrder: KeyOrder[Slice[Byte]]) =
    new KeyOrder[Slice[Byte]] {
      override def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
        val leftMapId = a.readUnsignedLong()
        val leftDataType = a.drop(Bytes.sizeOfUnsignedLong(leftMapId)).head

        val rightMapId = b.readUnsignedLong()
        val rightDataType = b.drop(Bytes.sizeOfUnsignedLong(rightMapId)).head

        //use default sorting if the keys are pointer keys
        if (leftDataType == MultiMapKey.mapStart || leftDataType == MultiMapKey.mapEnd || leftDataType == MultiMapKey.subMapsStart || leftDataType == MultiMapKey.subMapsEnd || leftDataType == MultiMapKey.mapEntriesStart || leftDataType == MultiMapKey.mapEntriesEnd ||
          rightDataType == MultiMapKey.mapStart || rightDataType == MultiMapKey.mapEnd || rightDataType == MultiMapKey.subMapsStart || rightDataType == MultiMapKey.subMapsEnd || rightDataType == MultiMapKey.mapEntriesStart || rightDataType == MultiMapKey.mapEntriesEnd) {
          KeyOrder.default.compare(a, b)
        } else if (leftDataType == MultiMapKey.mapEntry || leftDataType == MultiMapKey.subMap) {
          val tableBytesLeft = a.take(Bytes.sizeOfUnsignedLong(leftMapId) + 1)
          val tableBytesRight = b.take(Bytes.sizeOfUnsignedLong(rightMapId) + 1)

          val defaultOrderResult = KeyOrder.default.compare(tableBytesLeft, tableBytesRight)
          if (defaultOrderResult == 0) {
            val aToCompare =
              if (leftDataType == MultiMapKey.subMap)
                ???
              else
                a.drop(tableBytesLeft.size)

            val bToCompare =
              if (rightDataType == MultiMapKey.subMap)
                b.drop(tableBytesRight.size)
              else
                b.drop(tableBytesRight.size)

            customOrder.compare(a.drop(tableBytesLeft.size), b.drop(tableBytesRight.size))
          } else {
            defaultOrderResult
          }
        } else {
          throw IO.throwable(s"Invalid key with prefix byte ${a.head}")
        }
      }

      override def comparableKey(key: Slice[Byte]): Slice[Byte] = {
        val reader = key.createReader()
        val mapId = reader.readUnsignedLong()
        if (reader.get() == MultiMapKey.subMap) {
          val size = key.readUnsignedInt() //size of tableKey
          key.take(Bytes.sizeOfUnsignedLong(mapId) + Bytes.sizeOfUnsignedInt(size) + size)
        } else {
          key
        }
      }
    }
}
