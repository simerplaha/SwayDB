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

package swaydb.data.map

import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil
import swaydb.order.KeyOrder
import swaydb.serializers.Serializer

private[swaydb] sealed trait MapKey[K] {
  val mapKey: K
}

private[swaydb] object MapKey {

  case class Start[K](mapKey: K) extends MapKey[K]
  case class Entry[K](mapKey: K, dataKey: K) extends MapKey[K]
  case class End[K](mapKey: K) extends MapKey[K]

  private val start: Byte = "-128".toByte
  private val entry: Byte = "-127".toByte
  //leave enough space to allow for adding other data like mapSize etc.
  private val end: Byte = "-100".toByte

  /**
    * Serializer implementation for [[MapKey]] types.
    *
    * Formats:
    * [[Start]] - mapKey.size|mapKey|dataType
    * [[Entry]] - mapKey.size|mapKey|dataType|dataKey
    * [[End]]   - mapKey.size|mapKey|dataType
    *
    * mapKey   - the unique id of the Map.
    * dataType - the type of [[MapKey]] which can be either one of [[start]], [[entry]] or [[end]]
    * dataKey  - the entry key for the Map.
    */
  def mapKeySerializer[K](keySerializer: Serializer[K]): Serializer[MapKey[K]] =
    new Serializer[MapKey[K]] {
      override def write(data: MapKey[K]): Slice[Byte] =
        data match {
          case MapKey.Start(mapKey) =>
            val keyBytes = keySerializer.write(mapKey)

            Slice.create[Byte](ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(MapKey.start)

          case MapKey.Entry(mapKey, dataKey) =>
            val mapKeyBytes = keySerializer.write(mapKey)
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.create[Byte](ByteUtil.sizeOf(mapKeyBytes.size) + mapKeyBytes.size + 1 + dataKeyBytes.size)
              .addIntUnsigned(mapKeyBytes.size)
              .addAll(mapKeyBytes)
              .add(MapKey.entry)
              .addAll(dataKeyBytes)

          case MapKey.End(mapKey) =>
            val keyBytes = keySerializer.write(mapKey)

            Slice.create[Byte](ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(MapKey.end)
        }

      override def read(data: Slice[Byte]): MapKey[K] = {
        val reader = data.createReader()
        val keyBytes = reader.read(reader.readIntUnsigned())
        val key = keySerializer.read(keyBytes)
        val dataType = reader.get()
        if (dataType == MapKey.start)
          MapKey.Start(key)
        else if (dataType == MapKey.entry)
          MapKey.Entry(key, keySerializer.read(reader.readRemaining()))
        else if (dataType == MapKey.end)
          MapKey.End(key)
        else {
          throw new Exception(s"Invalid dataType: $dataType")
        }
      }
    }

  /**
    * Creates dual ordering on [[MapKey.mapKey]]. Orders mapKey using the [[KeyOrder.default]] order
    * and applies custom ordering to the user provided keys.
    */
  def ordering(customOrder: Ordering[Slice[Byte]]) =
    new Ordering[Slice[Byte]] {
      def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
        val readerLeft = a.createReader()
        val keySizeLeft = readerLeft.readIntUnsigned()
        readerLeft.skip(keySizeLeft)
        val dataTypeLeft = readerLeft.get()

        val readerRight = a.createReader()
        val keySizeRight = readerRight.readIntUnsigned()
        readerRight.skip(keySizeRight)
        val dataTypeRight = readerRight.get()

        if (dataTypeLeft == MapKey.start || dataTypeLeft == MapKey.end ||
          dataTypeRight == MapKey.start || dataTypeRight == MapKey.end) {
          KeyOrder.default.compare(a, b)
        } else if (dataTypeLeft == entry) {
          val tableBytesLeft = a.take(1 + keySizeLeft + 1)
          val tableBytesRight = b.take(1 + keySizeRight + 1)

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
