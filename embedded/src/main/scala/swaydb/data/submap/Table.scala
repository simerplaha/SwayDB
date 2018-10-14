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

package swaydb.data.submap

import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil
import swaydb.order.KeyOrder
import swaydb.serializers.Serializer

private[swaydb] sealed trait Table[K] {
  val tableKey: K
}

private[swaydb] object Table {
  case class Start[K](tableKey: K) extends Table[K]
  case class Row[K](tableKey: K, dataKey: K) extends Table[K]
  case class End[K](tableKey: K) extends Table[K]

  private val start = 1.toByte
  private val row = 2.toByte
  private val end = 50.toByte

  def tableKeySerializer[K](keySerializer: Serializer[K]): Serializer[Table[K]] =
    new Serializer[Table[K]] {
      override def write(data: Table[K]): Slice[Byte] =
        data match {
          case Table.Start(tableKey) =>
            val keyBytes = keySerializer.write(tableKey)

            Slice.create[Byte](ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Table.start)

          case Table.Row(tableKey, dataKey) =>
            val tableKeyBytes = keySerializer.write(tableKey)
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.create[Byte](ByteUtil.sizeOf(tableKeyBytes.size) + tableKeyBytes.size + 1 + dataKeyBytes.size)
              .addIntUnsigned(tableKeyBytes.size)
              .addAll(tableKeyBytes)
              .add(Table.row)
              .addAll(dataKeyBytes)

          case Table.End(tableKey) =>
            val keyBytes = keySerializer.write(tableKey)

            Slice.create[Byte](ByteUtil.sizeOf(keyBytes.size) + keyBytes.size + 1)
              .addIntUnsigned(keyBytes.size)
              .addAll(keyBytes)
              .add(Table.end)
        }

      override def read(data: Slice[Byte]): Table[K] = {
        val reader = data.createReader()
        val keyBytes = reader.read(reader.readIntUnsigned())
        val key = keySerializer.read(keyBytes)
        val dataType = reader.get()
        if (dataType == Table.start)
          Table.Start(key)
        else if (dataType == Table.row)
          Table.Row(key, keySerializer.read(reader.readRemaining()))
        else if (dataType == Table.end)
          Table.End(key)
        else {
          throw new Exception(s"Invalid dataType: $dataType")
        }
      }
    }

  def ordering(customOrder: Ordering[Slice[Byte]]) = new Ordering[Slice[Byte]] {
    def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
      val readerLeft = a.createReader()
      val keySizeLeft = readerLeft.readIntUnsigned()
      readerLeft.skip(keySizeLeft)
      val dataTypeLeft = readerLeft.get()

      val readerRight = a.createReader()
      val keySizeRight = readerRight.readIntUnsigned()
      readerRight.skip(keySizeRight)
      val dataTypeRight = readerRight.get()

      if (dataTypeLeft == Table.start || dataTypeLeft == Table.end ||
        dataTypeRight == Table.start || dataTypeRight == Table.end) {
        KeyOrder.default.compare(a, b)
      } else if (dataTypeLeft == row) {
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
