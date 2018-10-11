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

package swaydb.table

import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil
import swaydb.order.KeyOrder

private[swaydb] case class Table(start: Slice[Byte],
                                 row: Slice[Byte],
                                 subTable: Slice[Byte],
                                 end: Slice[Byte])

private[swaydb] object Table {

  private val tableStart = 1.toByte
  private val tableRow = 2.toByte
  private val subTable = 3.toByte
  private val tableEnd = 50.toByte

  def ordering(customOrder: Ordering[Slice[Byte]]) = new Ordering[Slice[Byte]] {
    def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
      val readerLeft = a.createReader()
      val keySizeLeft = readerLeft.readIntUnsigned()
      readerLeft.skip(keySizeLeft)
      val entryTypeLeft = readerLeft.get()

      val readerRight = a.createReader()
      val keySizeRight = readerRight.readIntUnsigned()
      readerRight.skip(keySizeRight)
      val entryTypeRight = readerRight.get()

      if (entryTypeLeft == tableStart || entryTypeLeft == tableEnd || entryTypeLeft == subTable ||
        entryTypeRight == tableStart || entryTypeRight == tableEnd || entryTypeRight == subTable) {
        KeyOrder.default.compare(a, b)
      } else if (entryTypeLeft == tableRow) {
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

  def buildTable(tableId: Slice[Byte]): Table = {
    val tableIdSize = ByteUtil.sizeOf(tableId.size)
    val tableEntrySize = tableIdSize + tableId.size + 1
    val start =
      Slice.create[Byte](tableEntrySize)
        .addIntUnsigned(tableId.size)
        .addAll(tableId)
        .add(tableStart)

    val row =
      Slice.create[Byte](tableEntrySize)
        .addIntUnsigned(tableId.size)
        .addAll(tableId)
        .add(tableRow)

    val sub =
      Slice.create[Byte](tableEntrySize)
        .addIntUnsigned(tableId.size)
        .addAll(tableId)
        .add(subTable)

    val end =
      Slice.create[Byte](tableEntrySize)
        .addIntUnsigned(tableId.size)
        .addAll(tableId)
        .add(tableEnd)

    Table(start = start, row = row, subTable = sub, end = end)
  }

  def buildSubTableRow(subTableId: Slice[Byte], parent: Table): Slice[Byte] =
    Slice.create[Byte](parent.subTable.size + subTableId.size)
      .addAll(parent.subTable)
      .addAll(subTableId)

  def buildRowKey(row: Slice[Byte], parent: Table): Slice[Byte] =
    Slice.create[Byte](parent.row.size + row.size)
      .addAll(parent.row)
      .addAll(row)

  def dropTableBytes(key: Slice[Byte]): Slice[Byte] = {
    val reader = key.createReader()
    reader.skip(reader.readIntUnsigned() + 1) //also skip the last tableIndicator byte.
    reader.readRemaining()
  }
}
