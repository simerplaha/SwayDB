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

package swaydb.nest.order

import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil
import swaydb.order.KeyOrder

private[swaydb] object NestOrder {

  val mapStart = 1.toByte
  val mapElement = 2.toByte
  val mapEnd = 10.toByte
  val other = 0.toByte

  private def getMapIdBytes(mapId: Slice[Byte]) = {
    val mapKeySize = mapId.drop(1).readIntUnsigned().get
    mapId.take(1 + ByteUtil.sizeOf(mapKeySize) + mapKeySize)
  }

  def ordering(customOrder: Ordering[Slice[Byte]]) = new Ordering[Slice[Byte]] {
    def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
      if (a.head == other) {
        customOrder.compare(a, b)
      } else if (a.head == mapStart || a.head == mapElement || a.head == mapEnd) {
        val leftPrefixKey = getMapIdBytes(a)
        val rightPrefixKey = getMapIdBytes(b)

        val result = KeyOrder.default.compare(leftPrefixKey, rightPrefixKey)
        if (result == 0)
          customOrder.compare(a.drop(leftPrefixKey.size), b.drop(rightPrefixKey.size))
        else
          result
      } else {
        throw new Exception(s"Invalid key with prefix byte ${a.head}")
      }
    }
  }

  def start(mapId: Slice[Byte]): Slice[Byte] =
    Slice.create[Byte](1 + ByteUtil.sizeOf(mapId.size) + mapId.size)
      .add(mapStart)
      .addIntUnsigned(mapId.size)
      .addAll(mapId)

  def element(mapId: Slice[Byte], key: Slice[Byte]): Slice[Byte] =
    Slice.create[Byte](1 + ByteUtil.sizeOf(key.size) + mapId.size + 1 + key.size)
      .add(mapStart)
      .addIntUnsigned(mapId.size)
      .addAll(mapId)
      .add(mapElement)
      .addAll(key)

  def end(mapId: Slice[Byte]): Slice[Byte] =
    Slice.create[Byte](1 + ByteUtil.sizeOf(mapId.size) + mapId.size)
      .add(mapEnd)
      .addIntUnsigned(mapId.size)
      .addAll(mapId)

  def open(key: Slice[Byte]): Slice[Byte] =
    Slice.create[Byte](1 + key.size)
      .add(other)
      .addAll(key)

  def buildNestId(mapId: Option[Slice[Byte]], key: Slice[Byte]): Slice[Byte] =
    mapId map {
      mapId =>
        element(mapId, key)
    } getOrElse {
      open(key)
    }

}
