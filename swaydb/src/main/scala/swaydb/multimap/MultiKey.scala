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

package swaydb.multimap

import swaydb.IO
import swaydb.core.io.reader.Reader
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

private[swaydb] sealed trait MultiKey[+C, +K] {
  def childId: Long
}

private[swaydb] object MultiKey {

  case class Start(childId: Long) extends MultiKey[Nothing, Nothing]

  case class KeysStart(childId: Long) extends MultiKey[Nothing, Nothing]
  case class Key[+K](childId: Long, key: K) extends MultiKey[Nothing, K]
  case class KeysEnd(childId: Long) extends MultiKey[Nothing, Nothing]

  case class ChildrenStart(childId: Long) extends MultiKey[Nothing, Nothing]
  case class Child[+C](childId: Long, childKey: C) extends MultiKey[C, Nothing]
  case class ChildrenEnd(childId: Long) extends MultiKey[Nothing, Nothing]

  case class End(childId: Long) extends MultiKey[Nothing, Nothing]

  //starting of the Map
  private val start: Byte = 1
  //ids for entries block
  private val keysStart: Byte = 2
  private val key: Byte = 3
  private val keysEnd: Byte = 10
  //ids for subMaps block
  private val childrenStart: Byte = 11
  private val child: Byte = 12
  private val childrenEnd: Byte = 20
  //leave enough space to allow for adding other data like mapSize etc.
  private val end: Byte = 50 //keep this to map so that there is enough space in the map to add more data types.
  //actual queues's data is outside the map

  /**
   * Serializer implementation for [[MultiKey]] types.
   *
   * Formats:
   * [[Start]] - formatId|mapKey.size|mapKey|dataType
   * [[Key]]   - formatId|mapKey.size|mapKey|dataType|dataKey
   * [[End]]   - formatId|mapKey.size|mapKey|dataType
   *
   * mapKey   - the unique id of the Map.
   * dataType - the type of [[MultiKey]] which can be either one of [[start]], [[key]] or [[end]]
   * dataKey  - the entry key for the Map.
   */
  implicit def serializer[T, K](implicit keySerializer: Serializer[K],
                                childKeySerializer: Serializer[T]): Serializer[MultiKey[T, K]] =
    new Serializer[MultiKey[T, K]] {
      override def write(data: MultiKey[T, K]): Slice[Byte] =
        data match {
          case MultiKey.Start(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.start)

          case MultiKey.KeysStart(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.keysStart)

          case MultiKey.Key(mapId, dataKey) =>
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + dataKeyBytes.size + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.key)
              .addAll(dataKeyBytes)

          case MultiKey.KeysEnd(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.keysEnd)

          case MultiKey.ChildrenStart(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.childrenStart)

          case MultiKey.Child(mapId, subMapKey) =>
            val dataKeyBytes = childKeySerializer.write(subMapKey)

            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1 + dataKeyBytes.size)
              .addUnsignedLong(mapId)
              .add(MultiKey.child)
              .addAll(dataKeyBytes)

          case MultiKey.ChildrenEnd(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.childrenEnd)

          case MultiKey.End(mapId) =>
            Slice.create[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.end)
        }

      override def read(data: Slice[Byte]): MultiKey[T, K] = {
        val reader = Reader(slice = data)
        val mapId = reader.readUnsignedLong()
        val dataType = reader.get()
        if (dataType == MultiKey.start)
          MultiKey.Start(mapId)
        else if (dataType == MultiKey.keysStart)
          MultiKey.KeysStart(mapId)
        else if (dataType == MultiKey.key)
          MultiKey.Key(mapId, keySerializer.read(reader.readRemaining()))
        else if (dataType == MultiKey.keysEnd)
          MultiKey.KeysEnd(mapId)
        else if (dataType == MultiKey.childrenStart)
          MultiKey.ChildrenStart(mapId)
        else if (dataType == MultiKey.child)
          MultiKey.Child(
            childId = mapId,
            childKey = childKeySerializer.read(reader.readRemaining())
          )
        else if (dataType == MultiKey.childrenEnd)
          MultiKey.ChildrenEnd(mapId)
        else if (dataType == MultiKey.end)
          MultiKey.End(mapId)
        else
          throw IO.throwable(s"Invalid dataType: $dataType")
      }
    }

  /**
   * Implements un-typed ordering for performance. This ordering can also be implemented using types.
   * See documentation at http://www.swaydb.io/custom-key-ordering/
   *
   * Creates dual ordering on [[MultiKey.childId]]. Orders mapKey using the [[KeyOrder.default]] order
   * and applies custom ordering on the user provided keys.
   */
  def ordering(customOrder: KeyOrder[Slice[Byte]]) =
    new KeyOrder[Slice[Byte]] {
      override def compare(left: Slice[Byte], right: Slice[Byte]): Int = {
        val letMapIdByteSize = left.readUnsignedLongByteSize()
        val leftType = left.get(letMapIdByteSize)

        val rightMapIdByteSize = right.readUnsignedLongByteSize()
        val rightType = right.get(rightMapIdByteSize)

        if (leftType != MultiKey.key && leftType != MultiKey.child && rightType != MultiKey.key && rightType != MultiKey.child) {
          KeyOrder.default.compare(left, right)
        } else {
          val defaultOrderResult =
            KeyOrder.defaultCompare(
              a = left,
              b = right,
              maxBytes = Math.min(letMapIdByteSize, rightMapIdByteSize) + 1
            )

          if (defaultOrderResult == 0) {
            val aTail = left.drop(letMapIdByteSize + 1)
            val bTail = right.drop(rightMapIdByteSize + 1)
            customOrder.compare(aTail, bTail)
          } else {
            defaultOrderResult
          }
        }

        //use default sorting if the keys are pointer keys
        //        if (leftDataType == MultiKey.start || leftDataType == MultiKey.end || leftDataType == MultiKey.childrenStart || leftDataType == MultiKey.childrenEnd || leftDataType == MultiKey.entriesStart || leftDataType == MultiKey.entriesEnd ||
        //          rightDataType == MultiKey.start || rightDataType == MultiKey.end || rightDataType == MultiKey.childrenStart || rightDataType == MultiKey.childrenEnd || rightDataType == MultiKey.entriesStart || rightDataType == MultiKey.entriesEnd) {
        //          KeyOrder.default.compare(a, b)
        //        } else if (leftDataType == MultiKey.entry || leftDataType == MultiKey.child) {
        //          val tableBytesLeft = a.take(Bytes.sizeOfUnsignedLong(leftMapId) + 1)
        //          val tableBytesRight = b.take(Bytes.sizeOfUnsignedLong(rightMapId) + 1)
        //
        //          val defaultOrderResult = KeyOrder.default.compare(tableBytesLeft, tableBytesRight)
        //          if (defaultOrderResult == 0) {
        //            val aTail = a.drop(tableBytesLeft.size)
        //            val bTail = b.drop(tableBytesRight.size)
        //            customOrder.compare(aTail, bTail)
        //          } else {
        //            defaultOrderResult
        //          }
        //        } else {
        //          throw IO.throwable(s"Invalid key with prefix byte ${a.head}")
        //        }
      }
    }
}
