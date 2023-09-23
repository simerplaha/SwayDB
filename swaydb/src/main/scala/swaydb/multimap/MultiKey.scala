/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.multimap

import swaydb.core.file.reader.Reader
import swaydb.core.util.Bytes
import swaydb.serializers.Serializer
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder

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
  //leave enough space to allow for adding other data like logSize etc.
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
            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.start)

          case MultiKey.KeysStart(mapId) =>
            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.keysStart)

          case MultiKey.Key(mapId, dataKey) =>
            val dataKeyBytes = keySerializer.write(dataKey)

            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + dataKeyBytes.size + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.key)
              .addAll(dataKeyBytes)

          case MultiKey.KeysEnd(mapId) =>
            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.keysEnd)

          case MultiKey.ChildrenStart(mapId) =>
            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.childrenStart)

          case MultiKey.Child(mapId, subMapKey) =>
            val dataKeyBytes = childKeySerializer.write(subMapKey)

            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1 + dataKeyBytes.size)
              .addUnsignedLong(mapId)
              .add(MultiKey.child)
              .addAll(dataKeyBytes)

          case MultiKey.ChildrenEnd(mapId) =>
            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.childrenEnd)

          case MultiKey.End(mapId) =>
            Slice.of[Byte](Bytes.sizeOfUnsignedLong(mapId) + 1)
              .addUnsignedLong(mapId)
              .add(MultiKey.end)
        }

      override def read(slice: Slice[Byte]): MultiKey[T, K] = {
        val reader = Reader(slice = slice)
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
          throw new Exception(s"Invalid dataType: $dataType")
      }
    }

  /**
   * Implements un-typed ordering for performance. This ordering can also be implemented using types.
   * See documentation at https://swaydb.simer.au/custom-key-ordering/
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
      }
    }
}
