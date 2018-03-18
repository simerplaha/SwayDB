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

package swaydb.core.map

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.map.MapEntry.{Put, Remove}
import swaydb.core.map.serializer.{MapCodec, MapEntryWriter}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

/**
  * [[MapEntry]]s can be batched via ++ function.
  *
  * Batched MapEntries are checksum and persisted as one batch operation.
  *
  * @tparam K Key type
  * @tparam V Value type
  */

private[swaydb] sealed trait MapEntry[K, +V] { thisEntry =>

  def applyTo[T >: V](map: ConcurrentSkipListMap[K, T]): Unit

  val hasRange: Boolean

  /**
    * Each map entry computes the bytes required for the entry on creation.
    * The total of all _entries are added to compute the file size of the Byte array to be persisted.
    *
    * This ensures that only single iteration will be required to create the final Byte array.
    */
  val entryBytesSize: Int

  def totalByteSize: Int =
    entryBytesSize + MapCodec.headerSize

  def writeTo(slice: Slice[Byte]): Unit

  protected val _entries = ListBuffer[MapEntry[K, _]](this)

  def asString(keyParser: K => String, valueParser: V => String): String = {
    this match {
      case Put(key, value) =>
        s"""
           |Type         : Add
           |key          : ${keyParser(key)}
           |Value        : ${valueParser(value)}
       """.stripMargin

      case Remove(key) =>
        s"""
           |Type          : Remove
           |key           : ${keyParser(key)}
       """.stripMargin
    }
  }
}

private[swaydb] object MapEntry {

  implicit class MapEntriesBatch[K, V](left: MapEntry[K, V]) {
    def ++(right: MapEntry[K, V]): MapEntry[K, V] =
      new MapEntry[K, V] {

        override protected val _entries = left._entries ++= right._entries

        override def writeTo(slice: Slice[Byte]): Unit =
          _entries foreach (_.writeTo(slice))

        override def asString(keyParser: K => String, valueParser: V => String): String =
          s"""${left.asString(keyParser, valueParser)}${right.asString(keyParser, valueParser)}"""

        override val entryBytesSize =
          left.entryBytesSize + right.entryBytesSize

        override def applyTo[T >: V](map: ConcurrentSkipListMap[K, T]): Unit =
          _entries.asInstanceOf[ListBuffer[MapEntry[K, V]]] foreach (_.applyTo(map))

        override val hasRange: Boolean =
          left.hasRange || right.hasRange
      }

    def entries: ListBuffer[MapEntry[K, V]] =
      left._entries.asInstanceOf[ListBuffer[MapEntry[K, V]]]

  }

  case class Put[K, V](key: K,
                       value: V)(implicit serializer: MapEntryWriter[MapEntry.Put[K, V]]) extends MapEntry[K, V] {

    val hasRange: Boolean = serializer.isRange

    override val entryBytesSize: Int =
      serializer bytesRequired this

    override def writeTo(slice: Slice[Byte]): Unit =
      serializer.write(this, slice)

    override def applyTo[T >: V](map: ConcurrentSkipListMap[K, T]): Unit =
      map.put(key, value)

  }

  case class Remove[K](key: K)(implicit serializer: MapEntryWriter[MapEntry.Remove[K]]) extends MapEntry[K, Nothing] {

    val hasRange: Boolean = serializer.isRange

    override val entryBytesSize: Int =
      serializer bytesRequired this

    override def writeTo(slice: Slice[Byte]): Unit =
      serializer.write(this, slice)

    override def applyTo[T >: Nothing](map: ConcurrentSkipListMap[K, T]): Unit =
      map.remove(key)
  }

}