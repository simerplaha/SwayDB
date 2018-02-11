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

import java.util

import swaydb.core.map.MapEntry.{Add, Remove}
import swaydb.core.map.serializer.{MapCodec, MapSerializer}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

/**
  * [[MapEntry]]s can be batched via ++ function.
  *
  * Batched MapEntries are checksum and persisted as one batch operation.
  *
  * Also requires an implementation [[MapSerializer]] which
  * is used to write bytes to disk for the input Key and Value.
  *
  * @tparam K Key type
  * @tparam V Value type
  */
private[swaydb] sealed trait MapEntry[K, V] { thisEntry =>
  def applyTo(map: java.util.Map[K, V]): Unit

  /**
    * Each map entry computes the bytes required for the entry on creation.
    * The total of all entries are added to compute the file size of the Byte array to be persisted.
    *
    * This ensures that only single iteration will be required to create the final Byte array.
    */
  val entryBytesSize: Int

  def totalByteSize: Int =
    entryBytesSize + MapCodec.headerSize

  def writeTo(slice: Slice[Byte]): Unit

  def applyTo(slice: Slice[Byte], map: java.util.Map[K, V]): Unit

  val entries = ListBuffer[MapEntry[K, V]](this)

  def asString(keyParser: K => String, valueParser: V => String): String = {
    this match {
      case Add(key, value) =>
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

  def +(key: K, value: V)(implicit mapFormatter: MapSerializer[K, V]): MapEntry[K, V] =
    ++(MapEntry.Add(key, value))

  def -(key: K)(implicit mapFormatter: MapSerializer[K, V]): MapEntry[K, V] =
    ++(MapEntry.Remove(key))

  def ++(nextEntry: MapEntry[K, V]): MapEntry[K, V] = {
    new MapEntry[K, V] {
      override val entries = thisEntry.entries
      entries ++= nextEntry.entries

      def applyTo(slice: Slice[Byte], map: java.util.Map[K, V]): Unit = {
        entries foreach {
          entry =>
            entry.writeTo(slice)
            entry.applyTo(map)
        }
      }

      override def applyTo(map: java.util.Map[K, V]): Unit =
        entries foreach (_.applyTo(map))

      override def writeTo(slice: Slice[Byte]): Unit =
        entries foreach (_.writeTo(slice))

      override def asString(keyParser: K => String, valueParser: V => String): String =
        s"""${thisEntry.asString(keyParser, valueParser)}${nextEntry.asString(keyParser, valueParser)}"""

      override val entryBytesSize =
        thisEntry.entryBytesSize + nextEntry.entryBytesSize
    }
  }

}

private[swaydb] object MapEntry {
  def apply[K, V](key: K, value: V)(implicit serializer: MapSerializer[K, V]): MapEntry[K, V] =
    MapEntry.Add(key, value)

  case class Add[K, V](key: K,
                       value: V)(implicit serializer: MapSerializer[K, V]) extends MapEntry[K, V] {
    override def applyTo(map: java.util.Map[K, V]): Unit =
      map.put(key, value)

    override val entryBytesSize: Int =
      serializer bytesRequiredFor this

    override def writeTo(slice: Slice[Byte]): Unit =
      serializer.writeTo(slice, this)

    override def applyTo(slice: Slice[Byte], map: util.Map[K, V]): Unit = {
      serializer.writeTo(slice, this)
      map.put(key, value)
    }
  }

  case class Remove[K, V](key: K)(implicit serializer: MapSerializer[K, V]) extends MapEntry[K, V] {
    override def applyTo(map: java.util.Map[K, V]): Unit =
      map remove key

    override val entryBytesSize: Int =
      serializer bytesRequiredFor this

    override def writeTo(slice: Slice[Byte]): Unit =
      serializer.writeTo(slice, this)

    override def applyTo(slice: Slice[Byte], map: util.Map[K, V]): Unit = {
      serializer.writeTo(slice, this)
      map.remove(key)
    }
  }
}