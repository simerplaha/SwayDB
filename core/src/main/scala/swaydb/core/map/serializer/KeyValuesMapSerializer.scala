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

package swaydb.core.map.serializer

import swaydb.core.data.ValueType
import swaydb.core.map.MapEntry
import swaydb.core.map.MapEntry.{Add, Remove}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.util.Try

private[core] object KeyValuesMapSerializer {

  def apply(implicit ordering: Ordering[Slice[Byte]]) =
    new KeyValuesMapSerializer()

}

private[core] class KeyValuesMapSerializer(implicit ordering: Ordering[Slice[Byte]]) extends MapSerializer[Slice[Byte], (ValueType, Option[Slice[Byte]])] {

  implicit val readerWriter = this

  override def read(reader: Reader): Try[Option[MapEntry[Slice[Byte], (ValueType, Option[Slice[Byte]])]]] =
    reader.foldLeftTry(Option.empty[MapEntry[Slice[Byte], (ValueType, Option[Slice[Byte]])]]) {
      case (previousEntry, reader) =>
        reader.get() flatMap {
          entryType =>
            if (entryType == 1) {
              for {
                keyLength <- reader.readInt()
                key <- reader.read(keyLength).map(_.unslice())
                valueType <- reader.readInt().map(ValueType(_))
                valueLength <- reader.readInt()
              } yield {
                val value =
                  if (valueLength == 0)
                    None
                  else
                    Some(reader.read(valueLength).get.unslice())
                val nextEntry = MapEntry.Add(key, (valueType, value))
                previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
              }
            } else {
              for {
                keyLength <- reader.readInt()
                key <- reader.read(keyLength).map(_.unslice())
              } yield {
                val nextEntry = MapEntry.Remove(key)
                previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
              }
            }
        }
    }

  override def writeTo(slice: Slice[Byte], entry: MapEntry[Slice[Byte], (ValueType, Option[Slice[Byte]])]): Unit =
    entry match {
      case Add(key, (valueType, value)) =>
        slice
          .addByte(1)
          .addInt(key.size)
          .addAll(key)
          .addInt(valueType.id)

        value match {
          case Some(value) =>
            slice
              .addInt(value.size)
              .addAll(value)

          case None =>
            slice.addInt(0)
        }

      case Remove(key) =>
        slice
          .addByte(0)
          .addInt(key.size)
          .addAll(key)
    }

  override def bytesRequiredFor(entry: MapEntry[Slice[Byte], (ValueType, Option[Slice[Byte]])]): Int =
    entry match {
      case Add(key, value) =>
        if (key.isEmpty)
          0
        else
          1 +
            ByteSizeOf.int +
            key.size +
            ByteSizeOf.int +
            ByteSizeOf.int +
            value._2.map(_.size).getOrElse(0)

      case Remove(key) =>
        if (key.isEmpty)
          0
        else
          1 +
            ByteSizeOf.int +
            key.size
    }

}