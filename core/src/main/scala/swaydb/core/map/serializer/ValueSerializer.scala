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

import swaydb.core.data.Value
import swaydb.core.io.reader.Reader
import swaydb.core.util.ByteUtilCore
import swaydb.core.util.TimeUtil._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.implicitNotFound
import scala.concurrent.duration.Deadline
import scala.util.Try

@implicitNotFound("Type class implementation not found for ValueSerializer of type ${T}")
trait ValueSerializer[T] {

  def write(value: T, bytes: Slice[Byte]): Unit

  def read(reader: Reader): Try[T]

  def read(bytes: Slice[Byte]): Try[T] =
    read(Reader(bytes))

  def bytesRequired(value: T): Int
}

object ValueSerializers {

  object LevelZero {

    def readDeadlineLevelZero(reader: Reader): Try[Option[Deadline]] =
      reader.readLong() map {
        deadline =>
          if (deadline == 0)
            None
          else
            deadline.toDeadlineOption
      }

    implicit object RemoveSerializerLevelZero extends ValueSerializer[Value.Remove] {

      override def write(value: Value.Remove, bytes: Slice[Byte]): Unit =
        bytes.addLong(value.deadline.toNanos)

      override def bytesRequired(value: Value.Remove): Int =
        ByteSizeOf.long

      override def read(reader: Reader): Try[Value.Remove] =
        readDeadlineLevelZero(reader) map {
          deadline =>
            Value.Remove(deadline)
        }
    }

    implicit object PutSerializerLevelZero extends ValueSerializer[Value.Put] {

      override def write(value: Value.Put, bytes: Slice[Byte]): Unit =
        bytes
          .addInt(value.value.map(_.size).getOrElse(0))
          .addAll(value.value.getOrElse(Slice.emptyByteSlice))
          .addLong(value.deadline.toNanos)

      override def bytesRequired(value: Value.Put): Int =
        ByteSizeOf.int +
          value.value.map(_.size).getOrElse(0) +
          ByteSizeOf.long

      override def read(reader: Reader): Try[Value.Put] =
        reader.readInt() flatMap {
          valueLength =>
            if (valueLength == 0)
              readDeadlineLevelZero(reader) map {
                deadline =>
                  Value.Put(None, deadline)
              }
            else
              reader.read(valueLength) flatMap {
                value =>
                  readDeadlineLevelZero(reader) map {
                    deadline =>
                      Value.Put(Some(value), deadline)
                  }
              }
        }
    }


    implicit object UpdateSerializerLevelZero extends ValueSerializer[Value.Update] {

      override def write(value: Value.Update, bytes: Slice[Byte]): Unit =
        bytes
          .addInt(value.value.map(_.size).getOrElse(0))
          .addAll(value.value.getOrElse(Slice.emptyByteSlice))
          .addLong(value.deadline.toNanos)

      override def bytesRequired(value: Value.Update): Int =
        ByteSizeOf.int +
          value.value.map(_.size).getOrElse(0) +
          ByteSizeOf.long

      override def read(reader: Reader): Try[Value.Update] =
        reader.readInt() flatMap {
          valueLength =>
            if (valueLength == 0)
              readDeadlineLevelZero(reader) map {
                deadline =>
                  Value.Update(None, deadline)
              }
            else
              reader.read(valueLength) flatMap {
                value =>
                  readDeadlineLevelZero(reader) map {
                    deadline =>
                      Value.Update(Some(value), deadline)
                  }
              }
        }
    }

  }

  object Levels {

    def readDeadlineLevels(reader: Reader): Try[Option[Deadline]] =
      reader.readLongUnsigned() map {
        deadline =>
          if (deadline == 0)
            None
          else
            deadline.toDeadlineOption
      }

    implicit object PutSerializerLevels extends ValueSerializer[Value.Put] {

      override def write(value: Value.Put, bytes: Slice[Byte]): Unit =
        bytes
          .addIntUnsigned(value.value.map(_.size).getOrElse(0))
          .addAll(value.value.getOrElse(Slice.emptyByteSlice))
          .addLongUnsigned(value.deadline.toNanos)

      override def bytesRequired(value: Value.Put): Int =
        ByteUtilCore.sizeUnsignedInt(value.value.map(_.size).getOrElse(0)) +
          value.value.map(_.size).getOrElse(0) +
          ByteUtilCore.sizeUnsignedLong(value.deadline.toNanos)

      override def read(reader: Reader): Try[Value.Put] =
        reader.readIntUnsigned() flatMap {
          valueLength =>
            if (valueLength == 0)
              readDeadlineLevels(reader) map {
                deadline =>
                  Value.Put(None, deadline)
              }
            else
              reader.read(valueLength) flatMap {
                value =>
                  readDeadlineLevels(reader) map {
                    deadline =>
                      Value.Put(Some(value), deadline)
                  }
              }
        }
    }

    implicit object RemoveSerializerLevels extends ValueSerializer[Value.Remove] {

      override def write(value: Value.Remove, bytes: Slice[Byte]): Unit =
        bytes.addLongUnsigned(value.deadline.toNanos)

      override def bytesRequired(value: Value.Remove): Int =
        ByteUtilCore.sizeUnsignedLong(value.deadline.toNanos)

      override def read(reader: Reader): Try[Value.Remove] =
        readDeadlineLevels(reader) map {
          deadline =>
            Value.Remove(deadline)
        }
    }

    implicit object UpdateSerializerLevels extends ValueSerializer[Value.Update] {

      override def write(value: Value.Update, bytes: Slice[Byte]): Unit =
        bytes
          .addIntUnsigned(value.value.map(_.size).getOrElse(0))
          .addAll(value.value.getOrElse(Slice.emptyByteSlice))
          .addLongUnsigned(value.deadline.toNanos)

      override def bytesRequired(value: Value.Update): Int =
        ByteUtilCore.sizeUnsignedInt(value.value.map(_.size).getOrElse(0)) +
          value.value.map(_.size).getOrElse(0) +
          ByteUtilCore.sizeUnsignedLong(value.deadline.toNanos)

      override def read(reader: Reader): Try[Value.Update] =
        reader.readIntUnsigned() flatMap {
          valueLength =>
            if (valueLength == 0)
              readDeadlineLevels(reader) map {
                deadline =>
                  Value.Update(None, deadline)
              }
            else
              reader.read(valueLength) flatMap {
                value =>
                  readDeadlineLevels(reader) map {
                    deadline =>
                      Value.Update(Some(value), deadline)
                  }
              }
        }
    }
  }
}

object ValueSerializer {

  def write[T](value: T)(bytes: Slice[Byte])(implicit serializer: ValueSerializer[T]): Unit =
    serializer.write(value, bytes)

  def read[T](value: Slice[Byte])(implicit serializer: ValueSerializer[T]): Try[T] =
    serializer.read(value)

  def read[T](reader: Reader)(implicit serializer: ValueSerializer[T]): Try[T] =
    serializer.read(reader)

  def bytesRequired[T](value: T)(implicit serializer: ValueSerializer[T]): Int =
    serializer.bytesRequired(value)
}