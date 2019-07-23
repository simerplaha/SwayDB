/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.map.serializer

import swaydb.IO
import swaydb.core.data.Value
import swaydb.core.data.Value.{Put, Remove, Update}
import swaydb.core.io.reader.Reader
import swaydb.core.util.Bytes
import swaydb.core.util.PipeOps._
import swaydb.data.slice.{Reader, Slice}
import swaydb.ErrorHandler.CoreError

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for RangeValueSerializer of type [${F}, ${R}]")
sealed trait RangeValueSerializer[F, R] {

  def write(fromValue: F, rangeValue: R, bytes: Slice[Byte]): Unit

  def bytesRequired(fromValue: F, rangeValue: R): Int
}

object RangeValueSerializer {

  /**
    * Single
    */

  implicit object UnitRemoveSerializer extends RangeValueSerializer[Unit, Value.Remove] {

    val id = swaydb.core.map.serializer.RemoveRange.id

    override def write(fromValue: Unit, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Remove](rangeValue)(bytes.addIntUnsigned(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Remove): Int =
      Bytes.sizeOf(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Unit, Remove)] =
      ValueSerializer.read[Value.Remove](reader).map(remove => ((), remove))
  }

  implicit object UnitUpdateSerializer extends RangeValueSerializer[Unit, Value.Update] {

    val id = swaydb.core.map.serializer.UpdateRange.id

    override def write(fromValue: Unit, rangeValue: Value.Update, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Update](rangeValue)(bytes.addIntUnsigned(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Update): Int =
      Bytes.sizeOf(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Unit, Value.Update)] =
      ValueSerializer.read[Value.Update](reader).map(update => ((), update))
  }

  implicit object UnitFunctionSerializer extends RangeValueSerializer[Unit, Value.Function] {

    val id = swaydb.core.map.serializer.FunctionRange.id

    override def write(fromValue: Unit, rangeValue: Value.Function, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Function](rangeValue)(bytes.addIntUnsigned(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Function): Int =
      Bytes.sizeOf(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Unit, Value.Function)] =
      ValueSerializer.read[Value.Function](reader).map(function => ((), function))
  }

  implicit object UnitPendingApplySerializer extends RangeValueSerializer[Unit, Value.PendingApply] {

    val id = swaydb.core.map.serializer.PendingApplyRange.id

    override def write(fromValue: Unit, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.PendingApply](rangeValue)(bytes.addIntUnsigned(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.PendingApply): Int =
      Bytes.sizeOf(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Unit, Value.PendingApply)] =
      ValueSerializer.read[Value.PendingApply](reader).map(put => ((), put))
  }

  /**
    * Remove
    */
  implicit object RemoveRemoveSerializer extends RangeValueSerializer[Value.Remove, Value.Remove] {

    val id = swaydb.core.map.serializer.RemoveRemoveRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Remove, Value.Remove)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Remove](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Remove](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object RemoveUpdateSerializer extends RangeValueSerializer[Value.Remove, Value.Update] {

    val id = swaydb.core.map.serializer.RemoveUpdateRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Remove, Value.Update)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Remove](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Update](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object RemoveFunctionSerializer extends RangeValueSerializer[Value.Remove, Value.Function] {

    val id = swaydb.core.map.serializer.RemoveFunctionRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Remove, Value.Function)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Remove](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Function](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object RemovePendingApplySerializer extends RangeValueSerializer[Value.Remove, Value.PendingApply] {

    val id = swaydb.core.map.serializer.RemovePendingApplyRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Remove, Value.PendingApply)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Remove](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.PendingApply](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  /**
    * Put
    */

  implicit object PutRemoveSerializer extends RangeValueSerializer[Value.Put, Value.Remove] {

    val id = swaydb.core.map.serializer.PutRemoveRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Put, Remove)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Put](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Remove](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object PutUpdateSerializer extends RangeValueSerializer[Value.Put, Value.Update] {

    val id = swaydb.core.map.serializer.PutUpdateRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Put, Value.Update)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Put](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Update](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object PutFunctionSerializer extends RangeValueSerializer[Value.Put, Value.Function] {

    val id = swaydb.core.map.serializer.PutFunctionRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Put, Value.Function)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Put](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Function](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object PutPendingApplySerializer extends RangeValueSerializer[Value.Put, Value.PendingApply] {

    val id = swaydb.core.map.serializer.PutPendingApplyRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Put, Value.PendingApply)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Put](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.PendingApply](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  /**
    * Update
    */
  implicit object UpdateRemoveSerializer extends RangeValueSerializer[Value.Update, Value.Remove] {
    val id = swaydb.core.map.serializer.UpdateRemoveRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Update, Value.Remove)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Update](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Remove](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object UpdateUpdateSerializer extends RangeValueSerializer[Value.Update, Value.Update] {

    val id = swaydb.core.map.serializer.UpdateUpdateRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Update, Value.Update)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Update](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Update](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object UpdateFunctionSerializer extends RangeValueSerializer[Value.Update, Value.Function] {
    val id = swaydb.core.map.serializer.UpdateFunctionRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Update, Value.Function)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Update](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Function](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object UpdatePendingApplySerializer extends RangeValueSerializer[Value.Update, Value.PendingApply] {
    val id = swaydb.core.map.serializer.UpdatePendingApplyRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Update, Value.PendingApply)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Update](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.PendingApply](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  /**
    * Function
    */
  implicit object FunctionRemoveSerializer extends RangeValueSerializer[Value.Function, Value.Remove] {
    val id = swaydb.core.map.serializer.FunctionRemoveRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Function, Value.Remove)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Function](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Remove](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object FunctionUpdateSerializer extends RangeValueSerializer[Value.Function, Value.Update] {
    val id = swaydb.core.map.serializer.FunctionUpdateRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Function, Value.Update)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Function](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Update](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object FunctionFunctionSerializer extends RangeValueSerializer[Value.Function, Value.Function] {
    val id = swaydb.core.map.serializer.FunctionFunctionRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Function, Value.Function)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Function](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Function](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object FunctionPendingApplySerializer extends RangeValueSerializer[Value.Function, Value.PendingApply] {
    val id = swaydb.core.map.serializer.FunctionPendingApplyRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.Function, Value.PendingApply)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.Function](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.PendingApply](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  /**
    * Function
    */
  implicit object PendingApplyRemoveSerializer extends RangeValueSerializer[Value.PendingApply, Value.Remove] {
    val id = swaydb.core.map.serializer.PendingApplyRemoveRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.PendingApply, Value.Remove)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.PendingApply](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Remove](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object PendingApplyUpdateSerializer extends RangeValueSerializer[Value.PendingApply, Value.Update] {
    val id = swaydb.core.map.serializer.PendingApplyUpdateRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.PendingApply, Value.Update)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.PendingApply](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Update](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object PendingApplyFunctionSerializer extends RangeValueSerializer[Value.PendingApply, Value.Function] {
    val id = swaydb.core.map.serializer.PendingApplyFunctionRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.PendingApply, Value.Function)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.PendingApply](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.Function](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  implicit object PendingApplyPendingApplySerializer extends RangeValueSerializer[Value.PendingApply, Value.PendingApply] {
    val id = swaydb.core.map.serializer.PendingApplyPendingApplyRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addIntUnsigned(id)
          .addIntUnsigned(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOf(id) +
        Bytes.sizeOf(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: Reader[IO.Error]): IO[IO.Error, (Value.PendingApply, Value.PendingApply)] =
      reader.readIntUnsigned().flatMap(reader.read) flatMap {
        fromValueBytes =>
          ValueSerializer.read[Value.PendingApply](fromValueBytes) flatMap {
            fromKeyValue =>
              ValueSerializer.read[Value.PendingApply](reader) map {
                rangeValue =>
                  (fromKeyValue, rangeValue)
              }
          }
      }
  }

  def write[F, R](fromValue: F, rangeValue: R)(bytes: Slice[Byte])(implicit serializer: RangeValueSerializer[F, R]): Unit =
    serializer.write(fromValue, rangeValue, bytes)

  def bytesRequired[F, R](fromValue: F, rangeValue: R)(implicit serializer: RangeValueSerializer[F, R]): Int =
    serializer.bytesRequired(fromValue, rangeValue)

  implicit object OptionRangeValueSerializer extends RangeValueSerializer[Option[Value.FromValue], Value.RangeValue] {

    override def write(fromValue: Option[Value.FromValue], rangeValue: Value.RangeValue, bytes: Slice[Byte]): Unit =
      (fromValue, rangeValue) match {

        case (None, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Unit, Value.Remove]((), rangeValue)(bytes)
        case (None, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Unit, Value.Update]((), rangeValue)(bytes)
        case (None, rangeValue: Value.Function) =>
          RangeValueSerializer.write[Unit, Value.Function]((), rangeValue)(bytes)
        case (None, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Unit, Value.PendingApply]((), rangeValue)(bytes)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Remove, Update](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Remove), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Remove, Remove](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Remove), rangeValue: Value.Function) =>
          RangeValueSerializer.write[Remove, Value.Function](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Remove), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Remove, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Put), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Put, Update](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Put), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Put, Remove](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Put), rangeValue: Value.Function) =>
          RangeValueSerializer.write[Put, Value.Function](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Put), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Put, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Update), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Update, Update](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Update), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Update, Remove](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Update), rangeValue: Value.Function) =>
          RangeValueSerializer.write[Update, Value.Function](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Update), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Update, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Function), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Value.Function, Update](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Function), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Value.Function, Remove](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Function), rangeValue: Value.Function) =>
          RangeValueSerializer.write[Value.Function, Value.Function](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Function), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Value.Function, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.PendingApply), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Value.PendingApply, Update](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.PendingApply), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Value.PendingApply, Remove](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.PendingApply), rangeValue: Value.Function) =>
          RangeValueSerializer.write[Value.PendingApply, Value.Function](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.PendingApply), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Value.PendingApply, Value.PendingApply](fromValue, rangeValue)(bytes)
      }

    override def bytesRequired(fromValue: Option[Value.FromValue], rangeValue: Value.RangeValue): Int =
      (fromValue, rangeValue) match {

        case (None, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Unit, Value.Remove]((), rangeValue)
        case (None, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Unit, Value.Update]((), rangeValue)
        case (None, rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Unit, Value.Function]((), rangeValue)
        case (None, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Unit, Value.PendingApply]((), rangeValue)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Remove, Update](fromValue, rangeValue)
        case (Some(fromValue: Value.Remove), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Remove, Remove](fromValue, rangeValue)
        case (Some(fromValue: Value.Remove), rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Remove, Value.Function](fromValue, rangeValue)
        case (Some(fromValue: Value.Remove), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Remove, Value.PendingApply](fromValue, rangeValue)

        case (Some(fromValue: Value.Put), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Put, Update](fromValue, rangeValue)
        case (Some(fromValue: Value.Put), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Put, Remove](fromValue, rangeValue)
        case (Some(fromValue: Value.Put), rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Put, Value.Function](fromValue, rangeValue)
        case (Some(fromValue: Value.Put), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Put, Value.PendingApply](fromValue, rangeValue)

        case (Some(fromValue: Value.Update), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Update, Update](fromValue, rangeValue)
        case (Some(fromValue: Value.Update), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Update, Remove](fromValue, rangeValue)
        case (Some(fromValue: Value.Update), rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Update, Value.Function](fromValue, rangeValue)
        case (Some(fromValue: Value.Update), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Update, Value.PendingApply](fromValue, rangeValue)

        case (Some(fromValue: Value.Function), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Value.Function, Update](fromValue, rangeValue)
        case (Some(fromValue: Value.Function), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Value.Function, Remove](fromValue, rangeValue)
        case (Some(fromValue: Value.Function), rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Value.Function, Value.Function](fromValue, rangeValue)
        case (Some(fromValue: Value.Function), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Value.Function, Value.PendingApply](fromValue, rangeValue)

        case (Some(fromValue: Value.PendingApply), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Update](fromValue, rangeValue)
        case (Some(fromValue: Value.PendingApply), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Remove](fromValue, rangeValue)
        case (Some(fromValue: Value.PendingApply), rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Value.Function](fromValue, rangeValue)
        case (Some(fromValue: Value.PendingApply), rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Value.PendingApply](fromValue, rangeValue)
      }
  }

  private def read(rangeId: Int,
                   reader: Reader[IO.Error]): IO[IO.Error, (Option[Value.FromValue], Value.RangeValue)] =
    rangeId match {
      case RemoveRemoveSerializer.id =>
        RemoveRemoveSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case RemoveUpdateSerializer.id =>
        RemoveUpdateSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case RemoveFunctionSerializer.id =>
        RemoveFunctionSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case RemovePendingApplySerializer.id =>
        RemovePendingApplySerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }

      case PutRemoveSerializer.id =>
        PutRemoveSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PutUpdateSerializer.id =>
        PutUpdateSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PutFunctionSerializer.id =>
        PutFunctionSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PutPendingApplySerializer.id =>
        PutPendingApplySerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }

      case UpdateRemoveSerializer.id =>
        UpdateRemoveSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case UpdateUpdateSerializer.id =>
        UpdateUpdateSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case UpdateFunctionSerializer.id =>
        UpdateFunctionSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case UpdatePendingApplySerializer.id =>
        UpdatePendingApplySerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }

      case FunctionRemoveSerializer.id =>
        FunctionRemoveSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case FunctionUpdateSerializer.id =>
        FunctionUpdateSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case FunctionFunctionSerializer.id =>
        FunctionFunctionSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case FunctionPendingApplySerializer.id =>
        FunctionPendingApplySerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }

      case PendingApplyRemoveSerializer.id =>
        PendingApplyRemoveSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PendingApplyUpdateSerializer.id =>
        PendingApplyUpdateSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PendingApplyFunctionSerializer.id =>
        PendingApplyFunctionSerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PendingApplyPendingApplySerializer.id =>
        PendingApplyPendingApplySerializer.read(reader) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }

      case UnitRemoveSerializer.id =>
        UnitRemoveSerializer.read(reader) map { case (_, rangeValue) => (None, rangeValue) }
      case UnitUpdateSerializer.id =>
        UnitUpdateSerializer.read(reader) map { case (_, rangeValue) => (None, rangeValue) }
      case UnitFunctionSerializer.id =>
        UnitFunctionSerializer.read(reader) map { case (_, rangeValue) => (None, rangeValue) }
      case UnitPendingApplySerializer.id =>
        UnitPendingApplySerializer.read(reader) map { case (_, rangeValue) => (None, rangeValue) }
    }

  def read(bytes: Slice[Byte]): IO[IO.Error, (Option[Value.FromValue], Value.RangeValue)] =
    Reader(bytes) ==> {
      reader =>
        reader.readIntUnsigned() flatMap {
          rangeId =>
            read(rangeId, reader)
        }
    }
}
