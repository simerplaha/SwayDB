/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import swaydb.core.data.Value
import swaydb.core.data.Value.{Put, Remove, Update}
import swaydb.core.io.reader.Reader
import swaydb.core.util.Bytes
import swaydb.data.slice.{ReaderBase, Slice}

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for RangeValueSerializer of type [${F}, ${R}]")
private[core] sealed trait RangeValueSerializer[F, R] {

  def write(fromValue: F, rangeValue: R, bytes: Slice[Byte]): Unit

  def bytesRequired(fromValue: F, rangeValue: R): Int
}

private[core] object RangeValueSerializer {

  /**
   * Single
   */

  implicit object UnitRemoveSerializer extends RangeValueSerializer[Unit, Value.Remove] {

    val id = swaydb.core.map.serializer.RemoveRange.id

    override def write(fromValue: Unit, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Remove](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Remove): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: ReaderBase): (Unit, Remove) =
      ((), ValueSerializer.read[Value.Remove](reader))
  }

  implicit object UnitUpdateSerializer extends RangeValueSerializer[Unit, Value.Update] {

    val id = swaydb.core.map.serializer.UpdateRange.id

    override def write(fromValue: Unit, rangeValue: Value.Update, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Update](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Update): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: ReaderBase): (Unit, Value.Update) =
      ((), ValueSerializer.read[Value.Update](reader))
  }

  implicit object UnitFunctionSerializer extends RangeValueSerializer[Unit, Value.Function] {

    val id = swaydb.core.map.serializer.FunctionRange.id

    override def write(fromValue: Unit, rangeValue: Value.Function, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Function](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Function): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: ReaderBase): (Unit, Value.Function) =
      ((), ValueSerializer.read[Value.Function](reader))
  }

  implicit object UnitPendingApplySerializer extends RangeValueSerializer[Unit, Value.PendingApply] {

    val id = swaydb.core.map.serializer.PendingApplyRange.id

    override def write(fromValue: Unit, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.PendingApply](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.PendingApply): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerializer.bytesRequired(rangeValue)

    def read(reader: ReaderBase): (Unit, Value.PendingApply) =
      ((), ValueSerializer.read[Value.PendingApply](reader))
  }

  /**
   * Remove
   */
  implicit object RemoveRemoveSerializer extends RangeValueSerializer[Value.Remove, Value.Remove] {

    val id = swaydb.core.map.serializer.RemoveRemoveRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Remove, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object RemoveUpdateSerializer extends RangeValueSerializer[Value.Remove, Value.Update] {

    val id = swaydb.core.map.serializer.RemoveUpdateRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Remove, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object RemoveFunctionSerializer extends RangeValueSerializer[Value.Remove, Value.Function] {

    val id = swaydb.core.map.serializer.RemoveFunctionRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Remove, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object RemovePendingApplySerializer extends RangeValueSerializer[Value.Remove, Value.PendingApply] {

    val id = swaydb.core.map.serializer.RemovePendingApplyRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Remove, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
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
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Put, Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PutUpdateSerializer extends RangeValueSerializer[Value.Put, Value.Update] {

    val id = swaydb.core.map.serializer.PutUpdateRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Put, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PutFunctionSerializer extends RangeValueSerializer[Value.Put, Value.Function] {

    val id = swaydb.core.map.serializer.PutFunctionRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Put, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PutPendingApplySerializer extends RangeValueSerializer[Value.Put, Value.PendingApply] {

    val id = swaydb.core.map.serializer.PutPendingApplyRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Put, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
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
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Update, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object UpdateUpdateSerializer extends RangeValueSerializer[Value.Update, Value.Update] {

    val id = swaydb.core.map.serializer.UpdateUpdateRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Update, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object UpdateFunctionSerializer extends RangeValueSerializer[Value.Update, Value.Function] {
    val id = swaydb.core.map.serializer.UpdateFunctionRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Update, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object UpdatePendingApplySerializer extends RangeValueSerializer[Value.Update, Value.PendingApply] {
    val id = swaydb.core.map.serializer.UpdatePendingApplyRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Update, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
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
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Function, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object FunctionUpdateSerializer extends RangeValueSerializer[Value.Function, Value.Update] {
    val id = swaydb.core.map.serializer.FunctionUpdateRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Function, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object FunctionFunctionSerializer extends RangeValueSerializer[Value.Function, Value.Function] {
    val id = swaydb.core.map.serializer.FunctionFunctionRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Function, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object FunctionPendingApplySerializer extends RangeValueSerializer[Value.Function, Value.PendingApply] {
    val id = swaydb.core.map.serializer.FunctionPendingApplyRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.Function, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
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
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.PendingApply, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PendingApplyUpdateSerializer extends RangeValueSerializer[Value.PendingApply, Value.Update] {
    val id = swaydb.core.map.serializer.PendingApplyUpdateRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.PendingApply, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PendingApplyFunctionSerializer extends RangeValueSerializer[Value.PendingApply, Value.Function] {
    val id = swaydb.core.map.serializer.PendingApplyFunctionRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Function, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.PendingApply, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PendingApplyPendingApplySerializer extends RangeValueSerializer[Value.PendingApply, Value.PendingApply] {
    val id = swaydb.core.map.serializer.PendingApplyPendingApplyRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.PendingApply, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerializer.bytesRequired(fromValue))
      }
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerializer.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerializer.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase): (Value.PendingApply, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerializer.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerializer.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
    }
  }

  def write[F, R](fromValue: F, rangeValue: R)(bytes: Slice[Byte])(implicit serializer: RangeValueSerializer[F, R]): Unit =
    serializer.write(fromValue, rangeValue, bytes)

  def bytesRequired[F, R](fromValue: F, rangeValue: R)(implicit serializer: RangeValueSerializer[F, R]): Int =
    serializer.bytesRequired(fromValue, rangeValue)

  implicit object OptionRangeValueSerializer extends RangeValueSerializer[Value.FromValueOption, Value.RangeValue] {

    override def write(fromValue: Value.FromValueOption, rangeValue: Value.RangeValue, bytes: Slice[Byte]): Unit =
      (fromValue, rangeValue) match {

        case (Value.FromValue.Null, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Unit, Value.Remove]((), rangeValue)(bytes)
        case (Value.FromValue.Null, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Unit, Value.Update]((), rangeValue)(bytes)
        case (Value.FromValue.Null, rangeValue: Value.Function) =>
          RangeValueSerializer.write[Unit, Value.Function]((), rangeValue)(bytes)
        case (Value.FromValue.Null, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Unit, Value.PendingApply]((), rangeValue)(bytes)

        case (fromValue: Value.Remove, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Remove, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Remove, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Remove, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Remove, rangeValue: Value.Function) =>
          RangeValueSerializer.write[Remove, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Remove, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Remove, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.Put, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Put, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Put, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Put, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Put, rangeValue: Value.Function) =>
          RangeValueSerializer.write[Put, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Put, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Put, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.Update, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Update, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Update, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Update, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Update, rangeValue: Value.Function) =>
          RangeValueSerializer.write[Update, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Update, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Update, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.Function, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Value.Function, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Function, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Value.Function, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Function, rangeValue: Value.Function) =>
          RangeValueSerializer.write[Value.Function, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Function, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Value.Function, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.PendingApply, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Value.PendingApply, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.PendingApply, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Value.PendingApply, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.PendingApply, rangeValue: Value.Function) =>
          RangeValueSerializer.write[Value.PendingApply, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.PendingApply, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.write[Value.PendingApply, Value.PendingApply](fromValue, rangeValue)(bytes)
      }

    override def bytesRequired(fromValue: Value.FromValueOption, rangeValue: Value.RangeValue): Int =
      (fromValue, rangeValue) match {

        case (Value.FromValue.Null, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Unit, Value.Remove]((), rangeValue)
        case (Value.FromValue.Null, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Unit, Value.Update]((), rangeValue)
        case (Value.FromValue.Null, rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Unit, Value.Function]((), rangeValue)
        case (Value.FromValue.Null, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Unit, Value.PendingApply]((), rangeValue)

        case (fromValue: Value.Remove, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Remove, Update](fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Remove, Remove](fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Remove, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Remove, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.Put, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Put, Update](fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Put, Remove](fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Put, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Put, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.Update, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Update, Update](fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Update, Remove](fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Update, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Update, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.Function, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Value.Function, Update](fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Value.Function, Remove](fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Value.Function, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Value.Function, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.PendingApply, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Update](fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Remove](fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.Function) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.PendingApply) =>
          RangeValueSerializer.bytesRequired[Value.PendingApply, Value.PendingApply](fromValue, rangeValue)
      }
  }

  @inline private implicit def unitFromValueToNone[R](tuple: (Unit, R)): (Value.FromValue.Null.type, R) =
    (Value.FromValue.Null, tuple._2)

  private def read(rangeId: Int,
                   reader: ReaderBase): (Value.FromValueOption, Value.RangeValue) =
    rangeId match {
      case RemoveRemoveSerializer.id =>
        RemoveRemoveSerializer.read(reader)
      case RemoveUpdateSerializer.id =>
        RemoveUpdateSerializer.read(reader)
      case RemoveFunctionSerializer.id =>
        RemoveFunctionSerializer.read(reader)
      case RemovePendingApplySerializer.id =>
        RemovePendingApplySerializer.read(reader)

      case PutRemoveSerializer.id =>
        PutRemoveSerializer.read(reader)
      case PutUpdateSerializer.id =>
        PutUpdateSerializer.read(reader)
      case PutFunctionSerializer.id =>
        PutFunctionSerializer.read(reader)
      case PutPendingApplySerializer.id =>
        PutPendingApplySerializer.read(reader)

      case UpdateRemoveSerializer.id =>
        UpdateRemoveSerializer.read(reader)
      case UpdateUpdateSerializer.id =>
        UpdateUpdateSerializer.read(reader)
      case UpdateFunctionSerializer.id =>
        UpdateFunctionSerializer.read(reader)
      case UpdatePendingApplySerializer.id =>
        UpdatePendingApplySerializer.read(reader)

      case FunctionRemoveSerializer.id =>
        FunctionRemoveSerializer.read(reader)
      case FunctionUpdateSerializer.id =>
        FunctionUpdateSerializer.read(reader)
      case FunctionFunctionSerializer.id =>
        FunctionFunctionSerializer.read(reader)
      case FunctionPendingApplySerializer.id =>
        FunctionPendingApplySerializer.read(reader)

      case PendingApplyRemoveSerializer.id =>
        PendingApplyRemoveSerializer.read(reader)
      case PendingApplyUpdateSerializer.id =>
        PendingApplyUpdateSerializer.read(reader)
      case PendingApplyFunctionSerializer.id =>
        PendingApplyFunctionSerializer.read(reader)
      case PendingApplyPendingApplySerializer.id =>
        PendingApplyPendingApplySerializer.read(reader)

      case UnitRemoveSerializer.id =>
        UnitRemoveSerializer.read(reader)
      case UnitUpdateSerializer.id =>
        UnitUpdateSerializer.read(reader)
      case UnitFunctionSerializer.id =>
        UnitFunctionSerializer.read(reader)
      case UnitPendingApplySerializer.id =>
        UnitPendingApplySerializer.read(reader)
    }

  def read(bytes: Slice[Byte]): (Value.FromValueOption, Value.RangeValue) = {
    val reader = Reader(bytes)
    val rangeId = reader.readUnsignedInt()
    read(rangeId, reader)
  }
}
