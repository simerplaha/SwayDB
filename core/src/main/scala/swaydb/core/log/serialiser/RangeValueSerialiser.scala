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

package swaydb.core.log.serialiser

import swaydb.core.segment.data.Value
import swaydb.core.segment.data.Value.{Put, Remove, Update}
import swaydb.core.file.reader.Reader
import swaydb.core.util.Bytes
import swaydb.slice.{ReaderBase, Slice, SliceMut}

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for RangeValueSerialiser of type [${F}, ${R}]")
private[core] sealed trait RangeValueSerialiser[F, R] {

  def write(fromValue: F, rangeValue: R, bytes: SliceMut[Byte]): Unit

  def bytesRequired(fromValue: F, rangeValue: R): Int
}

private[core] object RangeValueSerialiser {

  /**
   * Single
   */

  implicit object UnitRemoveSerialiser$ extends RangeValueSerialiser[Unit, Value.Remove] {

    val id = swaydb.core.log.serialiser.RemoveRange.id

    override def write(fromValue: Unit, rangeValue: Value.Remove, bytes: SliceMut[Byte]): Unit =
      ValueSerialiser.write[Value.Remove](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Remove): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerialiser.bytesRequired(rangeValue)

    def read(reader: ReaderBase[Byte]): (Unit, Remove) =
      ((), ValueSerialiser.read[Value.Remove](reader))
  }

  implicit object UnitUpdateSerialiser$ extends RangeValueSerialiser[Unit, Value.Update] {

    val id = swaydb.core.log.serialiser.UpdateRange.id

    override def write(fromValue: Unit, rangeValue: Value.Update, bytes: SliceMut[Byte]): Unit =
      ValueSerialiser.write[Value.Update](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Update): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerialiser.bytesRequired(rangeValue)

    def read(reader: ReaderBase[Byte]): (Unit, Value.Update) =
      ((), ValueSerialiser.read[Value.Update](reader))
  }

  implicit object UnitFunctionSerialiser$ extends RangeValueSerialiser[Unit, Value.Function] {

    val id = swaydb.core.log.serialiser.FunctionRange.id

    override def write(fromValue: Unit, rangeValue: Value.Function, bytes: SliceMut[Byte]): Unit =
      ValueSerialiser.write[Value.Function](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.Function): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerialiser.bytesRequired(rangeValue)

    def read(reader: ReaderBase[Byte]): (Unit, Value.Function) =
      ((), ValueSerialiser.read[Value.Function](reader))
  }

  implicit object UnitPendingApplySerialiser$ extends RangeValueSerialiser[Unit, Value.PendingApply] {

    val id = swaydb.core.log.serialiser.PendingApplyRange.id

    override def write(fromValue: Unit, rangeValue: Value.PendingApply, bytes: SliceMut[Byte]): Unit =
      ValueSerialiser.write[Value.PendingApply](rangeValue)(bytes.addUnsignedInt(id))

    override def bytesRequired(fromValue: Unit, rangeValue: Value.PendingApply): Int =
      Bytes.sizeOfUnsignedInt(id) + ValueSerialiser.bytesRequired(rangeValue)

    def read(reader: ReaderBase[Byte]): (Unit, Value.PendingApply) =
      ((), ValueSerialiser.read[Value.PendingApply](reader))
  }

  /**
   * Remove
   */
  implicit object RemoveRemoveSerialiser$ extends RangeValueSerialiser[Value.Remove, Value.Remove] {

    val id = swaydb.core.log.serialiser.RemoveRemoveRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Remove, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Remove, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object RemoveUpdateSerialiser$ extends RangeValueSerialiser[Value.Remove, Value.Update] {

    val id = swaydb.core.log.serialiser.RemoveUpdateRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Update, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Remove, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object RemoveFunctionSerialiser$ extends RangeValueSerialiser[Value.Remove, Value.Function] {

    val id = swaydb.core.log.serialiser.RemoveFunctionRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Function, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Remove, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object RemovePendingApplySerialiser$ extends RangeValueSerialiser[Value.Remove, Value.PendingApply] {

    val id = swaydb.core.log.serialiser.RemovePendingApplyRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.PendingApply, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Remove, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Remove, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Remove](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
    }
  }

  /**
   * Put
   */

  implicit object PutRemoveSerialiser$ extends RangeValueSerialiser[Value.Put, Value.Remove] {

    val id = swaydb.core.log.serialiser.PutRemoveRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Remove, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Put, Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PutUpdateSerialiser$ extends RangeValueSerialiser[Value.Put, Value.Update] {

    val id = swaydb.core.log.serialiser.PutUpdateRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Update, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Put, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PutFunctionSerialiser$ extends RangeValueSerialiser[Value.Put, Value.Function] {

    val id = swaydb.core.log.serialiser.PutFunctionRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Function, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Put, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PutPendingApplySerialiser$ extends RangeValueSerialiser[Value.Put, Value.PendingApply] {

    val id = swaydb.core.log.serialiser.PutPendingApplyRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.PendingApply, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Put, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Put, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Put](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
    }
  }

  /**
   * Update
   */
  implicit object UpdateRemoveSerialiser$ extends RangeValueSerialiser[Value.Update, Value.Remove] {
    val id = swaydb.core.log.serialiser.UpdateRemoveRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Remove, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Update, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object UpdateUpdateSerialiser$ extends RangeValueSerialiser[Value.Update, Value.Update] {

    val id = swaydb.core.log.serialiser.UpdateUpdateRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Update, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Update, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object UpdateFunctionSerialiser$ extends RangeValueSerialiser[Value.Update, Value.Function] {
    val id = swaydb.core.log.serialiser.UpdateFunctionRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Function, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Update, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object UpdatePendingApplySerialiser$ extends RangeValueSerialiser[Value.Update, Value.PendingApply] {
    val id = swaydb.core.log.serialiser.UpdatePendingApplyRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.PendingApply, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Update, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Update, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Update](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
    }
  }

  /**
   * Function
   */
  implicit object FunctionRemoveSerialiser extends RangeValueSerialiser[Value.Function, Value.Remove] {
    val id = swaydb.core.log.serialiser.FunctionRemoveRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Remove, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Function, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object FunctionUpdateSerialiser extends RangeValueSerialiser[Value.Function, Value.Update] {
    val id = swaydb.core.log.serialiser.FunctionUpdateRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Update, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Function, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object FunctionFunctionSerialiser extends RangeValueSerialiser[Value.Function, Value.Function] {
    val id = swaydb.core.log.serialiser.FunctionFunctionRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.Function, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Function, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object FunctionPendingApplySerialiser extends RangeValueSerialiser[Value.Function, Value.PendingApply] {
    val id = swaydb.core.log.serialiser.FunctionPendingApplyRange.id

    override def write(fromValue: Value.Function, rangeValue: Value.PendingApply, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.Function, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.Function, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.Function](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
    }
  }
  /**
   * Function
   */
  implicit object PendingApplyRemoveSerialiser$ extends RangeValueSerialiser[Value.PendingApply, Value.Remove] {
    val id = swaydb.core.log.serialiser.PendingApplyRemoveRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Remove, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Remove): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.PendingApply, Value.Remove) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Remove](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PendingApplyUpdateSerialiser$ extends RangeValueSerialiser[Value.PendingApply, Value.Update] {
    val id = swaydb.core.log.serialiser.PendingApplyUpdateRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Update, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Update): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.PendingApply, Value.Update) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Update](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PendingApplyFunctionSerialiser extends RangeValueSerialiser[Value.PendingApply, Value.Function] {
    val id = swaydb.core.log.serialiser.PendingApplyFunctionRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.Function, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.Function): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.PendingApply, Value.Function) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.Function](reader)
      (fromKeyValue, rangeValue)
    }
  }

  implicit object PendingApplyPendingApplySerialiser$ extends RangeValueSerialiser[Value.PendingApply, Value.PendingApply] {
    val id = swaydb.core.log.serialiser.PendingApplyPendingApplyRange.id

    override def write(fromValue: Value.PendingApply, rangeValue: Value.PendingApply, bytes: SliceMut[Byte]): Unit = {
      ValueSerialiser.write(fromValue) {
        bytes
          .addUnsignedInt(id)
          .addUnsignedInt(ValueSerialiser.bytesRequired(fromValue))
      }
      ValueSerialiser.write(rangeValue)(bytes)
    }

    override def bytesRequired(fromValue: Value.PendingApply, rangeValue: Value.PendingApply): Int = {
      val fromValueBytesRequired = ValueSerialiser.bytesRequired(fromValue)
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(fromValueBytesRequired) +
        fromValueBytesRequired +
        ValueSerialiser.bytesRequired(rangeValue)
    }

    def read(reader: ReaderBase[Byte]): (Value.PendingApply, Value.PendingApply) = {
      val fromValueBytes = reader.read(reader.readUnsignedInt())
      val fromKeyValue = ValueSerialiser.read[Value.PendingApply](fromValueBytes)
      val rangeValue = ValueSerialiser.read[Value.PendingApply](reader)
      (fromKeyValue, rangeValue)
    }
  }

  def write[F, R](fromValue: F, rangeValue: R)(bytes: SliceMut[Byte])(implicit serialiser: RangeValueSerialiser[F, R]): Unit =
    serialiser.write(fromValue, rangeValue, bytes)

  def bytesRequired[F, R](fromValue: F, rangeValue: R)(implicit serialiser: RangeValueSerialiser[F, R]): Int =
    serialiser.bytesRequired(fromValue, rangeValue)

  implicit object OptionRangeValueSerialiser extends RangeValueSerialiser[Value.FromValueOption, Value.RangeValue] {

    override def write(fromValue: Value.FromValueOption, rangeValue: Value.RangeValue, bytes: SliceMut[Byte]): Unit =
      (fromValue, rangeValue) match {

        case (Value.FromValue.Null, rangeValue: Value.Remove) =>
          RangeValueSerialiser.write[Unit, Value.Remove]((), rangeValue)(bytes)
        case (Value.FromValue.Null, rangeValue: Value.Update) =>
          RangeValueSerialiser.write[Unit, Value.Update]((), rangeValue)(bytes)
        case (Value.FromValue.Null, rangeValue: Value.Function) =>
          RangeValueSerialiser.write[Unit, Value.Function]((), rangeValue)(bytes)
        case (Value.FromValue.Null, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.write[Unit, Value.PendingApply]((), rangeValue)(bytes)

        case (fromValue: Value.Remove, rangeValue: Value.Update) =>
          RangeValueSerialiser.write[Remove, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Remove, rangeValue: Value.Remove) =>
          RangeValueSerialiser.write[Remove, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Remove, rangeValue: Value.Function) =>
          RangeValueSerialiser.write[Remove, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Remove, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.write[Remove, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.Put, rangeValue: Value.Update) =>
          RangeValueSerialiser.write[Put, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Put, rangeValue: Value.Remove) =>
          RangeValueSerialiser.write[Put, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Put, rangeValue: Value.Function) =>
          RangeValueSerialiser.write[Put, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Put, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.write[Put, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.Update, rangeValue: Value.Update) =>
          RangeValueSerialiser.write[Update, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Update, rangeValue: Value.Remove) =>
          RangeValueSerialiser.write[Update, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Update, rangeValue: Value.Function) =>
          RangeValueSerialiser.write[Update, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Update, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.write[Update, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.Function, rangeValue: Value.Update) =>
          RangeValueSerialiser.write[Value.Function, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Function, rangeValue: Value.Remove) =>
          RangeValueSerialiser.write[Value.Function, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Function, rangeValue: Value.Function) =>
          RangeValueSerialiser.write[Value.Function, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.Function, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.write[Value.Function, Value.PendingApply](fromValue, rangeValue)(bytes)

        case (fromValue: Value.PendingApply, rangeValue: Value.Update) =>
          RangeValueSerialiser.write[Value.PendingApply, Update](fromValue, rangeValue)(bytes)
        case (fromValue: Value.PendingApply, rangeValue: Value.Remove) =>
          RangeValueSerialiser.write[Value.PendingApply, Remove](fromValue, rangeValue)(bytes)
        case (fromValue: Value.PendingApply, rangeValue: Value.Function) =>
          RangeValueSerialiser.write[Value.PendingApply, Value.Function](fromValue, rangeValue)(bytes)
        case (fromValue: Value.PendingApply, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.write[Value.PendingApply, Value.PendingApply](fromValue, rangeValue)(bytes)
      }

    override def bytesRequired(fromValue: Value.FromValueOption, rangeValue: Value.RangeValue): Int =
      (fromValue, rangeValue) match {

        case (Value.FromValue.Null, rangeValue: Value.Remove) =>
          RangeValueSerialiser.bytesRequired[Unit, Value.Remove]((), rangeValue)
        case (Value.FromValue.Null, rangeValue: Value.Update) =>
          RangeValueSerialiser.bytesRequired[Unit, Value.Update]((), rangeValue)
        case (Value.FromValue.Null, rangeValue: Value.Function) =>
          RangeValueSerialiser.bytesRequired[Unit, Value.Function]((), rangeValue)
        case (Value.FromValue.Null, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.bytesRequired[Unit, Value.PendingApply]((), rangeValue)

        case (fromValue: Value.Remove, rangeValue: Value.Update) =>
          RangeValueSerialiser.bytesRequired[Remove, Update](fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.Remove) =>
          RangeValueSerialiser.bytesRequired[Remove, Remove](fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.Function) =>
          RangeValueSerialiser.bytesRequired[Remove, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.bytesRequired[Remove, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.Put, rangeValue: Value.Update) =>
          RangeValueSerialiser.bytesRequired[Put, Update](fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.Remove) =>
          RangeValueSerialiser.bytesRequired[Put, Remove](fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.Function) =>
          RangeValueSerialiser.bytesRequired[Put, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.bytesRequired[Put, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.Update, rangeValue: Value.Update) =>
          RangeValueSerialiser.bytesRequired[Update, Update](fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.Remove) =>
          RangeValueSerialiser.bytesRequired[Update, Remove](fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.Function) =>
          RangeValueSerialiser.bytesRequired[Update, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.bytesRequired[Update, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.Function, rangeValue: Value.Update) =>
          RangeValueSerialiser.bytesRequired[Value.Function, Update](fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.Remove) =>
          RangeValueSerialiser.bytesRequired[Value.Function, Remove](fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.Function) =>
          RangeValueSerialiser.bytesRequired[Value.Function, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.bytesRequired[Value.Function, Value.PendingApply](fromValue, rangeValue)

        case (fromValue: Value.PendingApply, rangeValue: Value.Update) =>
          RangeValueSerialiser.bytesRequired[Value.PendingApply, Update](fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.Remove) =>
          RangeValueSerialiser.bytesRequired[Value.PendingApply, Remove](fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.Function) =>
          RangeValueSerialiser.bytesRequired[Value.PendingApply, Value.Function](fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.PendingApply) =>
          RangeValueSerialiser.bytesRequired[Value.PendingApply, Value.PendingApply](fromValue, rangeValue)
      }
  }

  @inline private implicit def unitFromValueToNone[R](tuple: (Unit, R)): (Value.FromValue.Null.type, R) =
    (Value.FromValue.Null, tuple._2)

  private def read(rangeId: Int,
                   reader: ReaderBase[Byte]): (Value.FromValueOption, Value.RangeValue) =
    rangeId match {
      case RemoveRemoveSerialiser$.id =>
        RemoveRemoveSerialiser$.read(reader)
      case RemoveUpdateSerialiser$.id =>
        RemoveUpdateSerialiser$.read(reader)
      case RemoveFunctionSerialiser$.id =>
        RemoveFunctionSerialiser$.read(reader)
      case RemovePendingApplySerialiser$.id =>
        RemovePendingApplySerialiser$.read(reader)

      case PutRemoveSerialiser$.id =>
        PutRemoveSerialiser$.read(reader)
      case PutUpdateSerialiser$.id =>
        PutUpdateSerialiser$.read(reader)
      case PutFunctionSerialiser$.id =>
        PutFunctionSerialiser$.read(reader)
      case PutPendingApplySerialiser$.id =>
        PutPendingApplySerialiser$.read(reader)

      case UpdateRemoveSerialiser$.id =>
        UpdateRemoveSerialiser$.read(reader)
      case UpdateUpdateSerialiser$.id =>
        UpdateUpdateSerialiser$.read(reader)
      case UpdateFunctionSerialiser$.id =>
        UpdateFunctionSerialiser$.read(reader)
      case UpdatePendingApplySerialiser$.id =>
        UpdatePendingApplySerialiser$.read(reader)

      case FunctionRemoveSerialiser.id =>
        FunctionRemoveSerialiser.read(reader)
      case FunctionUpdateSerialiser.id =>
        FunctionUpdateSerialiser.read(reader)
      case FunctionFunctionSerialiser.id =>
        FunctionFunctionSerialiser.read(reader)
      case FunctionPendingApplySerialiser.id =>
        FunctionPendingApplySerialiser.read(reader)

      case PendingApplyRemoveSerialiser$.id =>
        PendingApplyRemoveSerialiser$.read(reader)
      case PendingApplyUpdateSerialiser$.id =>
        PendingApplyUpdateSerialiser$.read(reader)
      case PendingApplyFunctionSerialiser.id =>
        PendingApplyFunctionSerialiser.read(reader)
      case PendingApplyPendingApplySerialiser$.id =>
        PendingApplyPendingApplySerialiser$.read(reader)

      case UnitRemoveSerialiser$.id =>
        UnitRemoveSerialiser$.read(reader)
      case UnitUpdateSerialiser$.id =>
        UnitUpdateSerialiser$.read(reader)
      case UnitFunctionSerialiser$.id =>
        UnitFunctionSerialiser$.read(reader)
      case UnitPendingApplySerialiser$.id =>
        UnitPendingApplySerialiser$.read(reader)
    }

  def read(bytes: Slice[Byte]): (Value.FromValueOption, Value.RangeValue) = {
    val reader = Reader(bytes)
    val rangeId = reader.readUnsignedInt()
    read(rangeId, reader)
  }
}
