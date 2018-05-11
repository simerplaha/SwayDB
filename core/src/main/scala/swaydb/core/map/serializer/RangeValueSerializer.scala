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

import swaydb.core.data.Value.{Put, Remove, Update}
import swaydb.core.data.{DataId, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializers.{PutRemoveSerializer, PutUpdateSerializer, RemoveRemoveSerializer, RemoveUpdateSerializer, UnitRemoveSerializer, UnitUpdateSerializer, UpdateRemoveSerializer, UpdateUpdateSerializer}
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.implicitNotFound
import scala.util.Try

@implicitNotFound("Type class implementation not found for RangeValueSerializer of type [${F}, ${R}]")
sealed trait RangeValueSerializer[F, R] {

  def write(fromValue: F, rangeValue: R, bytes: Slice[Byte]): Unit

  def bytesRequiredAndRangeId(fromValue: F, rangeValue: R): (Int, Int)
}

object RangeValueSerializers {

  import ValueSerializers.Levels._

  implicit object UnitRemoveSerializer extends RangeValueSerializer[Unit, Value.Remove] {

    val id = DataId.RemoveRange.id

    override def write(fromValue: Unit, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Remove](rangeValue)(bytes)

    override def bytesRequiredAndRangeId(fromValue: Unit, rangeValue: Value.Remove): (Int, Int) =
      (ValueSerializer.bytesRequired(rangeValue), id)

    def read(reader: Reader): Try[(Unit, Remove)] =
      ValueSerializer.read[Value.Remove](reader).map(put => ((), put))
  }

  implicit object UnitUpdateSerializer extends RangeValueSerializer[Unit, Value.Update] {

    val id = DataId.UpdateRange.id

    override def write(fromValue: Unit, rangeValue: Value.Update, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Update](rangeValue)(bytes)

    override def bytesRequiredAndRangeId(fromValue: Unit, rangeValue: Value.Update): (Int, Int) =
      (ValueSerializer.bytesRequired(rangeValue), id)

    def read(reader: Reader): Try[(Unit, Value.Update)] =
      ValueSerializer.read[Value.Update](reader).map(put => ((), put))
  }

  implicit object RemoveRemoveSerializer extends RangeValueSerializer[Value.Remove, Value.Remove] {

    val id = DataId.RemoveRemoveRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue)(bytes)
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequiredAndRangeId(fromValue: Value.Remove, rangeValue: Value.Remove): (Int, Int) =
      (
        ValueSerializer.bytesRequired(fromValue) + ValueSerializer.bytesRequired(rangeValue),
        id
      )

    def read(reader: Reader): Try[(Value.Remove, Value.Remove)] =
      ValueSerializer.read[Value.Remove](reader) flatMap {
        fromKeyValue =>
          ValueSerializer.read[Value.Remove](reader) map {
            rangeValue =>
              (fromKeyValue, rangeValue)
          }
      }
  }

  implicit object RemoveUpdateSerializer extends RangeValueSerializer[Value.Remove, Value.Update] {

    val id = DataId.RemoveUpdateRange.id

    override def write(fromValue: Value.Remove, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue)(bytes)
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequiredAndRangeId(fromValue: Value.Remove, rangeValue: Value.Update): (Int, Int) =
      (
        ValueSerializer.bytesRequired(fromValue) + ValueSerializer.bytesRequired(rangeValue),
        id
      )

    def read(reader: Reader): Try[(Value.Remove, Value.Update)] =
      ValueSerializer.read[Value.Remove](reader) flatMap {
        fromKeyValue =>
          ValueSerializer.read[Value.Update](reader) map {
            rangeValue =>
              (fromKeyValue, rangeValue)
          }
      }
  }

  implicit object PutRemoveSerializer extends RangeValueSerializer[Value.Put, Value.Remove] {

    val id = DataId.PutRemoveRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue)(bytes)
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequiredAndRangeId(fromValue: Value.Put, rangeValue: Value.Remove): (Int, Int) =
      (ValueSerializer.bytesRequired(fromValue) + ValueSerializer.bytesRequired(rangeValue), id)

    def read(reader: Reader): Try[(Put, Remove)] =
      ValueSerializer.read[Value.Put](reader) flatMap {
        fromKeyValue =>
          ValueSerializer.read[Value.Remove](reader) map {
            rangeValue =>
              (fromKeyValue, rangeValue)
          }
      }
  }

  implicit object PutUpdateSerializer extends RangeValueSerializer[Value.Put, Value.Update] {

    val id = DataId.PutUpdateRange.id

    override def write(fromValue: Value.Put, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue)(bytes)
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequiredAndRangeId(fromValue: Value.Put, rangeValue: Value.Update): (Int, Int) =
      (
        ValueSerializer.bytesRequired(fromValue) + ValueSerializer.bytesRequired(rangeValue),
        id
      )

    def read(reader: Reader): Try[(Value.Put, Value.Update)] =
      ValueSerializer.read[Value.Put](reader) flatMap {
        fromKeyValue =>
          ValueSerializer.read[Value.Update](reader) map {
            rangeValue =>
              (fromKeyValue, rangeValue)
          }
      }
  }

  implicit object UpdateRemoveSerializer extends RangeValueSerializer[Value.Update, Value.Remove] {
    val id = DataId.UpdateRemoveRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue)(bytes)
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequiredAndRangeId(fromValue: Value.Update, rangeValue: Value.Remove): (Int, Int) =
      (ValueSerializer.bytesRequired(fromValue) + ValueSerializer.bytesRequired(rangeValue), id)

    def read(reader: Reader): Try[(Value.Update, Value.Remove)] =
      ValueSerializer.read[Value.Update](reader) flatMap {
        fromKeyValue =>
          ValueSerializer.read[Value.Remove](reader) map {
            rangeValue =>
              (fromKeyValue, rangeValue)
          }
      }
  }

  implicit object UpdateUpdateSerializer extends RangeValueSerializer[Value.Update, Value.Update] {

    val id = DataId.UpdateUpdateRange.id

    override def write(fromValue: Value.Update, rangeValue: Value.Update, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue)(bytes)
      ValueSerializer.write(rangeValue)(bytes)
    }

    override def bytesRequiredAndRangeId(fromValue: Value.Update, rangeValue: Value.Update): (Int, Int) =
      (ValueSerializer.bytesRequired(fromValue) + ValueSerializer.bytesRequired(rangeValue), id)

    def read(reader: Reader): Try[(Value.Update, Value.Update)] =
      ValueSerializer.read[Value.Update](reader) flatMap {
        fromKeyValue =>
          ValueSerializer.read[Value.Update](reader) map {
            rangeValue =>
              (fromKeyValue, rangeValue)
          }
      }
  }

  implicit object OptionRangeValueSerializer extends RangeValueSerializer[Option[Value.FromValue], Value.RangeValue] {

    override def write(fromValue: Option[Value.FromValue], rangeValue: Value.RangeValue, bytes: Slice[Byte]): Unit =
      (fromValue, rangeValue) match {
        case (None, rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Unit, Value.Remove]((), rangeValue)(bytes)

        case (None, rangeValue: Value.Update) =>
          RangeValueSerializer.write[Unit, Value.Update]((), rangeValue)(bytes)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Remove, Update](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Remove, Remove](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Put), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Put, Update](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Put), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Put, Remove](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Update), rangeValue: Value.Update) =>
          RangeValueSerializer.write[Update, Update](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Update), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Update, Remove](fromValue, rangeValue)(bytes)
      }

    override def bytesRequiredAndRangeId(fromValue: Option[Value.FromValue], rangeValue: Value.RangeValue): (Int, Int) =
      (fromValue, rangeValue) match {
        case (None, rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Unit, Value.Remove]((), rangeValue)

        case (None, rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Unit, Value.Update]((), rangeValue)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Remove, Update](fromValue, rangeValue)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Remove, Remove](fromValue, rangeValue)

        case (Some(fromValue: Value.Put), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Put, Update](fromValue, rangeValue)

        case (Some(fromValue: Value.Put), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Put, Remove](fromValue, rangeValue)

        case (Some(fromValue: Value.Update), rangeValue: Value.Update) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Update, Update](fromValue, rangeValue)

        case (Some(fromValue: Value.Update), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Update, Remove](fromValue, rangeValue)
      }
  }
}

object RangeValueSerializer {

  def isRangeValue(id: Int): Boolean =
    id >= DataId.rangeIdMin.id && id <= DataId.rangeIdMax.id

  def write[F, R](fromValue: F, rangeValue: R)(bytes: Slice[Byte])(implicit serializer: RangeValueSerializer[F, R]): Unit =
    serializer.write(fromValue, rangeValue, bytes)

  def bytesRequiredAndRangeId[F, R](fromValue: F, rangeValue: R)(implicit serializer: RangeValueSerializer[F, R]): (Int, Int) =
    serializer.bytesRequiredAndRangeId(fromValue, rangeValue)

  def read(id: Int, bytes: Slice[Byte]): Try[(Option[Value.FromValue], Value.RangeValue)] =
    id match {
      case RemoveRemoveSerializer.id =>
        RemoveRemoveSerializer.read(Reader(bytes)) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case RemoveUpdateSerializer.id =>
        RemoveUpdateSerializer.read(Reader(bytes)) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PutRemoveSerializer.id =>
        PutRemoveSerializer.read(Reader(bytes)) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case PutUpdateSerializer.id =>
        PutUpdateSerializer.read(Reader(bytes)) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case UpdateRemoveSerializer.id =>
        UpdateRemoveSerializer.read(Reader(bytes)) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case UpdateUpdateSerializer.id =>
        UpdateUpdateSerializer.read(Reader(bytes)) map { case (fromValue, rangeValue) => (Some(fromValue), rangeValue) }
      case UnitRemoveSerializer.id =>
        UnitRemoveSerializer.read(Reader(bytes)) map { case (_, rangeValue) => (None, rangeValue) }
      case UnitUpdateSerializer.id =>
        UnitUpdateSerializer.read(Reader(bytes)) map { case (_, rangeValue) => (None, rangeValue) }
    }
}