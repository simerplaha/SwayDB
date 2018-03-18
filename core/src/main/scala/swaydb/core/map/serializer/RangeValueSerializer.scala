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
import swaydb.core.data.Value.{Put, Remove}
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializers.{PutPutSerializer, PutRemoveSerializer, RemovePutSerializer, UnitPutSerializer}
import swaydb.core.map.serializer.ValueSerializers.{PutSerializerWithUnsignedSize, PutSerializerWithoutSize}
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.implicitNotFound
import scala.util.{Success, Try}

@implicitNotFound("Type class implementation not found for RangeValueSerializer of type [${F}, ${R}]")
sealed trait RangeValueSerializer[F, R] {

  def write(fromValue: F, rangeValue: R, bytes: Slice[Byte]): Unit

  def bytesRequiredAndRangeId(fromValue: F, rangeValue: R): (Int, Int)
}

object RangeValueSerializers {

  val removeRangeId = 2

  implicit object RemovePutSerializer extends RangeValueSerializer[Value.Remove, Value.Put] {
    val rangeId = 3

    override def write(fromValue: Value.Remove, rangeValue: Value.Put, bytes: Slice[Byte]): Unit =
      ValueSerializer.write(rangeValue)(bytes)(PutSerializerWithoutSize)

    override def bytesRequiredAndRangeId(fromValue: Value.Remove, rangeValue: Value.Put): (Int, Int) =
      (ValueSerializer.bytesRequired(rangeValue)(PutSerializerWithoutSize), rangeId)

    def read(reader: Reader): Try[(Remove, Put)] =
      ValueSerializer.read[Value.Put](reader)(PutSerializerWithoutSize) map {
        rangeValue =>
          (Value.Remove, rangeValue)
      }
  }

  implicit object PutRemoveSerializer extends RangeValueSerializer[Value.Put, Value.Remove] {
    val rangeId = 4

    override def write(fromValue: Value.Put, rangeValue: Value.Remove, bytes: Slice[Byte]): Unit =
      ValueSerializer.write(fromValue)(bytes)(PutSerializerWithoutSize)

    override def bytesRequiredAndRangeId(fromValue: Value.Put, rangeValue: Value.Remove): (Int, Int) =
      (ValueSerializer.bytesRequired(fromValue)(PutSerializerWithoutSize), rangeId)

    def read(reader: Reader): Try[(Put, Remove)] =
      ValueSerializer.read[Value.Put](reader)(PutSerializerWithoutSize) map {
        fromPut =>
          (fromPut, Value.Remove)
      }
  }

  implicit object PutPutSerializer extends RangeValueSerializer[Value.Put, Value.Put] {
    val rangeId = 5

    override def write(fromValue: Value.Put, rangeValue: Value.Put, bytes: Slice[Byte]): Unit = {
      ValueSerializer.write(fromValue)(bytes)(PutSerializerWithUnsignedSize)
      ValueSerializer.write(rangeValue)(bytes)(PutSerializerWithoutSize)
    }

    override def bytesRequiredAndRangeId(fromValue: Value.Put, rangeValue: Value.Put): (Int, Int) =
      (
        ValueSerializer.bytesRequired(fromValue)(PutSerializerWithUnsignedSize) + ValueSerializer.bytesRequired(rangeValue)(PutSerializerWithoutSize),
        rangeId
      )

    def read(reader: Reader): Try[(Put, Put)] =
      ValueSerializer.read[Value.Put](reader)(PutSerializerWithUnsignedSize) flatMap {
        fromKeyValue =>
          ValueSerializer.read[Value.Put](reader)(PutSerializerWithoutSize) map {
            rangeValue =>
              (fromKeyValue, rangeValue)
          }
      }
  }

  implicit object UnitPutSerializer extends RangeValueSerializer[Unit, Value.Put] {
    val rangeId = 7

    override def write(fromValue: Unit, rangeValue: Value.Put, bytes: Slice[Byte]): Unit =
      ValueSerializer.write[Value.Put](rangeValue)(bytes)(PutSerializerWithoutSize)

    override def bytesRequiredAndRangeId(fromValue: Unit, rangeValue: Value.Put): (Int, Int) =
      (ValueSerializer.bytesRequired(rangeValue)(PutSerializerWithoutSize), rangeId)

    def read(reader: Reader): Try[(Unit, Put)] =
      ValueSerializer.read[Value.Put](reader)(PutSerializerWithoutSize).map(put => ((), put))
  }

  implicit object OptionRangeValueSerializer extends RangeValueSerializer[Option[Value.Fixed], Value.Fixed] {

    override def write(fromValue: Option[Value.Fixed], rangeValue: Value.Fixed, bytes: Slice[Byte]): Unit =
      (fromValue, rangeValue) match {
        case (None, _: Value.Remove) | (Some(_: Value.Remove), _: Value.Remove) =>
          ()

        case (None, rangeValue: Value.Put) =>
          RangeValueSerializer.write[Unit, Value.Put]((), rangeValue)(bytes)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Put) =>
          RangeValueSerializer.write[Remove, Put](fromValue, rangeValue)(bytes)

        case (Some(fromValue: Value.Put), rangeValue: Value.Remove) =>
          RangeValueSerializer.write[Put, Remove](fromValue, rangeValue)(bytes)
        case (Some(fromValue: Value.Put), rangeValue: Value.Put) =>
          RangeValueSerializer.write[Put, Put](fromValue, rangeValue)(bytes)
      }

    override def bytesRequiredAndRangeId(fromValue: Option[Value.Fixed], rangeValue: Value.Fixed): (Int, Int) =
      (fromValue, rangeValue) match {
        case (None, _: Value.Remove) | (Some(_: Value.Remove), _: Value.Remove) =>
          (0, RangeValueSerializer.removeRangeId)
        case (None, rangeValue: Value.Put) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Unit, Value.Put]((), rangeValue)

        case (Some(fromValue: Value.Remove), rangeValue: Value.Put) =>
          RangeValueSerializer.bytesRequiredAndRangeId(fromValue, rangeValue)

        case (Some(fromValue: Value.Put), rangeValue: Value.Remove) =>
          RangeValueSerializer.bytesRequiredAndRangeId(fromValue, rangeValue)
        case (Some(fromValue: Value.Put), rangeValue: Value.Put) =>
          RangeValueSerializer.bytesRequiredAndRangeId(fromValue, rangeValue)
      }

  }

  implicit object OptionRemovePutSerializer extends RangeValueSerializer[Option[Value.Remove], Value.Put] {
    override def write(fromValue: Option[Value.Remove], rangeValue: Value.Put, bytes: Slice[Byte]): Unit =
      fromValue match {
        case Some(fromValue) =>
          RangeValueSerializer.write[Value.Remove, Value.Put](fromValue, rangeValue)(bytes)
        case None =>
          RangeValueSerializer.write[Unit, Value.Put]((), rangeValue)(bytes)
      }

    override def bytesRequiredAndRangeId(fromValue: Option[Value.Remove], rangeValue: Value.Put): (Int, Int) =
      fromValue match {
        case Some(fromValue) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Value.Remove, Value.Put](fromValue, rangeValue)
        case None =>
          RangeValueSerializer.bytesRequiredAndRangeId[Unit, Value.Put]((), rangeValue)
      }

  }

  implicit object OptionPutPutSerializer extends RangeValueSerializer[Option[Value.Put], Value.Put] {

    override def write(fromValue: Option[Value.Put], rangeValue: Value.Put, bytes: Slice[Byte]): Unit =
      fromValue match {
        case Some(fromValue) =>
          RangeValueSerializer.write(fromValue, rangeValue)(bytes)
        case None =>
          RangeValueSerializer.write((), rangeValue)(bytes)
      }

    override def bytesRequiredAndRangeId(fromValue: Option[Value.Put], rangeValue: Value.Put): (Int, Int) =
      fromValue match {
        case Some(fromValue) =>
          RangeValueSerializer.bytesRequiredAndRangeId[Value.Put, Value.Put](fromValue, rangeValue)
        case None =>
          RangeValueSerializer.bytesRequiredAndRangeId((), rangeValue)
      }
  }
}

object RangeValueSerializer {

  val minId = RangeValueSerializers.removeRangeId
  val maxId = UnitPutSerializer.rangeId

  val removeRangeId = RangeValueSerializers.removeRangeId

  def write[F, R](fromValue: F, rangeValue: R)(bytes: Slice[Byte])(implicit serializer: RangeValueSerializer[F, R]): Unit =
    serializer.write(fromValue, rangeValue, bytes)

  def bytesRequiredAndRangeId[F, R](fromValue: F, rangeValue: R)(implicit serializer: RangeValueSerializer[F, R]): (Int, Int) =
    serializer.bytesRequiredAndRangeId(fromValue, rangeValue)

  def read(id: Int, bytes: Slice[Byte]): Try[(Option[Value.Fixed], Value.Fixed)] =
    id match {
      case `removeRangeId` =>
        Success(None, Value.Remove)

      case RemovePutSerializer.rangeId =>
        RemovePutSerializer.read(Reader(bytes)) map {
          case (fromValue, rangeValue) =>
            (Some(fromValue), rangeValue)
        }
      case PutRemoveSerializer.rangeId =>
        PutRemoveSerializer.read(Reader(bytes)) map {
          case (fromValue, rangeValue) =>
            (Some(fromValue), rangeValue)
        }
      case PutPutSerializer.rangeId =>
        PutPutSerializer.read(Reader(bytes)) map {
          case (fromValue, rangeValue) =>
            (Some(fromValue), rangeValue)
        }
      case UnitPutSerializer.rangeId =>
        UnitPutSerializer.read(Reader(bytes)) map {
          case (_, rangeValue) =>
            (None, rangeValue)
        }
    }
}