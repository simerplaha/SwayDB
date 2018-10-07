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

package swaydb.core.function

import java.nio.charset.StandardCharsets

import swaydb.core.data.KeyValue
import swaydb.core.io.reader.Reader
import swaydb.core.util.{Bytes, UUIDUtil}
import swaydb.data.slice.{Reader, Slice}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Try}

object FunctionValueSerializer {

  val formatId = Slice.writeIntUnsigned(1)

  def compose(left: KeyValue.ReadOnly.UpdateFunction,
              right: KeyValue.ReadOnly.UpdateFunction): Try[Slice[Byte]] =
    left.getOrFetchValue flatMap {
      leftFunction =>
        right.getOrFetchValue flatMap {
          rightFunction =>
            compose(leftFunction, rightFunction)
        }
    }

  def compose(left: Option[Slice[Byte]],
              right: Option[Slice[Byte]]): Try[Slice[Byte]] =
    (left, right) match {
      case (Some(left), Some(right)) =>
        Try(compose(left, right))
      case (None, None) =>
        Failure(new Exception("No left and right function"))
      case (Some(_), None) =>
        Failure(new Exception("No right function"))
      case (None, Some(_)) =>
        Failure(new Exception("No left function"))
    }

  def compose(left: Slice[Byte],
              right: Slice[Byte]): Slice[Byte] =
    left append right

  def write(functionId: String): Slice[Byte] = {
    val insertIdBytes = UUIDUtil.randomIdNoHyphen().getBytes(StandardCharsets.UTF_8)
    val functionIdBytes = functionId.getBytes(StandardCharsets.UTF_8)
    val valueBytes =
      Slice.create[Byte](
        Bytes.sizeOf(formatId.size) +
          Bytes.sizeOf(insertIdBytes.length) +
          insertIdBytes.length +
          Bytes.sizeOf(functionIdBytes.length) +
          functionIdBytes.length)
    valueBytes.
      addAll(formatId)
      .addIntUnsigned(insertIdBytes.length)
      .addAll(insertIdBytes)
      .addIntUnsigned(functionIdBytes.length)
      .addAll(functionIdBytes)
  }

  def read(bytes: Slice[Byte]): Try[Iterable[(String, String)]] =
    read(Reader(bytes))

  private def read(reader: Reader): Try[Iterable[(String, String)]] =
    reader.foldLeftTry(ListBuffer.empty[(String, String)]) {
      case (result, reader) =>
        readOne(reader) map {
          case (insertId, functionId) =>
            result += ((insertId, functionId))
        }
    }

  private def readOne(reader: Reader): Try[(String, String)] =
    for {
      _ <- reader.readIntUnsigned() //formatId ignored as currently there is only single format
      insertIdSize <- reader.readIntUnsigned()
      insertId <- reader.readString(insertIdSize)
      functionIdSize <- reader.readIntUnsigned()
      functionId <- reader.readString(functionIdSize)
    } yield (insertId, functionId)
}