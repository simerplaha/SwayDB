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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.slice

import java.nio.charset.{Charset, StandardCharsets}

import swaydb.data.util.ByteUtil

import scala.annotation.tailrec
import scala.reflect.ClassTag
import swaydb.data.io.IO

private[swaydb] trait Reader { self =>

  def get(): IO[Int]

  def read(size: Long): IO[Slice[Byte]] =
    read(size.toInt)

  def read(size: Int): IO[Slice[Byte]]

  def size: IO[Long]

  def hasMore: IO[Boolean]

  def hasAtLeast(size: Long): IO[Boolean]

  def getPosition: Int

  def moveTo(position: Long): Reader

  def readRemaining(): IO[Slice[Byte]]

  def skip(skip: Long): Reader =
    moveTo(getPosition + skip)

  def readBoolean(): IO[Boolean] =
    ByteUtil.readBoolean(self)

  def readInt(): IO[Int] =
    ByteUtil.readInt(self)

  def readIntUnsigned(): IO[Int] =
    ByteUtil.readUnsignedInt(self)

  def readIntUnsignedBytes(): IO[Slice[Byte]] =
    ByteUtil.readUnsignedInt(self) flatMap {
      size =>
        read(size)
    }

  def readIntSigned(): IO[Int] =
    ByteUtil.readSignedInt(self)

  def readLong(): IO[Long] =
    ByteUtil.readLong(self)

  def readLongUnsigned(): IO[Long] =
    ByteUtil.readUnsignedLong(self)

  def readLongSigned(): IO[Long] =
    ByteUtil.readSignedLong(self)

  def readRemainingAsString(charset: Charset = StandardCharsets.UTF_8): IO[String] =
    ByteUtil.readString(self, charset)

  def readString(size: Int, charset: Charset = StandardCharsets.UTF_8): IO[String] =
    ByteUtil.readString(size, self, charset)

  def remaining: IO[Long] =
    size map {
      size =>
        size - getPosition
    }

  def copy(): Reader

  @tailrec
  final def foldLeftIO[R: ClassTag](result: R)(f: (R, Reader) => IO[R]): IO[R] =
    hasMore match {
      case IO.Failure(exception) =>
        IO.Failure(exception)

      case IO.Success(yes) if yes =>
        f(result, self) match {
          case IO.Success(newResult) =>
            foldLeftIO(newResult)(f)

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }

      case _ =>
        IO.Success(result)
    }
}
