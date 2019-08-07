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

package swaydb.data.slice

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Path

import swaydb.data.util.ByteUtil
import swaydb.{ErrorHandler, IO}

import scala.annotation.tailrec
import scala.reflect.ClassTag

private[swaydb] abstract class ReaderBase[E >: swaydb.Error.IO : ErrorHandler] { self =>

  def path: Path

  def get(): IO[E, Int]

  def read(size: Long): IO[E, Slice[Byte]] =
    read(size.toInt)

  def read(size: Int): IO[E, Slice[Byte]]

  def size: IO[E, Long]

  def hasMore: IO[E, Boolean]

  def hasAtLeast(size: Long): IO[E, Boolean]

  def getPosition: Int

  def moveTo(position: Long): ReaderBase[E]

  def readRemaining(): IO[E, Slice[Byte]]

  def isFile: Boolean

  def skip(skip: Long): ReaderBase[E] =
    moveTo(getPosition + skip)

  def readBoolean(): IO[E, Boolean] =
    ByteUtil.readBoolean(self)

  def readInt(): IO[E, Int] =
    ByteUtil.readInt(self)

  def readInt(unsigned: Boolean): IO[E, Int] =
    if (unsigned)
      readIntUnsigned()
    else
      readInt()

  def readIntUnsigned(): IO[E, Int] =
    ByteUtil.readUnsignedInt(self)

  def readIntUnsignedBytes(): IO[E, Slice[Byte]] =
    ByteUtil.readUnsignedInt(self) flatMap {
      size =>
        read(size)
    }

  def readIntSigned(): IO[E, Int] =
    ByteUtil.readSignedInt(self)

  def readLong(): IO[E, Long] =
    ByteUtil.readLong(self)

  def readLongUnsigned(): IO[E, Long] =
    ByteUtil.readUnsignedLong(self)

  def readLongSigned(): IO[E, Long] =
    ByteUtil.readSignedLong(self)

  def readRemainingAsString(charset: Charset = StandardCharsets.UTF_8): IO[E, String] =
    ByteUtil.readString(self, charset)

  def readString(size: Int, charset: Charset = StandardCharsets.UTF_8): IO[E, String] =
    ByteUtil.readString(size, self, charset)

  def remaining: IO[E, Long] =
    size map {
      size =>
        size - getPosition
    }

  def copy(): ReaderBase[E]

  def reset(): ReaderBase[E] =
    this moveTo 0

  @tailrec
  final def foldLeftIO[R: ClassTag](result: R)(f: (R, ReaderBase[E]) => IO[E, R]): IO[E, R] =
    hasMore match {
      case IO.Failure(error) =>
        IO.Failure(error)

      case IO.Success(yes) if yes =>
        f(result, self) match {
          case IO.Success(newResult) =>
            foldLeftIO(newResult)(f)

          case IO.Failure(error) =>
            IO.Failure(error)
        }

      case _ =>
        IO.Success(result)
    }
}
