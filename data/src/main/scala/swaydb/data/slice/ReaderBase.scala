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

import swaydb.IO
import swaydb.data.util.Bytez

import scala.annotation.tailrec
import scala.reflect.ClassTag

private[swaydb] abstract class ReaderBase[E >: swaydb.Error.IO : IO.ExceptionHandler] { self =>

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
    Bytez.readBoolean(self)

  def readInt(): IO[E, Int] =
    Bytez.readInt(self)

  def readInt(unsigned: Boolean): IO[E, Int] =
    if (unsigned)
      readUnsignedInt()
    else
      readInt()

  def readUnsignedInt(): IO[E, Int] =
    Bytez.readUnsignedInt(self)

  def readUnsignedIntBytes(): IO[E, Slice[Byte]] =
    Bytez.readUnsignedInt(self) flatMap {
      size =>
        read(size)
    }

  def readSignedInt(): IO[E, Int] =
    Bytez.readSignedInt(self)

  def readLong(): IO[E, Long] =
    Bytez.readLong(self)

  def readUnsignedLong(): IO[E, Long] =
    Bytez.readUnsignedLong(self)

  def readSignedLong(): IO[E, Long] =
    Bytez.readSignedLong(self)

  def readRemainingAsString(charset: Charset = StandardCharsets.UTF_8): IO[E, String] =
    Bytez.readString(self, charset)

  def readString(size: Int, charset: Charset = StandardCharsets.UTF_8): IO[E, String] =
    Bytez.readString(size, self, charset)

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
      case IO.Left(error) =>
        IO.Left(error)

      case IO.Right(yes) if yes =>
        f(result, self) match {
          case IO.Right(newResult) =>
            foldLeftIO(newResult)(f)

          case IO.Left(error) =>
            IO.Left(error)
        }

      case _ =>
        IO.Right(result)
    }
}
