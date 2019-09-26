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

private[swaydb] abstract class ReaderBase { self =>

  def path: Path

  def get(): Int

  def read(size: Long): Slice[Byte] =
    read(size.toInt)

  def read(size: Int): Slice[Byte]

  def size: Long

  def hasMore: Boolean

  def hasAtLeast(size: Long): Boolean

  def getPosition: Int

  def moveTo(position: Long): ReaderBase

  def moveTo(position: Int): ReaderBase

  def readRemaining(): Slice[Byte]

  def isFile: Boolean

  def skip(skip: Long): ReaderBase =
    moveTo(getPosition + skip)

  def readBoolean(): Boolean =
    Bytez.readBoolean(self)

  def readInt(): Int =
    Bytez.readInt(self)

  def readInt(unsigned: Boolean): Int =
    if (unsigned)
      readUnsignedInt()
    else
      readInt()

  def readUnsignedInt(): Int =
    Bytez.readUnsignedInt(self)

  def readUnsignedIntSized(): Slice[Byte] =
    read(Bytez.readUnsignedInt(self))

  def readSignedInt(): Int =
    Bytez.readSignedInt(self)

  def readLong(): Long =
    Bytez.readLong(self)

  def readUnsignedLong(): Long =
    Bytez.readUnsignedLong(self)

  def readSignedLong(): Long =
    Bytez.readSignedLong(self)

  def readRemainingAsString(charset: Charset = StandardCharsets.UTF_8): String =
    Bytez.readString(self, charset)

  def readString(size: Int, charset: Charset = StandardCharsets.UTF_8): String =
    Bytez.readString(size, self, charset)

  def remaining: Long =
    size - getPosition

  def copy(): ReaderBase

  def reset(): ReaderBase =
    this moveTo 0

  @tailrec
  final def foldLeftIO[E: IO.ExceptionHandler, R: ClassTag](result: R)(f: (R, ReaderBase) => IO[E, R]): IO[E, R] =
    IO(hasMore) match {
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

  @tailrec
  final def foldLeft[R: ClassTag](result: R)(f: (R, ReaderBase) => R): R =
    if (hasMore)
      foldLeft(f(result, self))(f)
    else
      result
}
