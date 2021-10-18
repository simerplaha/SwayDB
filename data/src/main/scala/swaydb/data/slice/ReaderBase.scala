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

package swaydb.data.slice

import swaydb.data.utils.ByteOps
import swaydb.utils.Maybe.Maybe
import swaydb.{IO, Pair}

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.Path
import scala.annotation.tailrec

trait ReaderBase[B] { self =>

  def byteOps: ByteOps[B]

  def path: Path

  def get(): B

  def read(size: Long): Slice[B] =
    read(size.toInt)

  def read(size: Int): Slice[B]

  def size: Long

  def hasMore: Boolean

  def hasAtLeast(size: Long): Boolean

  def getPosition: Int

  def moveTo(position: Long): ReaderBase[B]

  def moveTo(position: Int): ReaderBase[B]

  def readRemaining(): Slice[B]

  def isFile: Boolean

  def skip(skip: Long): ReaderBase[B] =
    moveTo(getPosition + skip)

  def readBoolean(): Boolean =
    byteOps.readBoolean(self)

  def readInt(): Int =
    byteOps.readInt(self)

  def readInt(unsigned: Boolean): Int =
    if (unsigned)
      readUnsignedInt()
    else
      readInt()

  def readUnsignedInt(): Int =
    byteOps.readUnsignedInt(self)

  def readUnsignedIntWithByteSize(): (Int, Int) =
    byteOps.readUnsignedIntWithByteSize(self)

  def readUnsignedIntWithByteSizePair(): Pair[Int, Int] =
    Pair(readUnsignedIntWithByteSize())

  def readNonZeroUnsignedInt(): Int =
    byteOps.readUnsignedIntNonZero(self)

  def readNonZeroStrictUnsignedInt(): Maybe[Int] =
    byteOps.readUnsignedIntNonZeroStrict(self)

  def readNonZeroUnsignedIntWithByteSize(): (Int, Int) =
    byteOps.readUnsignedIntNonZeroWithByteSize(self)

  def readNonZeroUnsignedIntWithByteSizePair(): Pair[Int, Int] =
    Pair(readNonZeroUnsignedIntWithByteSize())

  def readUnsignedIntSized(): Slice[B] =
    read(byteOps.readUnsignedInt(self))

  def readSignedInt(): Int =
    byteOps.readSignedInt(self)

  def readLong(): Long =
    byteOps.readLong(self)

  def readUnsignedLong(): Long =
    byteOps.readUnsignedLong(self)

  def readSignedLong(): Long =
    byteOps.readSignedLong(self)

  def readRemainingAsString(charset: Charset = StandardCharsets.UTF_8): String =
    byteOps.readString(self, charset)

  def readRemainingAsStringUTF8(): String =
    byteOps.readString(self, StandardCharsets.UTF_8)

  def readString(size: Int, charset: Charset = StandardCharsets.UTF_8): String =
    byteOps.readString(size, self, charset)

  def readStringUTF8(size: Int): String =
    byteOps.readString(size, self, StandardCharsets.UTF_8)

  def readStringWithSizeUTF8(): String =
    byteOps.readStringWithSizeUTF8(self)

  def remaining: Long =
    size - getPosition

  def copy(): ReaderBase[B]

  def reset(): ReaderBase[B] =
    this moveTo 0

  @tailrec
  final def foldLeftIO[E: IO.ExceptionHandler, R](result: R)(f: (R, ReaderBase[B]) => IO[E, R]): IO[E, R] =
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
  final def foldLeft[R](result: R)(f: (R, ReaderBase[B]) => R): R =
    if (hasMore)
      foldLeft(f(result, self))(f)
    else
      result
}
