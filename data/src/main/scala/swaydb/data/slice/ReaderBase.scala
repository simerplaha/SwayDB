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

  def read(size: Int): Slice[B]

  def size: Int

  def hasMore: Boolean

  def hasAtLeast(size: Int): Boolean

  def getPosition: Int

  def moveTo(position: Int): ReaderBase[B]

  def readRemaining(): Slice[B]

  def isFile: Boolean

  def copy(): ReaderBase[B]

  @inline def skip(skip: Int): ReaderBase[B] =
    moveTo(getPosition + skip)

  @inline def readBoolean(): Boolean =
    byteOps.readBoolean(self)

  @inline def readInt(): Int =
    byteOps.readInt(self)

  @inline def readInt(unsigned: Boolean): Int =
    if (unsigned)
      readUnsignedInt()
    else
      readInt()

  @inline def readUnsignedInt(): Int =
    byteOps.readUnsignedInt(self)

  @inline def readUnsignedIntWithByteSize(): (Int, Int) =
    byteOps.readUnsignedIntWithByteSize(self)

  @inline def readUnsignedIntWithByteSizePair(): Pair[Int, Int] =
    Pair(readUnsignedIntWithByteSize())

  @inline def readNonZeroUnsignedInt(): Int =
    byteOps.readUnsignedIntNonZero(self)

  @inline def readNonZeroStrictUnsignedInt(): Maybe[Int] =
    byteOps.readUnsignedIntNonZeroStrict(self)

  @inline def readNonZeroUnsignedIntWithByteSize(): (Int, Int) =
    byteOps.readUnsignedIntNonZeroWithByteSize(self)

  @inline def readNonZeroUnsignedIntWithByteSizePair(): Pair[Int, Int] =
    Pair(readNonZeroUnsignedIntWithByteSize())

  @inline def readUnsignedIntSized(): Slice[B] =
    read(byteOps.readUnsignedInt(self))

  @inline def readSignedInt(): Int =
    byteOps.readSignedInt(self)

  @inline def readLong(): Long =
    byteOps.readLong(self)

  @inline def readUnsignedLong(): Long =
    byteOps.readUnsignedLong(self)

  @inline def readSignedLong(): Long =
    byteOps.readSignedLong(self)

  @inline def readRemainingAsString(charset: Charset = StandardCharsets.UTF_8): String =
    byteOps.readString(self, charset)

  @inline def readRemainingAsStringUTF8(): String =
    byteOps.readString(self, StandardCharsets.UTF_8)

  @inline def readString(size: Int, charset: Charset = StandardCharsets.UTF_8): String =
    byteOps.readString(size, self, charset)

  @inline def readStringUTF8(size: Int): String =
    byteOps.readString(size, self, StandardCharsets.UTF_8)

  @inline def readStringWithSizeUTF8(): String =
    byteOps.readStringWithSizeUTF8(self)

  @inline def remaining: Int =
    size - getPosition

  @inline def reset(): ReaderBase[B] =
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
