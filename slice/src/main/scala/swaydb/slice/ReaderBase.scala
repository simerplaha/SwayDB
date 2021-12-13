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

package swaydb.slice

import swaydb.slice.utils.ByteSlice
import swaydb.utils.Maybe.Maybe

import java.nio.charset.{Charset, StandardCharsets}
import scala.annotation.tailrec

trait ReaderBase { self =>

  def read(size: Int): Slice[Byte]

  def read(size: Int, blockSize: Int): SliceRO[Byte]

  def size(): Int

  def hasMore: Boolean

  def hasAtLeast(size: Int): Boolean

  def getPosition: Int

  def moveTo(position: Int): ReaderBase

  def readRemaining(): Slice[Byte]

  def isFile: Boolean

  def copy(): ReaderBase

  @inline def skip(skip: Int): this.type = {
    moveTo(getPosition + skip)
    this
  }

  @inline def readInt(): Int =
    ByteSlice.readInt(self)

  @inline def readInt(unsigned: Boolean): Int =
    if (unsigned)
      readUnsignedInt()
    else
      readInt()

  @inline def readUnsignedInt(): Int =
    ByteSlice.readUnsignedInt(self)

  @inline def readUnsignedIntWithByteSize(): (Int, Int) =
    ByteSlice.readUnsignedIntWithByteSize(self)

  @inline def readNonZeroUnsignedInt(): Int =
    ByteSlice.readUnsignedIntNonZero(self)

  @inline def readNonZeroStrictUnsignedInt(): Maybe[Int] =
    ByteSlice.readUnsignedIntNonZeroStrict(self)

  @inline def readNonZeroUnsignedIntWithByteSize(): (Int, Int) =
    ByteSlice.readUnsignedIntNonZeroWithByteSize(self)

  @inline def readUnsignedIntSized(): Slice[Byte] =
    read(ByteSlice.readUnsignedInt(self))

  @inline def readSignedInt(): Int =
    ByteSlice.readSignedInt(self)

  @inline def readLong(): Long =
    ByteSlice.readLong(self)

  @inline def readUnsignedLong(): Long =
    ByteSlice.readUnsignedLong(self)

  @inline def readSignedLong(): Long =
    ByteSlice.readSignedLong(self)

  @inline def readRemainingAsString(charset: Charset = StandardCharsets.UTF_8): String =
    ByteSlice.readString(self, charset)

  @inline def readRemainingAsStringUTF8(): String =
    ByteSlice.readString(self, StandardCharsets.UTF_8)

  @inline def readString(size: Int, charset: Charset = StandardCharsets.UTF_8): String =
    ByteSlice.readString(size, self, charset)

  @inline def readStringUTF8(size: Int): String =
    ByteSlice.readString(size, self, StandardCharsets.UTF_8)

  @inline def readStringWithSizeUTF8(): String =
    ByteSlice.readStringWithSizeUTF8(self)

  @inline def remaining(): Int =
    size() - getPosition

  @inline def reset(): ReaderBase =
    this moveTo 0

  @tailrec
  final def foldLeft[R](result: R)(f: (R, this.type) => R): R =
    if (hasMore)
      foldLeft(f(result, self))(f)
    else
      result
}
