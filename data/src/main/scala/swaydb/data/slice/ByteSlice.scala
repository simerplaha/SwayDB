/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data.slice

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.Pair
import swaydb.data.slice.Slice.Sliced

trait ByteSlice {

  @inline def addByte(value: Byte): Sliced[Byte]

  @inline def addBytes(anotherSlice: Sliced[Byte]): Sliced[Byte]

  @inline def addBoolean(bool: Boolean): Sliced[Byte]

  @inline def readBoolean(): Boolean

  @inline def addInt(integer: Int): Sliced[Byte]

  @inline def readInt(): Int

  @inline def dropUnsignedInt(): Sliced[Byte]

  @inline def addSignedInt(integer: Int): Sliced[Byte]

  @inline def readSignedInt(): Int

  @inline def addUnsignedInt(integer: Int): Sliced[Byte]

  @inline def addNonZeroUnsignedInt(integer: Int): Sliced[Byte]

  @inline def readUnsignedInt(): Int

  @inline def readUnsignedIntWithByteSize(): (Int, Int)

  @inline def readUnsignedIntWithByteSizePair(): Pair[Int, Int] =
    Pair(readUnsignedIntWithByteSize())

  @inline def readNonZeroUnsignedIntWithByteSize(): (Int, Int)

  @inline def readNonZeroUnsignedIntWithByteSizePair(): Pair[Int, Int] =
    Pair(readNonZeroUnsignedIntWithByteSize())

  @inline def addLong(num: Long): Sliced[Byte]

  @inline def readLong(): Long

  @inline def addUnsignedLong(num: Long): Sliced[Byte]

  @inline def readUnsignedLong(): Long

  @inline def readUnsignedLongWithByteSize(): (Long, Int)

  @inline def readUnsignedLongWithByteSizePair(): Pair[Long, Int] =
    Pair(readUnsignedLongWithByteSize())

  @inline def readUnsignedLongByteSize(): Int

  @inline def addSignedLong(num: Long): Sliced[Byte]

  @inline def readSignedLong(): Long

  @inline def addString(string: String, charsets: Charset = StandardCharsets.UTF_8): Sliced[Byte]

  @inline def addStringUTF8(string: String): Sliced[Byte] =
    addString(string, StandardCharsets.UTF_8)

  @inline def readString(charset: Charset = StandardCharsets.UTF_8): String

  @inline def readStringUTF8(): String =
    readString(StandardCharsets.UTF_8)

  @inline def toByteBufferWrap: ByteBuffer

  @inline def toByteBufferDirect: ByteBuffer

  @inline def toByteArrayOutputStream: ByteArrayInputStream

  @inline def createReader(): Reader
}
