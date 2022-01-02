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

import java.nio.charset.{Charset, StandardCharsets}

/**
 * Companion implementation for [[SliceMut]].
 *
 * This is a trait because the [[SliceMut]] class itself is getting too
 * long even though inheritance such as like this is discouraged.
 */
trait CompanionSliceMut {
  implicit class SliceMutByteImplicits(self: SliceMut[Byte]) {

    @inline def addBoolean(bool: Boolean): SliceMut[Byte] =
      ByteSlice.writeBoolean(bool, self)

    @inline def addInt(integer: Int): SliceMut[Byte] = {
      ByteSlice.writeInt(integer, self)
      self
    }

    @inline def addSignedInt(integer: Int): SliceMut[Byte] = {
      ByteSlice.writeSignedInt(integer, self)
      self
    }

    @inline def addUnsignedInt(integer: Int): SliceMut[Byte] = {
      ByteSlice.writeUnsignedInt(integer, self)
      self
    }

    @inline def addNonZeroUnsignedInt(integer: Int): SliceMut[Byte] = {
      ByteSlice.writeUnsignedIntNonZero(integer, self)
      self
    }

    @inline def addLong(num: Long): SliceMut[Byte] = {
      ByteSlice.writeLong(num, self)
      self
    }

    @inline def addUnsignedLong(num: Long): SliceMut[Byte] = {
      ByteSlice.writeUnsignedLong(num, self)
      self
    }

    @inline def addSignedLong(num: Long): SliceMut[Byte] = {
      ByteSlice.writeSignedLong(num, self)
      self
    }

    @inline def addString(string: String, charsets: Charset = StandardCharsets.UTF_8): SliceMut[Byte] = {
      ByteSlice.writeString(string, self, charsets)
      self
    }

    @inline def addStringUTF8(string: String): SliceMut[Byte] = {
      ByteSlice.writeString(string, self, StandardCharsets.UTF_8)
      self
    }

    @inline def addStringUTF8WithSize(string: String): SliceMut[Byte] = {
      ByteSlice.writeStringWithSize(string, self, StandardCharsets.UTF_8)
      self
    }

    @inline def createReader(): SliceReader =
      SliceReader(self)
  }
}
