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
