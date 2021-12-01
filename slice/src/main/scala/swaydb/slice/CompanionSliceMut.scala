package swaydb.slice

import swaydb.slice.utils.ScalaByteOps

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
      ScalaByteOps.writeBoolean(bool, self)

    @inline def addInt(integer: Int): SliceMut[Byte] = {
      ScalaByteOps.writeInt(integer, self)
      self
    }

    @inline def addSignedInt(integer: Int): SliceMut[Byte] = {
      ScalaByteOps.writeSignedInt(integer, self)
      self
    }

    @inline def addUnsignedInt(integer: Int): SliceMut[Byte] = {
      ScalaByteOps.writeUnsignedInt(integer, self)
      self
    }

    @inline def addNonZeroUnsignedInt(integer: Int): SliceMut[Byte] = {
      ScalaByteOps.writeUnsignedIntNonZero(integer, self)
      self
    }

    @inline def addLong(num: Long): SliceMut[Byte] = {
      ScalaByteOps.writeLong(num, self)
      self
    }

    @inline def addUnsignedLong(num: Long): SliceMut[Byte] = {
      ScalaByteOps.writeUnsignedLong(num, self)
      self
    }

    @inline def addSignedLong(num: Long): SliceMut[Byte] = {
      ScalaByteOps.writeSignedLong(num, self)
      self
    }

    @inline def addString(string: String, charsets: Charset = StandardCharsets.UTF_8): SliceMut[Byte] = {
      ScalaByteOps.writeString(string, self, charsets)
      self
    }

    @inline def addStringUTF8(string: String): SliceMut[Byte] = {
      ScalaByteOps.writeString(string, self, StandardCharsets.UTF_8)
      self
    }

    @inline def addStringUTF8WithSize(string: String): SliceMut[Byte] = {
      ScalaByteOps.writeStringWithSize(string, self, StandardCharsets.UTF_8)
      self
    }

    @inline def createReader(): SliceReader =
      SliceReader(self)
  }
}
