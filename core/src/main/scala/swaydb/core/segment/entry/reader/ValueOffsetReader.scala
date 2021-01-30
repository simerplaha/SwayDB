/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.entry.reader

import swaydb.core.data.{Persistent, PersistentOption}
import swaydb.core.segment.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.utils.ByteSizeOf

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for ValueOffsetReader of type ${T}")
sealed trait ValueOffsetReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: ReaderBase[Byte],
           previous: PersistentOption): Int
}

object ValueOffsetReader {

  private def readOffset(indexReader: ReaderBase[Byte],
                         previous: PersistentOption,
                         commonBytes: Int): Int =
    previous match {
      case previous: Persistent =>
        Bytes.decompress(
          previous = Slice.writeInt[Byte](previous.valueOffset),
          next = indexReader.read(ByteSizeOf.int - commonBytes),
          commonBytes = commonBytes
        ).readInt()

      case Persistent.Null =>
        throw EntryReaderFailure.NoPreviousKeyValue
    }

  implicit object ValueOffsetOneCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.OneCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      readOffset(indexReader, previous, 1)
  }

  implicit object ValueOffsetTwoCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.TwoCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      readOffset(indexReader, previous, 2)
  }

  implicit object ValueOffsetThreeCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.ThreeCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      readOffset(indexReader, previous, 3)
  }

  implicit object ValueOffsetUncompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      indexReader.readUnsignedInt()
  }

  implicit object ValueOffsetReaderValueOffsetFullyCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.FullyCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      previous match {
        case previous: Persistent =>
          previous.valueOffset

        case Persistent.Null =>
          throw EntryReaderFailure.NoPreviousKeyValue
      }
  }

  implicit object ValueOffsetReaderNoValue extends ValueOffsetReader[BaseEntryId.Value.NoValue] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      0
  }
}
