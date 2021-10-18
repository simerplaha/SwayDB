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
