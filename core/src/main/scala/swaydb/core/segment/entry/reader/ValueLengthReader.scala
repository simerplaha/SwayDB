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

@implicitNotFound("Type class implementation not found for ValueLengthReader of type ${T}")
sealed trait ValueLengthReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: ReaderBase[Byte],
           previous: PersistentOption): Int
}

object ValueLengthReader {

  private def readLength(indexReader: ReaderBase[Byte],
                         previous: PersistentOption,
                         commonBytes: Int): Int =
    previous match {
      case previous: Persistent =>
        Bytes.decompress(
          previous = Slice.writeInt[Byte](previous.valueLength),
          next = indexReader.read(ByteSizeOf.int - commonBytes),
          commonBytes = commonBytes
        ).readInt()

      case Persistent.Null =>
        throw EntryReaderFailure.NoPreviousKeyValue
    }

  implicit object ValueLengthOneCompressed extends ValueLengthReader[BaseEntryId.ValueLength.OneCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      readLength(indexReader, previous, 1)
  }

  implicit object ValueLengthTwoCompressed extends ValueLengthReader[BaseEntryId.ValueLength.TwoCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      readLength(indexReader, previous, 2)
  }

  implicit object ValueLengthThreeCompressed extends ValueLengthReader[BaseEntryId.ValueLength.ThreeCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      readLength(indexReader, previous, 3)
  }

  implicit object ValueLengthFullyCompressed extends ValueLengthReader[BaseEntryId.ValueLength.FullyCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      previous match {
        case previous: Persistent =>
          previous.valueLength

        case Persistent.Null =>
          throw EntryReaderFailure.NoPreviousKeyValue
      }
  }

  implicit object ValueLengthUncompressed extends ValueLengthReader[BaseEntryId.ValueLength.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      indexReader.readUnsignedInt()
  }

  implicit object NoValue extends ValueLengthReader[BaseEntryId.Value.NoValue] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Int =
      0
  }
}
