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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.entry.reader

import swaydb.core.data.{Persistent, PersistentOption, Time}
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.data.slice.ReaderBase

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for TimeReader of type ${T}")
sealed trait TimeReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: ReaderBase[Byte],
           previous: PersistentOption): Time
}

/**
 * Time is always set for only Fixed key-values.
 * Group and Range key-values do not have time set.
 */
object TimeReader {

  implicit object NoTimeReader extends TimeReader[BaseEntryId.Time.NoTime] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Time =
      Time.empty
  }

  implicit object UnCompressedTimeReader extends TimeReader[BaseEntryId.Time.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Time = {
      val timeSize = indexReader.readUnsignedInt()
      val time = indexReader.read(timeSize)
      Time(time)
    }
  }

  implicit object PartiallyCompressedTimeReader extends TimeReader[BaseEntryId.Time.PartiallyCompressed] {

    override def isPrefixCompressed: Boolean = true

    def readTime(indexReader: ReaderBase[Byte],
                 previousTime: Time): Time = {
      val commonBytes = indexReader.readUnsignedInt()
      val uncompressedBytes = indexReader.readUnsignedInt()
      val rightBytes = indexReader.read(uncompressedBytes)
      val timeBytes = Bytes.decompress(previousTime.time, rightBytes, commonBytes)
      Time(timeBytes)
    }

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Time =
      previous match {
        case previous: Persistent =>
          previous match {
            case previous: Persistent.Put =>
              readTime(indexReader, previous.time)

            case previous: Persistent.Remove =>
              readTime(indexReader, previous.time)

            case previous: Persistent.Function =>
              readTime(indexReader, previous.time)

            case previous: Persistent.PendingApply =>
              readTime(indexReader, previous.time)

            case previous: Persistent.Update =>
              readTime(indexReader, previous.time)

            case _: Persistent.Range =>
              throw EntryReaderFailure.PreviousIsNotFixedKeyValue
          }

        case Persistent.Null =>
          throw EntryReaderFailure.NoPreviousKeyValue
      }
  }
}
