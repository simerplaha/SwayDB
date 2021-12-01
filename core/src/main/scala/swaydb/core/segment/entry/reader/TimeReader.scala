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

import swaydb.core.segment.data.{Persistent, PersistentOption, Time}
import swaydb.core.segment.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.slice.ReaderBase

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for TimeReader of type ${T}")
sealed trait TimeReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: ReaderBase,
           previous: PersistentOption): Time
}

/**
 * Time is always set for only Fixed key-values.
 * Group and Range key-values do not have time set.
 */
object TimeReader {

  implicit object NoTimeReader extends TimeReader[BaseEntryId.Time.NoTime] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase,
                      previous: PersistentOption): Time =
      Time.empty
  }

  implicit object UnCompressedTimeReader extends TimeReader[BaseEntryId.Time.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase,
                      previous: PersistentOption): Time = {
      val timeSize = indexReader.readUnsignedInt()
      val time = indexReader.read(timeSize)
      Time(time)
    }
  }

  implicit object PartiallyCompressedTimeReader extends TimeReader[BaseEntryId.Time.PartiallyCompressed] {

    override def isPrefixCompressed: Boolean = true

    def readTime(indexReader: ReaderBase,
                 previousTime: Time): Time = {
      val commonBytes = indexReader.readUnsignedInt()
      val uncompressedBytes = indexReader.readUnsignedInt()
      val rightBytes = indexReader.read(uncompressedBytes)
      val timeBytes = Bytes.decompress(previousTime.time, rightBytes, commonBytes)
      Time(timeBytes)
    }

    override def read(indexReader: ReaderBase,
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
