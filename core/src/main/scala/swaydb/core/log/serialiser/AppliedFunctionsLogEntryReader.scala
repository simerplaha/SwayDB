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

package swaydb.core.log.serialiser

import swaydb.core.log.LogEntry
import swaydb.slice.{ReaderBase, Slice}

private[swaydb] object AppliedFunctionsLogEntryReader {

  object FunctionsPutLogEntryReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Slice.Null.type]] {
    override def read(reader: ReaderBase): LogEntry.Put[Slice[Byte], Slice.Null.type] =
      LogEntry.Put(reader.read(reader.readUnsignedInt()), Slice.Null)(AppliedFunctionsLogEntryWriter.FunctionsPutLogEntryWriter)
  }

  object FunctionsRemoveLogEntryReader extends LogEntryReader[LogEntry.Remove[Slice[Byte]]] {
    override def read(reader: ReaderBase): LogEntry.Remove[Slice[Byte]] =
      LogEntry.Remove(reader.read(reader.readUnsignedInt()))(AppliedFunctionsLogEntryWriter.FunctionsRemoveLogEntryWriter)
  }

  implicit object FunctionsLogEntryReader extends LogEntryReader[LogEntry[Slice[Byte], Slice.Null.type]] {
    override def read(reader: ReaderBase): LogEntry[Slice[Byte], Slice.Null.type] =
      reader.foldLeft(null: LogEntry[Slice[Byte], Slice.Null.type]) {
        case (previousEntryOrNull, reader) =>
          val entryId = reader.readUnsignedInt()
          if (entryId == AppliedFunctionsLogEntryWriter.FunctionsPutLogEntryWriter.id) {
            val nextEntry = FunctionsPutLogEntryReader.read(reader)
            if (previousEntryOrNull == null)
              nextEntry
            else
              previousEntryOrNull ++ nextEntry
          } else if (entryId == AppliedFunctionsLogEntryWriter.FunctionsRemoveLogEntryWriter.id) {
            val nextEntry = FunctionsPutLogEntryReader.read(reader)
            if (previousEntryOrNull == null)
              nextEntry
            else
              previousEntryOrNull ++ nextEntry
          } else {
            throw new IllegalArgumentException(s"Invalid Functions entry id - $entryId")
          }
      }
  }
}
