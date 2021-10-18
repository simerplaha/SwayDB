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

package swaydb.core.map.serializer

import swaydb.core.map.MapEntry
import swaydb.data.slice.{ReaderBase, Slice}

private[swaydb] object AppliedFunctionsMapEntryReader {

  object FunctionsPutMapEntryReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Slice.Null.type]] {
    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Slice.Null.type] =
      MapEntry.Put(reader.read(reader.readUnsignedInt()), Slice.Null)(AppliedFunctionsMapEntryWriter.FunctionsPutMapEntryWriter)
  }

  object FunctionsRemoveMapEntryReader extends MapEntryReader[MapEntry.Remove[Slice[Byte]]] {
    override def read(reader: ReaderBase[Byte]): MapEntry.Remove[Slice[Byte]] =
      MapEntry.Remove(reader.read(reader.readUnsignedInt()))(AppliedFunctionsMapEntryWriter.FunctionsRemoveMapEntryWriter)
  }

  implicit object FunctionsMapEntryReader extends MapEntryReader[MapEntry[Slice[Byte], Slice.Null.type]] {
    override def read(reader: ReaderBase[Byte]): MapEntry[Slice[Byte], Slice.Null.type] =
      reader.foldLeft(null: MapEntry[Slice[Byte], Slice.Null.type]) {
        case (previousEntryOrNull, reader) =>
          val entryId = reader.readUnsignedInt()
          if (entryId == AppliedFunctionsMapEntryWriter.FunctionsPutMapEntryWriter.id) {
            val nextEntry = FunctionsPutMapEntryReader.read(reader)
            if (previousEntryOrNull == null)
              nextEntry
            else
              previousEntryOrNull ++ nextEntry
          } else if (entryId == AppliedFunctionsMapEntryWriter.FunctionsRemoveMapEntryWriter.id) {
            val nextEntry = FunctionsPutMapEntryReader.read(reader)
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
