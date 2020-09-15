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
