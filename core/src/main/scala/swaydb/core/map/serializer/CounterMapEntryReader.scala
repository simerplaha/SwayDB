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

private[swaydb] object CounterMapEntryReader {

  implicit object CounterPutMapEntryReader extends MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]] {
    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Slice[Byte]] = {
      val _ = reader.get()
      val keySize = reader.readUnsignedInt()

      val key =
        if (keySize == 0)
          Slice.emptyBytes
        else
          reader.read(keySize)

      val valueSize = reader.readUnsignedInt()

      val value =
        if (valueSize == 0)
          Slice.emptyBytes
        else
          reader.read(valueSize)

      MapEntry.Put(key, value)(CounterMapEntryWriter.CounterPutMapEntryWriter)
    }
  }
}
