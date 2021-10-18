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
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.utils.ByteSizeOf

private[swaydb] object AppliedFunctionsMapEntryWriter {

  implicit object FunctionsPutMapEntryWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Slice.Null.type]] {
    val id: Byte = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Slice.Null.type], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Slice.Null.type]): Int =
      ByteSizeOf.byte +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size
  }

  implicit object FunctionsRemoveMapEntryWriter extends MapEntryWriter[MapEntry.Remove[Slice[Byte]]] {
    val id: Byte = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Remove[Slice[Byte]], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Remove[Slice[Byte]]): Int =
      ByteSizeOf.byte +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size
  }
}
