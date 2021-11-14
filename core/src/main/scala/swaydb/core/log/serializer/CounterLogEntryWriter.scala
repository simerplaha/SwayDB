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

package swaydb.core.log.serializer

import swaydb.core.log.LogEntry
import swaydb.core.util.Bytes
import swaydb.data.slice.{Slice, SliceMut}
import swaydb.utils.ByteSizeOf

private[swaydb] object CounterLogEntryWriter {

  implicit object CounterPutLogEntryWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]] {
    val id: Byte = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Put[Slice[Byte], Slice[Byte]], bytes: SliceMut[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)
        .addUnsignedInt(entry.value.size)
        .addAll(entry.value)

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Slice[Byte]]): Int =
      ByteSizeOf.byte +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size +
        Bytes.sizeOfUnsignedInt(entry.value.size) +
        entry.value.size
  }
}
