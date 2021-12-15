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
import swaydb.core.util.Bytes
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.ByteSizeOf

private[swaydb] object AppliedFunctionsLogEntryWriter {

  implicit object FunctionsPutLogEntryWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Slice.Null.type]] {
    val id: Byte = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Put[Slice[Byte], Slice.Null.type], bytes: SliceMut[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Slice.Null.type]): Int =
      ByteSizeOf.byte +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size
  }

  implicit object FunctionsRemoveLogEntryWriter extends LogEntryWriter[LogEntry.Remove[Slice[Byte]]] {
    val id: Byte = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Remove[Slice[Byte]], bytes: SliceMut[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: LogEntry.Remove[Slice[Byte]]): Int =
      ByteSizeOf.byte +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size
  }
}
