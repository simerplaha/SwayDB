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
import swaydb.core.segment.{Segment, SegmentSerialiser}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice

private[core] object AppendixLogEntryWriter {

  implicit object AppendixRemoveWriter extends LogEntryWriter[LogEntry.Remove[Slice[Byte]]] {
    val id: Int = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Remove[Slice[Byte]], bytes: Slice[Byte]): Unit =
      bytes
        .addUnsignedInt(this.id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: LogEntry.Remove[Slice[Byte]]): Int =
      Bytes.sizeOfUnsignedInt(this.id) +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size
  }

  implicit object AppendixPutWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Segment]] {
    val id: Int = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Put[Slice[Byte], Segment], bytes: Slice[Byte]): Unit =
      SegmentSerialiser.FormatA.write(
        segment = entry.value,
        bytes = bytes.addUnsignedInt(this.id)
      )

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Segment]): Int =
      Bytes.sizeOfUnsignedInt(this.id) +
        SegmentSerialiser.FormatA.bytesRequired(entry.value)
  }
}
