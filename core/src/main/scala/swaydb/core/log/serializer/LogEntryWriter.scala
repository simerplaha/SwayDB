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
import swaydb.data.slice.SliceMut

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for LogEntryWriter of type ${T}")
trait LogEntryWriter[T <: LogEntry[_, _]] {
  def write(entry: T, bytes: SliceMut[Byte]): Unit

  def bytesRequired(entry: T): Int

  val isRange: Boolean

  val isUpdate: Boolean
}

object LogEntryWriter {

  def write[T <: LogEntry[_, _]](entry: T, bytes: SliceMut[Byte])(implicit serializer: LogEntryWriter[T]): Unit =
    serializer.write(entry, bytes)

  def bytesRequired[T <: LogEntry[_, _]](entry: T)(implicit serializer: LogEntryWriter[T]): Int =
    serializer.bytesRequired(entry)
}
