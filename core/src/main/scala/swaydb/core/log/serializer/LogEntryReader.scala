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

import swaydb.core.io.reader.Reader
import swaydb.core.log.LogEntry
import swaydb.slice.{ReaderBase, Slice}

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for LogEntryReader of type ${T}")
trait LogEntryReader[T <: LogEntry[_, _]] {
  def read(reader: ReaderBase[Byte]): T
}

object LogEntryReader {

  def read[T <: LogEntry[_, _]](bytes: Slice[Byte])(implicit serializer: LogEntryReader[T]): T =
    serializer.read(Reader(bytes))

  def read[T <: LogEntry[_, _]](reader: ReaderBase[Byte])(implicit serializer: LogEntryReader[T]): T =
    serializer.read(reader)
}
