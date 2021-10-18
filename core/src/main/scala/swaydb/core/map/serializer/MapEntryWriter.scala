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
import swaydb.data.slice.Slice

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for MapEntryWriter of type ${T}")
trait MapEntryWriter[T <: MapEntry[_, _]] {
  def write(entry: T, bytes: Slice[Byte]): Unit

  def bytesRequired(entry: T): Int

  val isRange: Boolean

  val isUpdate: Boolean
}

object MapEntryWriter {

  def write[T <: MapEntry[_, _]](entry: T, bytes: Slice[Byte])(implicit serializer: MapEntryWriter[T]): Unit =
    serializer.write(entry, bytes)

  def bytesRequired[T <: MapEntry[_, _]](entry: T)(implicit serializer: MapEntryWriter[T]): Int =
    serializer.bytesRequired(entry)
}
