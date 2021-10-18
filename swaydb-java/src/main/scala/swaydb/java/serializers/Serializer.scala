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

package swaydb.java.serializers

import swaydb.data.slice.Slice

trait Serializer[T] {

  /**
   * You can also use ByteSliceBuilder to build
   * custom serialisation.
   */
  def write(data: T): Slice[java.lang.Byte]

  /**
   * A Slice is a section of Segment's byte array so this
   * Slice could be directly coming from a Segment.
   *
   * Do not mutate this byte array, just read its content build your object.
   */
  def read(slice: Slice[java.lang.Byte]): T

}
