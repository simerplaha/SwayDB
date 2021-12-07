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

package swaydb.slice.order

import swaydb.slice.Slice
import swaydb.slice.utils.ByteSlice

private[swaydb] object TimeOrder {
  val long = new TimeOrder[Slice[Byte]] {
    override def compare(left: Slice[Byte], right: Slice[Byte]): Int =
      if (left.isEmpty || right.isEmpty)
        1 //if either of them are empty then favour left to be the largest.
      else
        ByteSlice.readUnsignedInt(left) compare ByteSlice.readUnsignedInt(right)
  }
}

private[swaydb] trait TimeOrder[T] extends Ordering[T]
