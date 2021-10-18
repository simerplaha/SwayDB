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

package swaydb.utils

private[swaydb] object ByteSizeOf {
  @inline val byte = java.lang.Byte.BYTES
  @inline val short = java.lang.Short.BYTES
  @inline val int = java.lang.Integer.BYTES
  @inline val varInt = int + 1 //5
  @inline val long = java.lang.Long.BYTES
  @inline val varLong = long + 2 //10
  @inline val boolean = java.lang.Byte.BYTES
  @inline val char = java.lang.Character.BYTES
  @inline val double = java.lang.Double.BYTES
  @inline val float = java.lang.Float.BYTES
}
