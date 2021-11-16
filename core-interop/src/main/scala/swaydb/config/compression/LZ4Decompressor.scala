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

package swaydb.config.compression

sealed trait LZ4Decompressor
object LZ4Decompressor {
  def fastDecompressor: LZ4Decompressor = Fast
  case object Fast extends LZ4Decompressor

  def safeDecompressor: LZ4Decompressor = Safe
  case object Safe extends LZ4Decompressor
}
