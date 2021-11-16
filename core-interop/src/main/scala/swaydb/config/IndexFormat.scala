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

package swaydb.config

sealed trait IndexFormat
object IndexFormat {
  /**
   * Stores a reference to the position of a the entire key-value entry within the sorted index.
   *
   * This configuration requires a maximum of 1 to 5 bytes and is space efficient but might be
   * slower then [[ReferenceKey]] and [[CopyKey]].
   */
  def reference: IndexFormat.Reference = Reference
  sealed trait Reference extends IndexFormat
  object Reference extends Reference

  /**
   * In addition to information stored by [[Reference]] this also stores a copy of the key within the index itself.
   *
   * In addition to space required by [[ReferenceKey]] this requires additional space to store the key.
   * This config increases read and compaction performance since as it reduces the amount
   * parsed data to fetch the stored key and also reduces CPU and IO.
   *
   * Fastest config.
   */
  def copyKey: IndexFormat.CopyKey = CopyKey
  sealed trait CopyKey extends IndexFormat
  object CopyKey extends CopyKey
}
