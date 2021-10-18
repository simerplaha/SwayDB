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

package swaydb.data.config

/**
 * Each thread is assigned a state. This config indicates if
 * that state should be limited or unlimited.
 *
 * A single thread can spawn reads of over 100s of segments.
 * For eg: performing forward and reverse iterations over millions of keys
 * could spread over multiple segments. These iterators cannot use bloomFilter since
 * bloomFilters only do exists check on a key. This states are used for skipping
 * reading a segment if it's not required.
 */
sealed trait ThreadStateCache
object ThreadStateCache {

  case class Limit(hashMapMaxSize: Int,
                   maxProbe: Int) extends ThreadStateCache

  def noLimit: ThreadStateCache.NoLimit = NoLimit
  sealed trait NoLimit extends ThreadStateCache
  case object NoLimit extends NoLimit

  /**
   * Disabling ThreadState can be used if your database configuration
   * allows for perfect HashIndexes and if you do not use iterations.
   * Otherwise disabling [[ThreadStateCache]] can have noticable performance impact.
   */

  def off: ThreadStateCache.Off = Off
  sealed trait Off extends ThreadStateCache
  case object Off extends Off
}
