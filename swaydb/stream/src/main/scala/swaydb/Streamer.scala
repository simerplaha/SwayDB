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

package swaydb

/**
 * An Iterator like implementation that can be used to build [[Stream]]s
 * from other streaming libraries.
 *
 * This trait can be used to create async or sync streams.
 *
 * @see [[Stream.streamer]] to create this object from a SwayDB stream
 *      which can then be converted into other stream from other Streaming
 *      libraries.
 */
trait Streamer[A, BAG[_]] {
  def nextOrNull: BAG[A]
  def nextOption: BAG[Option[A]]
}
