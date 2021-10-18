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

package swaydb.java

import swaydb.Glass

object Source {

  @inline def apply[K, T](scalaSource: => swaydb.Source[K, T, Glass]): Source[K, T] =
    new Source[K, T] {
      override def asScalaStream: swaydb.Source[K, T, Glass] =
        scalaSource
    }

}

trait Source[K, T] extends Stream[T] {

  override def asScalaStream: swaydb.Source[K, T, Glass]

  def from(key: K): Source[K, T] =
    Source(asScalaStream.from(key))

  def before(key: K): Source[K, T] =
    Source(asScalaStream.before(key))

  def fromOrBefore(key: K): Source[K, T] =
    Source(asScalaStream.fromOrBefore(key))

  def after(key: K): Source[K, T] =
    Source(asScalaStream.after(key))

  def fromOrAfter(key: K): Source[K, T] =
    Source(asScalaStream.fromOrAfter(key))

  def reverse: Source[K, T] =
    Source(asScalaStream.reverse)

  def stream: Source[K, T] =
    this
}
