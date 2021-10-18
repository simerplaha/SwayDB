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

import swaydb.stream.SourceFree

object Source {
  @inline def apply[K, T, BAG[_]](nextFree: => SourceFree[K, T])(implicit bag: Bag[BAG]): Source[K, T, BAG] =
    new Source[K, T, BAG] {
      private[swaydb] override def free: SourceFree[K, T] =
        nextFree
    }
}

/**
 * [[Source]] carries the [[BAG]] information at the time of creation whereas [[SourceFree]] requires
 * [[BAG]] at the time of execution.
 */
abstract class Source[K, T, BAG[_]](implicit bag: Bag[BAG]) extends Stream[T, BAG] {

  private[swaydb] def free: SourceFree[K, T]

  def from(key: K): Source[K, T, BAG] =
    Source(free.from(key))

  def before(key: K): Source[K, T, BAG] =
    Source(free.before(key))

  def fromOrBefore(key: K): Source[K, T, BAG] =
    Source(free.fromOrBefore(key))

  def after(key: K): Source[K, T, BAG] =
    Source(free.after(key))

  def fromOrAfter(key: K): Source[K, T, BAG] =
    Source(free.fromOrAfter(key))

  def reverse: Source[K, T, BAG] =
    Source(free.reverse)

  def stream: Source[K, T, BAG] =
    this

  private[swaydb] def transformValue[B](f: T => B): Source[K, B, BAG] =
    Source(free.transformValue(f))
}
