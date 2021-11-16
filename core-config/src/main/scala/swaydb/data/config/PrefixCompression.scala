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

sealed trait PrefixCompression {
  private[swaydb] def shouldCompress(interval: Int): Boolean
  def enabled: Boolean
  def keysOnly: Boolean
  def normaliseIndexForBinarySearch: Boolean
}

object PrefixCompression {
  case class Off(normaliseIndexForBinarySearch: Boolean) extends PrefixCompression {
    override def keysOnly = false

    override def enabled: Boolean = false

    private[swaydb] override def shouldCompress(interval: Int): Boolean = false
  }

  sealed trait Interval {
    private[swaydb] def shouldCompress(index: Int): Boolean
  }

  def resetCompressionAt(indexInterval: Int): Interval =
    Interval.ResetCompressionAt(indexInterval)

  def compressAt(indexInterval: Int): Interval =
    Interval.CompressAt(indexInterval)

  object Interval {

    case class ResetCompressionAt(indexInterval: Int) extends Interval {
      private[swaydb] override def shouldCompress(index: Int): Boolean =
        indexInterval != 0 && index % this.indexInterval != 0
    }

    case class CompressAt(indexInterval: Int) extends Interval {
      private[swaydb] override def shouldCompress(index: Int): Boolean =
        indexInterval != 0 && index % this.indexInterval == 0
    }
  }

  case class On(keysOnly: Boolean, interval: Interval) extends PrefixCompression {
    override def normaliseIndexForBinarySearch: Boolean = false

    override def enabled: Boolean = true

    private[swaydb] override def shouldCompress(interval: Int): Boolean =
      this.interval.shouldCompress(interval)
  }
}
