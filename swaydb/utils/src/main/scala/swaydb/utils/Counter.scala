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

private[swaydb] sealed trait Counter[T] {
  def incrementAndGet(): T

  def decrementAndGet(): T

  def get(): T
}

private[swaydb] object Counter {

  def forInt(start: Int): IntCounter =
    new IntCounter(start)

  class IntCounter(private var value: Int) extends Counter[Int] {
    override def incrementAndGet(): Int = {
      value += 1
      value
    }

    override def decrementAndGet(): Int = {
      value -= 1
      value
    }

    override def get(): Int =
      value
  }

  /**
   * Maintains a count of number of requests for an item.
   */
  object Request {
    def apply[T](item: T, start: Int): Request[T] =
      new Request(item, Counter.forInt(start))
  }

  class Request[+T](val item: T, val counter: Counter.IntCounter)
}
