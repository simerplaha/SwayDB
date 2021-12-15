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

sealed trait OptimiseWrites

case object OptimiseWrites {

  val randomOrder: OptimiseWrites.RandomOrder =
    RandomOrder

  def sequentialOrder(initialSkipListLength: Int): OptimiseWrites.SequentialOrder =
    SequentialOrder(initialSkipListLength = initialSkipListLength)

  /**
   * Always use this setting if writes are in random order or when unsure.
   *
   */
  sealed trait RandomOrder extends OptimiseWrites
  case object RandomOrder extends RandomOrder

  /**
   * Optimises writes for sequential order. Eg: if you inserts are simple
   * sequential put eg - 1, 2, 3 ... N with [[swaydb.slice.order.KeyOrder.integer]].
   * Then this setting would increase write throughput.
   *
   * @param initialSkipListLength set the initial length of SkipList's Array.
   *                              The Array is extended if the size is too small.
   */
  case class SequentialOrder(initialSkipListLength: Int) extends OptimiseWrites

}
