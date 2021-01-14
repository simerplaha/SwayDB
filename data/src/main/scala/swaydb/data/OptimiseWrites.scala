/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data

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
   * sequential put eg - 1, 2, 3 ... N with [[swaydb.data.order.KeyOrder.integer]].
   * Then this setting would increase write throughput.
   *
   * @param initialSkipListLength set the initial length of SkipList's Array.
   *                              The Array is extended if the size is too small.
   */
  case class SequentialOrder(initialSkipListLength: Int) extends OptimiseWrites

}
