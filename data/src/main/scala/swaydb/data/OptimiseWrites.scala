/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data

sealed trait OptimiseWrites {
  def transactionQueueMaxSize: Int
}

case object OptimiseWrites {

  /**
   * Always use this setting if writes are in random order or when unsure.
   *
   * @param transactionQueueMaxSize sets the max number of pending transactions
   *                                before they are merged with other transactions.
   */
  case class RandomOrder(transactionQueueMaxSize: Int) extends OptimiseWrites

  /**
   * Optimises writes for sequential order. Eg: if you insert are simple
   * sequential put eg - 1, 2, 3 ... N with [[swaydb.data.order.KeyOrder.integer]].
   * Then this setting would yield faster write throughput.
   *
   * @param transactionQueueMaxSize sets the max number of pending transactions
   *                                before they are merged with other transactions.
   * @param initialSkipListLength   set the initial length of SkipList's Array.
   *                                The Array is extended if the size is too small.
   */
  case class SequentialOrder(transactionQueueMaxSize: Int,
                             initialSkipListLength: Int) extends OptimiseWrites

}
