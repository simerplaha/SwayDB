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

package swaydb.configs.level

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext

object DefaultExecutionContext extends LazyLogging {

  /**
   * A new compaction ExecutionContext is created for each new SwayDB instance.
   *
   * You can overwrite this when creating your SwayDB instance.
   *
   * Needs at least 1-2 threads. 1 for compaction because default configuration uses
   * single threaded compaction and another thread optionally for scheduling. For majority
   * of the cases only 1 thread will be active.
   */
  def compactionEC: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, DefaultThreadFactory.create()))

  /**
   * ExecutionContext used for [[swaydb.data.config.FileCache]] and [[swaydb.data.config.MemoryCache]].
   *
   * Initialised lazily and used globally for all SwayDB instances. Sweepers are cache/memory and
   * file(close, clean & delete) managers and their execution is needed at machine level so that
   * we do not hit memory overflow or run into too many open files. So this ExecutionContext is required
   * at machine level.
   *
   * If you have multiple SwayDB instances this CachedThreadPool should be enough as thread counts
   * will adjust dynamically or else you can overwrite this by provided your own [[swaydb.data.config.FileCache]]
   * and [[swaydb.data.config.MemoryCache]] configurations.
   *
   * For a single instance it needs at least 2-3 threads. 1 for processing, 1 scheduling and optionally 1
   * when [[swaydb.Bag.Sync]] bag is used for termination (during shutdown). But for majority of the cases only 1
   * thread will be active. If [[swaydb.Bag.Async]] bag is used then only 2 threads are needed per instance.
   */
  lazy val sweeperEC: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool(DefaultThreadFactory.create()))

}
