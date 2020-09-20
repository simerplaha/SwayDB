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
   * ExecutionContext used for Compaction. This is a lazy val so once initialised
   * it will be used for all SwayDB instances that use default ExecutionContext. If you
   * are using multiple SwayDB instance create these fixed ThreadPool for each instance.
   *
   * You can overwrite this when creating your SwayDB instance.
   *
   * Needs at least 2 threads. 1 for compaction because default configuration uses
   * single threaded compaction and another thread for scheduling (if required).
   */
  lazy val compactionEC: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, DefaultThreadFactory.create()))

  /**
   * ExecutionContext used for [[swaydb.data.config.FileCache]] and [[swaydb.data.config.MemoryCache]] Actors.
   *
   * You can overwrite this by provided your own [[swaydb.data.config.FileCache]] and [[swaydb.data.config.MemoryCache]]
   * configurations.
   *
   * Needs at least 3 threads. 1 for processing, 1 scheduling and 1 for termination (during shutdown)
   * when [[swaydb.Bag.Sync]] bag is used. If the API is using [[swaydb.Bag.Async]] bag then only 2
   * threads are needed.
   */
  lazy val sweeperEC: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3, DefaultThreadFactory.create()))

}
