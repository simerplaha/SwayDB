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
   * it will be used for all SwayDB instances that use default ExecutionContext.
   *
   * You can overwrite this when creating your SwayDB instance.
   */
  def compactionEC: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor(DefaultThreadFactory.create()))

  /**
   * ExecutionContext used for [[swaydb.data.config.FileCache]] and [[swaydb.data.config.MemoryCache]] Actors.
   *
   * You can overwrite this by provided your own [[swaydb.data.config.FileCache]] and [[swaydb.data.config.MemoryCache]]
   * configurations.
   */
  def sweeperEC: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor(DefaultThreadFactory.create()))

}
