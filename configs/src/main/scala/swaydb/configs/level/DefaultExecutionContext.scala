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

package swaydb.configs.level

import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.Executors
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
  def compactionEC(maxThreads: Int): ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(maxThreads max 1, DefaultThreadFactory.create()))

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
  lazy val sweeperEC: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, DefaultThreadFactory.create()))

}
