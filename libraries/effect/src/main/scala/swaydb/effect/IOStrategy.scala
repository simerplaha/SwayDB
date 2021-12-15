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
package swaydb.effect

import swaydb.utils.Java.JavaFunction

import scala.compat.java8.FunctionConverters._

sealed trait IOStrategy {
  def cacheOnAccess: Boolean
  def forceCacheOnAccess: IOStrategy
}

object IOStrategy {

  /**
   * Allows for only a single Thread to access a resource.
   * This can be synchronous [[IOStrategy.SynchronisedIO]] or
   * asynchronous [[IOStrategy.AsyncIO]].
   *
   * [[IOStrategy.ConcurrentIO]] is used for files that are already
   * open and are being concurrently being read either via in-memory cached
   * bytes or directly via disk IO.
   */
  sealed trait ThreadSafe extends IOStrategy

  /**
   * The default [[IOStrategy]] strategy used for all [[IOAction.ReadCompressedData]]
   * or [[IOAction.ReadUncompressedData]] blocks.
   */

  def defaultSynchronised: IOAction => IOStrategy.SynchronisedIO = {
    case IOAction.ReadDataOverview =>
      IOStrategy.SynchronisedIO(cacheOnAccess = true)

    case action: IOAction.DecompressAction =>
      IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
  }

  def defaultSynchronisedJava: JavaFunction[IOAction, SynchronisedIO] =
    defaultSynchronised.asJava

  def defaultConcurrent: IOAction => IOStrategy.ConcurrentIO = {
    case IOAction.ReadDataOverview =>
      IOStrategy.ConcurrentIO(cacheOnAccess = true)

    case action: IOAction.DecompressAction =>
      IOStrategy.ConcurrentIO(cacheOnAccess = action.isCompressed)
  }

  def defaultConcurrentJava: JavaFunction[IOAction, IOStrategy.ConcurrentIO] =
    defaultConcurrent.asJava

  case class ConcurrentIO(cacheOnAccess: Boolean) extends IOStrategy {
    def forceCacheOnAccess: ConcurrentIO =
      copy(cacheOnAccess = true)
  }

  object SynchronisedIO {
    val cached = SynchronisedIO(cacheOnAccess = true)
  }

  case class SynchronisedIO(cacheOnAccess: Boolean) extends ThreadSafe {
    def forceCacheOnAccess: SynchronisedIO =
      copy(cacheOnAccess = true)
  }

  case class AsyncIO(cacheOnAccess: Boolean) extends ThreadSafe {
    def forceCacheOnAccess: AsyncIO =
      copy(cacheOnAccess = true)
  }
}
