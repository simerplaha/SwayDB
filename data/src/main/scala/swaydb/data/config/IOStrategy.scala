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
 */
package swaydb.data.config
import swaydb.data.util.Java.JavaFunction

import scala.compat.java8.FunctionConverters._

sealed trait IOStrategy {
  def cacheOnAccess: Boolean
  def forceCacheOnAccess: IOStrategy
}
object IOStrategy {

  /**
   * The default [[IOStrategy]] strategy used for all [[IOAction.ReadCompressedData]]
   * or [[IOAction.ReadUncompressedData]] blocks.
   */

  def defaultSynchronised: IOAction => IOStrategy.SynchronisedIO = {
    case IOAction.OpenResource =>
      IOStrategy.SynchronisedIO(cacheOnAccess = true)

    case IOAction.ReadDataOverview =>
      IOStrategy.SynchronisedIO(cacheOnAccess = true)

    case action: IOAction.DataAction =>
      IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
  }

  def defaultSynchronisedJava: JavaFunction[IOAction, SynchronisedIO] =
    defaultSynchronised.asJava

  def defaultConcurrent: IOAction => IOStrategy.ConcurrentIO = {
    case IOAction.OpenResource =>
      IOStrategy.ConcurrentIO(cacheOnAccess = true)

    case IOAction.ReadDataOverview =>
      IOStrategy.ConcurrentIO(cacheOnAccess = true)

    case action: IOAction.DataAction =>
      IOStrategy.ConcurrentIO(cacheOnAccess = action.isCompressed)
  }

  def defaultConcurrentJava: JavaFunction[IOAction, IOStrategy.ConcurrentIO] =
    defaultConcurrent.asJava

  case class ConcurrentIO(cacheOnAccess: Boolean) extends IOStrategy {
    def forceCacheOnAccess: ConcurrentIO =
      copy(cacheOnAccess = true)
  }
  case class SynchronisedIO(cacheOnAccess: Boolean) extends IOStrategy {
    def forceCacheOnAccess: SynchronisedIO =
      copy(cacheOnAccess = true)
  }
  case class AsyncIO(cacheOnAccess: Boolean) extends IOStrategy {
    def forceCacheOnAccess: AsyncIO =
      copy(cacheOnAccess = true)
  }
}
