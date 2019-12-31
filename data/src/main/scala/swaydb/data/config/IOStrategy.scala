/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

sealed trait IOStrategy {
  def cacheOnAccess: Boolean
  def forceCacheOnAccess: IOStrategy
}
object IOStrategy {

  /**
   * The default [[IOStrategy]] strategy used for all [[IOAction.ReadCompressedData]]
   * or [[IOAction.ReadUncompressedData]] blocks.
   */
  val synchronisedStoredIfCompressed: IOAction => IOStrategy.SynchronisedIO =
    (dataType: IOAction) =>
      IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed)

  val synchronisedStored: IOAction => IOStrategy.SynchronisedIO =
    (_: IOAction) =>
      IOStrategy.SynchronisedIO(cacheOnAccess = true)

  val concurrentStoredIfCompressed: IOAction => IOStrategy.SynchronisedIO =
    (dataType: IOAction) =>
      IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed)

  val concurrentStored: IOAction => IOStrategy.SynchronisedIO =
    (_: IOAction) =>
      IOStrategy.SynchronisedIO(cacheOnAccess = true)

  val reserved: IOAction => IOStrategy.AsyncIO =
    (_: IOAction) =>
      IOStrategy.AsyncIO(cacheOnAccess = true)

  /**
   * The default [[IOStrategy]] strategy used for all [[IOAction.ReadDataOverview]].
   * BlockInfos are never individually unless the entire Segment is compressed.
   */
  val defaultBlockInfoStored =
    IOStrategy.ConcurrentIO(true)

  val defaultBlockReadersStored =
    IOStrategy.ConcurrentIO(true)

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
