/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
  def withCacheOnAccess: IOStrategy
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

  val reservedStoredIfCompressed: IOAction => IOStrategy.ReservedIO =
    (dataType: IOAction) =>
      IOStrategy.ReservedIO(cacheOnAccess = dataType.isCompressed)

  val reservedStored: IOAction => IOStrategy.ReservedIO =
    (_: IOAction) =>
      IOStrategy.ReservedIO(cacheOnAccess = true)

  val reservedNotStored: IOAction => IOStrategy.ReservedIO =
    (_: IOAction) =>
      IOStrategy.ReservedIO(cacheOnAccess = false)

  /**
   * The default [[IOStrategy]] strategy used for all [[IOAction.ReadDataOverview]].
   * BlockInfos are never individually unless the entire Segment is compressed.
   */
  val defaultBlockInfoStored =
    IOStrategy.ConcurrentIO(true)

  val defaultBlockReadersStored =
    IOStrategy.ConcurrentIO(true)

  case class ConcurrentIO(cacheOnAccess: Boolean) extends IOStrategy {
    def withCacheOnAccess: ConcurrentIO =
      copy(cacheOnAccess = true)
  }
  case class SynchronisedIO(cacheOnAccess: Boolean) extends IOStrategy {
    def withCacheOnAccess: SynchronisedIO =
      copy(cacheOnAccess = true)
  }
  case class ReservedIO(cacheOnAccess: Boolean) extends IOStrategy {
    def withCacheOnAccess: ReservedIO =
      copy(cacheOnAccess = true)
  }
}
