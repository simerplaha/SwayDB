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

sealed trait BlockIO {
  def cacheOnAccess: Boolean
}
object BlockIO {

  /**
    * The default [[BlockIO]] strategy used for all [[BlockStatus.CompressedBlock]]
    * or [[BlockStatus.UncompressedBlock]] blocks.
    */
  val defaultSynchronised: BlockStatus => BlockIO.SynchronisedIO =
    (blockStatus: BlockStatus) =>
      BlockIO.SynchronisedIO(cacheOnAccess = blockStatus.isCompressed)

  /**
    * The default [[BlockIO]] strategy used for all [[BlockStatus.BlockInfo]].
    * BlockInfos are never individually unless the entire Segment is compressed.
    */
  val defaultBlockInfo =
    BlockIO.ConcurrentIO(true)

  val defaultBlockReaders =
    BlockIO.ConcurrentIO(false)

  case class ConcurrentIO(cacheOnAccess: Boolean) extends BlockIO
  case class SynchronisedIO(cacheOnAccess: Boolean) extends BlockIO
  case class ReservedIO(cacheOnAccess: Boolean) extends BlockIO
}
