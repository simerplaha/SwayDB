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

package swaydb.core.io.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.format.a.block.Block

/**
  * A typed object that indicates that block is already decompressed and now is reading data bytes.
  *
  * [[Block.createBlockDataReader]] creates the object and should be the only function that creates it.
  */

private[core] class BlockDataReader[B <: Block](parentBlockReader: BlockReader[_],
                                                parentBlock: Block,
                                                override val block: B) extends BlockReader(parentBlockReader, block) with LazyLogging
