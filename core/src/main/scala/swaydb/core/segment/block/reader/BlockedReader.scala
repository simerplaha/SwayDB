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

package swaydb.core.segment.block.reader

import swaydb.core.file.reader.Reader
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockOffset}
import swaydb.core.segment.block.{Block, BlockCacheState, BlockOffset, BlockOps}
import swaydb.slice.utils.ByteOps
import swaydb.slice.{Reader, Slice}

private[core] object BlockedReader {

  def apply[O <: BlockOffset, B <: Block[O]](block: B,
                                             bytes: Slice[Byte]): BlockedReader[O, B] =
    new BlockedReader[O, B](
      reader = Reader(bytes),
      blockCache = None,
      rootBlockRefOffset = block.offset,
      block = block
    )

  def apply[O <: BlockOffset, B <: Block[O]](ref: BlockRefReader[O])(implicit blockOps: BlockOps[O, B]): BlockedReader[O, B] = {
    val header = Block.readHeader(ref)
    val block = blockOps.readBlock(header)
    new BlockedReader[O, B](
      reader = ref.reader,
      rootBlockRefOffset = ref.rootBlockRefOffset,
      blockCache = ref.blockCache,
      block = block
    )
  }

  def apply[O <: BlockOffset, B <: Block[O]](block: B, reader: UnblockedReader[SegmentBlockOffset, SegmentBlock]): BlockedReader[O, B] =
    new BlockedReader[O, B](
      reader = reader.reader,
      rootBlockRefOffset = reader.rootBlockRefOffset,
      blockCache = reader.blockCache,
      block = block
    )
}

private[core] class BlockedReader[O <: BlockOffset, B <: Block[O]] private(private[reader] val reader: Reader[Byte],
                                                                           val rootBlockRefOffset: BlockOffset,
                                                                           val blockCache: Option[BlockCacheState],
                                                                           val block: B)(implicit val byteOps: ByteOps[Byte]) extends BlockReaderBase {

  def offset: O =
    block.offset

  override def moveTo(newPosition: Int): BlockedReader[O, B] = {
    super.moveTo(newPosition)
    this
  }

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, B]): BlockedReader[O, B] = {
    val bytes = readFullBlock()
    BlockedReader[O, B](
      bytes = bytes,
      block = blockOps.updateBlockOffset(block, 0, bytes.size)
    )
  }

  override def copy(): BlockedReader[O, B] =
    new BlockedReader(
      reader = reader.copy(),
      blockCache = blockCache,
      rootBlockRefOffset = rootBlockRefOffset,
      block = block
    )
}
