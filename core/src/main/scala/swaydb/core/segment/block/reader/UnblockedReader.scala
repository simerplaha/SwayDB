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

import swaydb.core.file.FileReader
import swaydb.core.segment.block.{Block, BlockCacheState, BlockOffset, BlockOps}
import swaydb.slice.{Reader, Slice, SliceReader}

/**
 * A typed object that indicates that block is already decompressed and now is reading data bytes.
 */

sealed trait UnblockedReaderOption[+O <: BlockOffset, +B <: Block[O]] {
  def isNone: Boolean
  def isSome: Boolean = !isNone
}

private[core] object UnblockedReader {

  final case object Null extends UnblockedReaderOption[Nothing, Nothing] {
    override def isNone: Boolean = true
  }

  def empty[O <: BlockOffset, B <: Block[O]](block: B)(implicit blockOps: BlockOps[O, B]) = {
    val emptyBlock = blockOps.updateBlockOffset(block = block, start = 0, size = 0)

    new UnblockedReader[O, B](
      block = emptyBlock,
      rootBlockRefOffset = emptyBlock.offset,
      blockCache = None,
      reader = SliceReader(Slice.emptyBytes)
    )
  }

  def apply[O <: BlockOffset, B <: Block[O]](block: B,
                                             bytes: Slice[Byte]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      block = block,
      rootBlockRefOffset = block.offset,
      blockCache = None,
      reader = SliceReader(bytes)
    )

  def moveTo[O <: BlockOffset, B <: Block[O]](offset: O,
                                              reader: UnblockedReader[O, B])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      block = blockOps.updateBlockOffset(reader.block, reader.offset.start + offset.start, offset.size),
      rootBlockRefOffset = reader.rootBlockRefOffset,
      blockCache = reader.blockCache,
      reader = reader.reader
    )

  def apply[O <: BlockOffset, B <: Block[O]](blockedReader: BlockedReader[O, B],
                                             readAllIfUncompressed: Boolean)(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    Block.unblock(
      reader = blockedReader,
      readAllIfUncompressed = readAllIfUncompressed
    )

  def fromUncompressed[O <: BlockOffset, B <: Block[O]](blockedReader: BlockedReader[O, B]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      block = blockedReader.block,
      rootBlockRefOffset = blockedReader.rootBlockRefOffset,
      blockCache = blockedReader.blockCache,
      reader = blockedReader.reader
    )
}

private[core] class UnblockedReader[O <: BlockOffset, B <: Block[O]] private(val block: B,
                                                                             val rootBlockRefOffset: BlockOffset,
                                                                             val blockCache: Option[BlockCacheState],
                                                                             private[reader] val reader: Reader) extends BlockReaderBase with UnblockedReaderOption[O, B] {

  val hasBlockCache: Boolean =
    blockCache.isDefined

  def offset: O =
    block.offset

  override def isNone: Boolean =
    false

  def underlyingArraySizeOrReaderSize: Int =
    reader match {
      case reader: FileReader =>
        reader.size()

      case SliceReader(slice, _) =>
        slice.underlyingArraySize
    }

  override def moveTo(newPosition: Int): this.type =
    super.moveTo(newPosition)

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] = {
    val bytes = readFullBlock()
    UnblockedReader[O, B](
      bytes = bytes,
      block = blockOps.updateBlockOffset(block, 0, bytes.size)
    )
  }

  def copy(): this.type =
    new UnblockedReader[O, B](
      block = block,
      blockCache = blockCache,
      rootBlockRefOffset = rootBlockRefOffset,
      reader = reader.copy()
    ).asInstanceOf[this.type]
}
