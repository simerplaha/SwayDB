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

import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.{FileReader, Reader}
import swaydb.core.segment.block._
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.data.slice.{Reader, Slice, SliceReader}
import swaydb.data.utils.ByteOps

private[core] object BlockRefReader {

  def apply(file: DBFile,
            blockCache: Option[BlockCache.State]): BlockRefReader[SegmentBlock.Offset] = {
    val offset = SegmentBlock.Offset(0, file.fileSize.toInt)

    new BlockRefReader(
      offset = offset,
      rootBlockRefOffset = offset,
      blockCache = blockCache,
      reader = Reader(file)
    )
  }

  def apply(file: DBFile,
            fileSize: Int,
            blockCache: Option[BlockCache.State]): BlockRefReader[SegmentBlock.Offset] = {
    val offset = SegmentBlock.Offset(0, fileSize)

    new BlockRefReader(
      offset = offset,
      rootBlockRefOffset = offset,
      blockCache = blockCache,
      reader = Reader(file)
    )
  }

  def apply(file: DBFile,
            start: Int,
            fileSize: Int,
            blockCache: Option[BlockCache.State]): BlockRefReader[SegmentBlock.Offset] = {
    val offset = SegmentBlock.Offset(start, fileSize)

    new BlockRefReader(
      offset = SegmentBlock.Offset(start, fileSize),
      rootBlockRefOffset = offset,
      blockCache = blockCache,
      reader = Reader(file)
    )
  }

  def apply[O <: BlockOffset](bytes: Slice[Byte])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] = {
    val offset = blockOps.createOffset(0, bytes.size)

    new BlockRefReader(
      offset = offset,
      rootBlockRefOffset = offset,
      blockCache = None,
      reader = Reader(bytes)
    )
  }

  /**
   * @note these readers are required to be nested because [[UnblockedReader]] might have a header size which is not current read.
   */
  def moveTo[O <: BlockOffset, OO <: BlockOffset](start: Int,
                                                  size: Int,
                                                  reader: UnblockedReader[OO, _],
                                                  blockCache: Option[BlockCache.State])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader(
      offset = blockOps.createOffset(reader.offset.start + start, size),
      rootBlockRefOffset = reader.rootBlockRefOffset,
      blockCache = blockCache,
      reader = reader.reader
    )

  def moveTo[O <: BlockOffset, OO <: BlockOffset](offset: O,
                                                  reader: UnblockedReader[OO, _],
                                                  blockCache: Option[BlockCache.State])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader(
      offset = blockOps.createOffset(reader.offset.start + offset.start, offset.size),
      rootBlockRefOffset = reader.rootBlockRefOffset,
      blockCache = blockCache,
      reader = reader.reader
    )


  /**
   * NOTE: [[swaydb.core.segment.PersistentSegment]]s should not create [[BlockRefReader]]
   * from other [[BlockReaderBase]]. They should be created directly on a [[DBFile]] because
   * [[BlockRefReader]] becomes the [[BlockReaderBase.rootBlockRefOffset]] which [[BlockReaderBase]]
   * uses within it's [[BlockCache]] to adjust position (key) in the cache such that they are
   * transferable when Segments are transferred to other Segments.
   */
  def apply[O <: BlockOffset](ref: BlockRefReader[_ <: BlockOffset],
                              start: Int,
                              blockCache: Option[BlockCache.State])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader[O](
      offset = blockOps.createOffset(ref.offset.start + start, ref.size.toInt),
      rootBlockRefOffset = ref.rootBlockRefOffset,
      blockCache = blockCache,
      reader = ref.reader
    )

  def apply[O <: BlockOffset](ref: BlockRefReader[_ <: BlockOffset],
                              start: Int,
                              size: Int,
                              blockCache: Option[BlockCache.State])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader[O](
      offset = blockOps.createOffset(ref.offset.start + start, size),
      rootBlockRefOffset = ref.rootBlockRefOffset,
      blockCache = blockCache,
      reader = ref.reader
    )

  def apply[O <: BlockOffset](reader: Reader[Byte],
                              blockCache: Option[BlockCache.State])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] = {
    val offset = blockOps.createOffset(0, reader.size.toInt)

    new BlockRefReader(
      offset = blockOps.createOffset(0, reader.size.toInt),
      rootBlockRefOffset = offset,
      blockCache = blockCache,
      reader = reader
    )
  }
}

private[core] class BlockRefReader[O <: BlockOffset] private(val offset: O,
                                                             val rootBlockRefOffset: BlockOffset,
                                                             val blockCache: Option[BlockCache.State],
                                                             private[reader] val reader: Reader[Byte])(implicit val byteOps: ByteOps[Byte]) extends BlockReaderBase {

  override def moveTo(newPosition: Long): BlockRefReader[O] = {
    moveTo(newPosition.toInt)
    this
  }

  override def moveTo(newPosition: Int): BlockRefReader[O] = {
    super.moveTo(newPosition)
    this
  }

  def transfer(position: Int, count: Int, transferTo: DBFile): Unit =
    reader match {
      case reader: FileReader =>
        reader.transfer(position = offset.start + position, count = count, transferTo = transferTo)

      case SliceReader(slice, position) =>
        val toTransfer = slice.take(fromIndex = offset.start + position, count = count)
        transferTo.append(toTransfer)
    }

  /**
   * Transfers bytes outside this [[BlockRefReader]]'s offset.
   */
  def transferIgnoreOffset(position: Int, count: Int, transferTo: DBFile): Unit =
    reader match {
      case reader: FileReader =>
        reader.transfer(position = position, count = count, transferTo = transferTo)

      case SliceReader(slice, position) =>
        val toTransfer = slice.take(fromIndex = position, count = count)
        transferTo.append(toTransfer)
    }

  def readFullBlockAndGetReader()(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    BlockRefReader(readFullBlock())

  def copy(): BlockRefReader[O] =
    new BlockRefReader(
      reader = reader.copy(),
      blockCache = blockCache,
      rootBlockRefOffset = rootBlockRefOffset,
      offset = offset
    )

}
