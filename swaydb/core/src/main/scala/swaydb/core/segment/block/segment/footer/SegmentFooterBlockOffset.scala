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

package swaydb.core.segment.block.segment.footer

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object SegmentFooterBlockOffset {

  implicit object SegmentFooterBlockOps extends BlockOps[SegmentFooterBlockOffset, SegmentFooterBlock] {
    override def updateBlockOffset(block: SegmentFooterBlock, start: Int, size: Int): SegmentFooterBlock =
      block.copy(offset = createOffset(start, size))

    override def createOffset(start: Int, size: Int): SegmentFooterBlockOffset =
      SegmentFooterBlockOffset(start, size)

    override def readBlock(header: BlockHeader[SegmentFooterBlockOffset]): SegmentFooterBlock =
      throw new Exception("Footers do not have block header readers.")
  }

}

@inline case class SegmentFooterBlockOffset(start: Int,
                                            size: Int) extends BlockOffset
