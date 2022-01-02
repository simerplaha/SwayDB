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

package swaydb.core.segment.block.bloomfilter

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object BloomFilterBlockOffset {
  implicit object BloomFilterBlockOps extends BlockOps[BloomFilterBlockOffset, BloomFilterBlock] {
    override def updateBlockOffset(block: BloomFilterBlock, start: Int, size: Int): BloomFilterBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): BloomFilterBlockOffset =
      BloomFilterBlockOffset(start = start, size = size)

    override def readBlock(header: BlockHeader[BloomFilterBlockOffset]): BloomFilterBlock =
      BloomFilterBlock.read(header)
  }
}

@inline case class BloomFilterBlockOffset(start: Int,
                                          size: Int) extends BlockOffset
