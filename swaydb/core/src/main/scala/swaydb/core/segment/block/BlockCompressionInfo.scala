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

package swaydb.core.segment.block

import swaydb.core.compression.CoreDecompressor
import swaydb.utils.SomeOrNone

/**
 * Optional [[BlockCompressionInfo]] where the None value is [[BlockCompressionInfo.Null]]
 */
sealed trait BlockCompressionInfoOption extends SomeOrNone[BlockCompressionInfoOption, BlockCompressionInfo] {
  override def noneS: BlockCompressionInfoOption = BlockCompressionInfo.Null
}

case object BlockCompressionInfo {

  case object Null extends BlockCompressionInfoOption {
    override def isNoneS: Boolean =
      true

    override def getS: BlockCompressionInfo =
      throw new Exception(s"${BlockCompressionInfo.productPrefix} is ${Null.productPrefix}")
  }

  @inline def apply(decompressor: CoreDecompressor,
                    decompressedLength: Int): BlockCompressionInfo =
    new BlockCompressionInfo(decompressor, decompressedLength)

}

class BlockCompressionInfo(val decompressor: CoreDecompressor,
                           val decompressedLength: Int) extends BlockCompressionInfoOption {
  override def isNoneS: Boolean = false
  override def getS: BlockCompressionInfo = this
}
