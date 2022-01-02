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

package swaydb.core.segment.block.hashindex

import swaydb.config.UncompressedBlockInfo
import swaydb.core.compression.CoreCompression
import swaydb.core.util.CRC32
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.SomeOrNone

import scala.beans.BeanProperty

sealed trait HashIndexBlockStateOption extends SomeOrNone[HashIndexBlockStateOption, HashIndexBlockState] {
  override def noneS: HashIndexBlockStateOption =
    HashIndexBlockState.Null
}

case object HashIndexBlockState {

  final case object Null extends HashIndexBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: HashIndexBlockState = throw new Exception(s"${HashIndexBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}


private[block] final class HashIndexBlockState(var hit: Int,
                                               var miss: Int,
                                               val format: HashIndexEntryFormat,
                                               val minimumNumberOfKeys: Int,
                                               val minimumNumberOfHits: Int,
                                               val writeAbleLargestValueSize: Int,
                                               @BeanProperty var minimumCRC: Long,
                                               val maxProbe: Int,
                                               var compressibleBytes: SliceMut[Byte],
                                               val cacheableBytes: Slice[Byte],
                                               var header: Slice[Byte],
                                               val compressions: UncompressedBlockInfo => Iterable[CoreCompression]) extends HashIndexBlockStateOption {

  override def isNoneS: Boolean =
    false

  override def getS: HashIndexBlockState =
    this


  def blockSize: Int =
    header.size + compressibleBytes.size

  def hasMinimumHits: Boolean =
    hit >= minimumNumberOfHits

  //CRC can be -1 when HashIndex is not fully copied.
  def minimumCRCToWrite(): Long =
    if (minimumCRC == CRC32.disabledCRC)
      0
    else
      minimumCRC

  val hashMaxOffset: Int =
    compressibleBytes.allocatedSize - writeAbleLargestValueSize
}
