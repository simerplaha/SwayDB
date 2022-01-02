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

package swaydb.core.segment.block.values

import swaydb.config.UncompressedBlockInfo
import swaydb.core.compression.CoreCompression
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.SomeOrNone

sealed trait ValuesBlockStateOption extends SomeOrNone[ValuesBlockStateOption, ValuesBlockState] {
  override def noneS: ValuesBlockStateOption =
    ValuesBlockState.Null
}

case object ValuesBlockState {

  final case object Null extends ValuesBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: ValuesBlockState = throw new Exception(s"${ValuesBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}


private[block] class ValuesBlockState(var compressibleBytes: SliceMut[Byte],
                                      val cacheableBytes: Slice[Byte],
                                      var header: Slice[Byte],
                                      val compressions: UncompressedBlockInfo => Iterable[CoreCompression],
                                      val builder: EntryWriter.Builder) extends ValuesBlockStateOption {

  override def isNoneS: Boolean =
    false

  override def getS: ValuesBlockState =
    this

  def blockSize: Int =
    header.size + compressibleBytes.size

  def blockBytes =
    header ++ compressibleBytes
}
