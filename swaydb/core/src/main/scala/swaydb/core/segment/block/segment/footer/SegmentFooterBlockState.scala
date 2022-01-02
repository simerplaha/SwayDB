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

import swaydb.slice.SliceMut
import swaydb.utils.SomeOrNone

sealed trait SegmentFooterBlockStateOption extends SomeOrNone[SegmentFooterBlockStateOption, SegmentFooterBlockState] {
  override def noneS: SegmentFooterBlockStateOption =
    SegmentFooterBlockState.Null
}

case object SegmentFooterBlockState {

  final case object Null extends SegmentFooterBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: SegmentFooterBlockState = throw new Exception(s"${SegmentFooterBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}


private[block] case class SegmentFooterBlockState(footerSize: Int,
                                                  createdInLevel: Int,
                                                  var bytes: SliceMut[Byte],
                                                  keyValuesCount: Int,
                                                  rangeCount: Int,
                                                  updateCount: Int,
                                                  putCount: Int,
                                                  putDeadlineCount: Int) extends SegmentFooterBlockStateOption {
  override def isNoneS: Boolean =
    false

  override def getS: SegmentFooterBlockState =
    this

}
