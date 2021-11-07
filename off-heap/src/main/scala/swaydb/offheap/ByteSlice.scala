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

package swaydb.offheap

import swaydb.utils.SomeOrNone

sealed trait ByteSliceOption extends SomeOrNone[ByteSliceOption, ByteSlice] {
  override def noneS: ByteSliceOption = ByteSlice.Null
}

case object ByteSlice {

  final case object Null extends ByteSliceOption {
    override val isNoneS: Boolean = true

    override def getS: ByteSlice =
      throw new Exception(s"${ByteSlice.productPrefix} is of type ${Null.productPrefix}")
  }
}

class ByteSlice(val address: Address,
                val fromOffset: Int,
                val toOffset: Int,
                private var written: Int,
                reference: ByteSliceOption) extends ByteSliceOption { self =>

  private var writePosition = fromOffset + written

  override def isNoneS: Boolean = false
  override def getS: ByteSlice = this


}
