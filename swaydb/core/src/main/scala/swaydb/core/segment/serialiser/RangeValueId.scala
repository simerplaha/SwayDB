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

package swaydb.core.segment.serialiser

import swaydb.macros.MacroSealed

sealed trait RangeValueId {
  val id: Int
}

case object RangeValueId {

  //Single
  case object Remove extends RangeValueId {
    override val id: Int = 0
  }

  case object Put extends RangeValueId {
    override val id: Int = 1
  }

  case object Update extends RangeValueId {
    override val id: Int = 2
  }

  case object Function extends RangeValueId {
    override val id: Int = 3
  }

  case object PendingApply extends RangeValueId {
    override val id: Int = 4
  }

  //Remove
  case object RemoveRemove extends RangeValueId {
    override val id: Int = 5
  }

  case object RemoveUpdate extends RangeValueId {
    override val id: Int = 6
  }

  case object RemoveFunction extends RangeValueId {
    override val id: Int = 7
  }

  case object RemovePendingApply extends RangeValueId {
    override val id: Int = 8
  }

  //Function
  case object FunctionRemove extends RangeValueId {
    override val id: Int = 9
  }

  case object FunctionUpdate extends RangeValueId {
    override val id: Int = 10
  }

  case object FunctionFunction extends RangeValueId {
    override val id: Int = 11
  }

  case object FunctionPendingApply extends RangeValueId {
    override val id: Int = 12
  }

  //Put
  case object PutUpdate extends RangeValueId {
    override val id: Int = 13
  }

  case object PutRemove extends RangeValueId {
    override val id: Int = 14
  }

  case object PutFunction extends RangeValueId {
    override val id: Int = 15
  }

  case object PutPendingApply extends RangeValueId {
    override val id: Int = 16
  }

  //Update
  case object UpdateRemove extends RangeValueId {
    override val id: Int = 17
  }

  case object UpdateUpdate extends RangeValueId {
    override val id: Int = 18
  }

  case object UpdateFunction extends RangeValueId {
    override val id: Int = 19
  }

  case object UpdatePendingApply extends RangeValueId {
    override val id: Int = 20
  }

  //Apply
  case object PendingApplyPendingApply extends RangeValueId {
    override val id: Int = 21
  }

  case object PendingApplyRemove extends RangeValueId {
    override val id: Int = 22
  }

  case object PendingApplyFunction extends RangeValueId {
    override val id: Int = 23
  }

  case object PendingApplyUpdate extends RangeValueId {
    override val id: Int = 24
  }

  val ids: Array[RangeValueId] =
    MacroSealed.array[RangeValueId].sortBy(_.id)

  def apply(id: Int): RangeValueId =
    ids(id)

}
