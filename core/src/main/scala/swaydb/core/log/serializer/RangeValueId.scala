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

package swaydb.core.log.serializer

import swaydb.IO
import swaydb.macros.Sealed

sealed trait RangeValueId {
  val id: Int
}

//Single
case object RemoveRange extends RangeValueId {
  override val id: Int = 0
}

case object PutRange extends RangeValueId {
  override val id: Int = 1
}

case object UpdateRange extends RangeValueId {
  override val id: Int = 2
}

case object FunctionRange extends RangeValueId {
  override val id: Int = 3
}

case object PendingApplyRange extends RangeValueId {
  override val id: Int = 4
}

//Remove
case object RemoveRemoveRange extends RangeValueId {
  override val id: Int = 5
}

case object RemoveUpdateRange extends RangeValueId {
  override val id: Int = 6
}

case object RemoveFunctionRange extends RangeValueId {
  override val id: Int = 7
}

case object RemovePendingApplyRange extends RangeValueId {
  override val id: Int = 8
}

//Function
case object FunctionRemoveRange extends RangeValueId {
  override val id: Int = 9
}

case object FunctionUpdateRange extends RangeValueId {
  override val id: Int = 10
}

case object FunctionFunctionRange extends RangeValueId {
  override val id: Int = 11
}

case object FunctionPendingApplyRange extends RangeValueId {
  override val id: Int = 12
}

//Put
case object PutUpdateRange extends RangeValueId {
  override val id: Int = 13
}

case object PutRemoveRange extends RangeValueId {
  override val id: Int = 14
}

case object PutFunctionRange extends RangeValueId {
  override val id: Int = 15
}

case object PutPendingApplyRange extends RangeValueId {
  override val id: Int = 16
}

//Update
case object UpdateRemoveRange extends RangeValueId {
  override val id: Int = 17
}

case object UpdateUpdateRange extends RangeValueId {
  override val id: Int = 18
}

case object UpdateFunctionRange extends RangeValueId {
  override val id: Int = 19
}

case object UpdatePendingApplyRange extends RangeValueId {
  override val id: Int = 20
}

//Apply
case object PendingApplyPendingApplyRange extends RangeValueId {
  override val id: Int = 21
}

case object PendingApplyRemoveRange extends RangeValueId {
  override val id: Int = 22
}

case object PendingApplyFunctionRange extends RangeValueId {
  override val id: Int = 23
}

case object PendingApplyUpdateRange extends RangeValueId {
  override val id: Int = 24
}

case object RangeValueId {

  val ids: Map[Int, RangeValueId] =
    Sealed.list[RangeValueId].map {
      rangeId =>
        rangeId.id -> rangeId
    }.toMap

  def apply(id: Int): IO[swaydb.Error.Fatal, RangeValueId] =
    ids.get(id)
      .map(IO.Right[swaydb.Error.Fatal, RangeValueId](_))
      .getOrElse(IO.failed[swaydb.Error.Fatal, RangeValueId](s"Invalid ${this.productPrefix}: $id"))

}
