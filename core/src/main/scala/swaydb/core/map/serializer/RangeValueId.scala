/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.map.serializer

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

object RangeValueId {
  val ids: Map[Int, RangeValueId] =
    Sealed.list[RangeValueId].map {
      rangeId =>
        rangeId.id -> rangeId
    }.toMap

  def apply(id: Int): IO[swaydb.Error.Fatal, RangeValueId] =
    ids.get(id)
      .map(IO.Right[swaydb.Error.Fatal, RangeValueId](_))
      .getOrElse(IO.failed[swaydb.Error.Fatal, RangeValueId](s"Invalid ${this.getClass.getSimpleName}: $id"))
}
