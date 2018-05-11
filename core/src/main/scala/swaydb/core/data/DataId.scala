/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.data

import scala.util.{Failure, Success, Try}

sealed trait DataId {
  val id: Int
}

object DataId {

  val rangeIds =
    Seq(
      PutRange,
      UpdateRange,
      RemoveRange,
      RemoveRemoveRange,
      RemoveUpdateRange,
      PutUpdateRange,
      PutRemoveRange,
      PutPutRange,
      UpdateRemoveRange,
      UpdateUpdateRange
    ) sortBy (_.id)

  val ids: Seq[DataId] =
    (Seq(
      Remove,
      Put,
      Update
    ) ++ rangeIds) sortBy (_.id)

  val rangeIdMin = rangeIds.head

  val rangeIdMax = rangeIds.last

  def apply(id: Int): Try[DataId] =
    ids.find(_.id == id).map(Success(_)).getOrElse(Failure(new Exception(s"Invalid data id: $id")))

  case object Remove extends DataId {
    override val id: Int = 0
  }

  case object Put extends DataId {
    override val id: Int = 1
  }

  case object Update extends DataId {
    override val id: Int = 2
  }

  case object RemoveRange extends DataId {
    override val id: Int = 3
  }

  case object PutRange extends DataId {
    override val id: Int = 4
  }

  case object UpdateRange extends DataId {
    override val id: Int = 5
  }

  case object RemoveRemoveRange extends DataId {
    override val id: Int = 6
  }

  case object RemoveUpdateRange extends DataId {
    override val id: Int = 7
  }

  case object PutUpdateRange extends DataId {
    override val id: Int = 8
  }

  case object PutRemoveRange extends DataId {
    override val id: Int = 9
  }

  case object PutPutRange extends DataId {
    override val id: Int = 10
  }

  case object UpdateRemoveRange extends DataId {
    override val id: Int = 11
  }

  case object UpdateUpdateRange extends DataId {
    override val id: Int = 12
  }
}