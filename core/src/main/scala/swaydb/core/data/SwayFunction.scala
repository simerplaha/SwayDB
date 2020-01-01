/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.data

import swaydb.data.slice.{Slice, SliceOptional}

import scala.concurrent.duration.Deadline

private[swaydb] sealed trait SwayFunction

private[swaydb] object SwayFunction {
  sealed trait RequiresKey extends SwayFunction
  sealed trait RequiresValue extends SwayFunction
  sealed trait RequiresDeadline extends SwayFunction

  case class Key(f: Slice[Byte] => SwayFunctionOutput) extends RequiresKey
  case class KeyDeadline(f: (Slice[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresDeadline
  case class KeyValue(f: (Slice[Byte], SliceOptional[Byte]) => SwayFunctionOutput) extends RequiresKey with RequiresValue

  case class KeyValueDeadline(f: (Slice[Byte], SliceOptional[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresValue with RequiresDeadline
  case class Value(f: SliceOptional[Byte] => SwayFunctionOutput) extends RequiresValue
  case class ValueDeadline(f: (SliceOptional[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresValue with RequiresDeadline
}

private[swaydb] sealed trait SwayFunctionOutput
private[swaydb] object SwayFunctionOutput {

  case object Nothing extends SwayFunctionOutput
  case object Remove extends SwayFunctionOutput

  case class Expire(deadline: Deadline) extends SwayFunctionOutput
  case class Update(value: SliceOptional[Byte], deadline: Option[Deadline]) extends SwayFunctionOutput
}
