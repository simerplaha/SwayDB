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

package swaydb.core.data

import swaydb.data.slice.{Slice, SliceOption}

import scala.concurrent.duration.Deadline

private[swaydb] sealed trait SwayFunction

private[swaydb] object SwayFunction {
  sealed trait RequiresKey extends SwayFunction
  sealed trait RequiresValue extends SwayFunction
  sealed trait RequiresDeadline extends SwayFunction

  case class Key(f: Slice[Byte] => SwayFunctionOutput) extends RequiresKey
  case class KeyDeadline(f: (Slice[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresDeadline
  case class KeyValue(f: (Slice[Byte], SliceOption[Byte]) => SwayFunctionOutput) extends RequiresKey with RequiresValue

  case class Value(f: SliceOption[Byte] => SwayFunctionOutput) extends RequiresValue
  case class ValueDeadline(f: (SliceOption[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresValue with RequiresDeadline
  case class KeyValueDeadline(f: (Slice[Byte], SliceOption[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresValue with RequiresDeadline
}

private[swaydb] sealed trait SwayFunctionOutput
private[swaydb] object SwayFunctionOutput {

  case object Nothing extends SwayFunctionOutput
  case object Remove extends SwayFunctionOutput

  case class Expire(deadline: Deadline) extends SwayFunctionOutput
  case class Update(value: SliceOption[Byte], deadline: Option[Deadline]) extends SwayFunctionOutput
}
