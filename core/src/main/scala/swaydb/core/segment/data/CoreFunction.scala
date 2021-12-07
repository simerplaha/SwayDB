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

package swaydb.core.segment.data

import swaydb.slice.{Slice, SliceOption}

import scala.concurrent.duration.Deadline

private[swaydb] sealed trait CoreFunction

private[swaydb] object CoreFunction {
  sealed trait RequiresKey extends CoreFunction
  sealed trait RequiresValue extends CoreFunction
  sealed trait RequiresDeadline extends CoreFunction

  case class Key(f: Slice[Byte] => CoreFunctionOutput) extends RequiresKey
  case class KeyDeadline(f: (Slice[Byte], Option[Deadline]) => CoreFunctionOutput) extends RequiresKey with RequiresDeadline
  case class KeyValue(f: (Slice[Byte], SliceOption[Byte]) => CoreFunctionOutput) extends RequiresKey with RequiresValue

  case class Value(f: SliceOption[Byte] => CoreFunctionOutput) extends RequiresValue
  case class ValueDeadline(f: (SliceOption[Byte], Option[Deadline]) => CoreFunctionOutput) extends RequiresValue with RequiresDeadline
  case class KeyValueDeadline(f: (Slice[Byte], SliceOption[Byte], Option[Deadline]) => CoreFunctionOutput) extends RequiresKey with RequiresValue with RequiresDeadline
}

private[swaydb] sealed trait CoreFunctionOutput
private[swaydb] object CoreFunctionOutput {

  case object Nothing extends CoreFunctionOutput
  case object Remove extends CoreFunctionOutput

  case class Expire(deadline: Deadline) extends CoreFunctionOutput
  case class Update(value: SliceOption[Byte], deadline: Option[Deadline]) extends CoreFunctionOutput
}
