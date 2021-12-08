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

private[swaydb] sealed trait SegmentFunction

private[swaydb] object SegmentFunction {
  sealed trait RequiresKey extends SegmentFunction
  sealed trait RequiresValue extends SegmentFunction
  sealed trait RequiresDeadline extends SegmentFunction

  case class Key(f: Slice[Byte] => SegmentFunctionOutput) extends RequiresKey
  case class KeyDeadline(f: (Slice[Byte], Option[Deadline]) => SegmentFunctionOutput) extends RequiresKey with RequiresDeadline
  case class KeyValue(f: (Slice[Byte], SliceOption[Byte]) => SegmentFunctionOutput) extends RequiresKey with RequiresValue

  case class Value(f: SliceOption[Byte] => SegmentFunctionOutput) extends RequiresValue
  case class ValueDeadline(f: (SliceOption[Byte], Option[Deadline]) => SegmentFunctionOutput) extends RequiresValue with RequiresDeadline
  case class KeyValueDeadline(f: (Slice[Byte], SliceOption[Byte], Option[Deadline]) => SegmentFunctionOutput) extends RequiresKey with RequiresValue with RequiresDeadline
}

private[swaydb] sealed trait SegmentFunctionOutput
private[swaydb] object SegmentFunctionOutput {

  case object Nothing extends SegmentFunctionOutput
  case object Remove extends SegmentFunctionOutput

  case class Expire(deadline: Deadline) extends SegmentFunctionOutput
  case class Update(value: SliceOption[Byte], deadline: Option[Deadline]) extends SegmentFunctionOutput
}
