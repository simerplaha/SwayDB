/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import scala.concurrent.duration.Deadline
import swaydb.core.segment.Segment
import swaydb.data.slice.Slice

private[swaydb] sealed trait Value {
  def hasRemoveMayBe: Boolean
  def unslice: Value
  def time: Time
}

private[swaydb] object Value {

  def hasTimeLeft(rangeValue: Value.RangeValue): Boolean =
    rangeValue match {
      case remove: Value.Remove =>
        remove.deadline exists (_.hasTimeLeft())
      case update: Value.Update =>
        update.deadline forall (_.hasTimeLeft())
      case _: Value.Function =>
        true
      case _: Value.PendingApply =>
        true
    }

  def hasTimeLeft(fromValue: Value.FromValue): Boolean =
    fromValue match {
      case rangeValue: RangeValue =>
        hasTimeLeft(rangeValue)
      case put: Put =>
        put.deadline forall (_.hasTimeLeft())
    }

  private[swaydb] sealed trait RangeValue extends FromValue {
    def unslice: RangeValue
  }
  private[swaydb] sealed trait FromValue extends Value {
    def unslice: FromValue
    def toMemory(key: Slice[Byte]): Memory.Fixed
    def toPutMayBe(key: Slice[Byte]): Option[Memory.Put]
  }

  private[swaydb] sealed trait Apply extends RangeValue {
    def unslice: Apply
    def time: Time
  }

  case class Remove(deadline: Option[Deadline],
                    time: Time) extends RangeValue with Apply {

    override val hasRemoveMayBe: Boolean = true

    def unslice(): Value.Remove =
      Remove(deadline = deadline, time = time.unslice())

    def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    override def toMemory(key: Slice[Byte]): Memory.Remove =
      Memory.Remove(
        key = key,
        deadline = deadline,
        time = time
      )

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      None
  }

  case class Put(value: Option[Slice[Byte]],
                 deadline: Option[Deadline],
                 time: Time) extends FromValue {

    override val hasRemoveMayBe: Boolean = false

    def unslice(): Value.Put =
      Put(value = value.unslice(), deadline, time.unslice())

    def toMemory(key: Slice[Byte]): Memory.Put =
      Memory.Put(
        key = key,
        value = value,
        deadline = deadline,
        time = time
      )

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      Some(toMemory(key))

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())
  }

  case class Update(value: Option[Slice[Byte]],
                    deadline: Option[Deadline],
                    time: Time) extends RangeValue with Apply {

    override val hasRemoveMayBe: Boolean = false

    def unslice(): Value.Update =
      Update(value = value.unslice(), deadline, time.unslice())

    def toMemory(key: Slice[Byte]): Memory.Update =
      Memory.Update(
        key = key,
        value = value,
        deadline = deadline,
        time = time
      )

    def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      None
  }

  case class Function(function: Slice[Byte],
                      time: Time) extends RangeValue with Apply {

    override val hasRemoveMayBe: Boolean = true

    def unslice(): Function =
      Function(function.unslice(), time.unslice())

    def toMemory(key: Slice[Byte]): Memory.Function =
      Memory.Function(
        key = key,
        function = function,
        time = time
      )

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      None
  }

  /**
    * Applies are in ascending order where the head apply is the oldest.
    */
  case class PendingApply(applies: Slice[Value.Apply]) extends RangeValue {
    override def hasRemoveMayBe: Boolean = applies.exists(_.hasRemoveMayBe)

    override def time = Time.fromApplies(applies)

    def unslice(): Value.PendingApply =
      PendingApply(applies.map(_.unslice))

    def toMemory(key: Slice[Byte]): Memory.PendingApply =
      Memory.PendingApply(
        key = key,
        applies = applies
      )

    def deadline =
      Segment.getNearestDeadline(None, applies)

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      None
  }
}
