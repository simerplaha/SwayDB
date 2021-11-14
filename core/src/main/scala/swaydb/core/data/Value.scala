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
import swaydb.utils.SomeOrNone

import scala.concurrent.duration.Deadline

private[swaydb] sealed trait Value {
  def mightContainRemove: Boolean
  def cut(): Value
  def isCut: Boolean
  def time: Time
}

private[swaydb] object Value {

  def hasTimeLeft(rangeValue: Value.RangeValue): Boolean =
    rangeValue match {
      case remove: Value.Remove =>
        remove.deadline exists (_.hasTimeLeft())

      case update: Value.Update =>
        update.deadline forall (_.hasTimeLeft())

      case _: Value.Function | _: Value.PendingApply =>
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
    def cut(): RangeValue
  }

  sealed trait FromValueOption extends SomeOrNone[FromValueOption, Value.FromValue] {
    override def noneS: FromValueOption =
      Value.FromValue.Null
  }

  object FromValue {
    final case object Null extends FromValueOption {
      override def isNoneS: Boolean =
        true

      def getS: Value.FromValue =
        throw new Exception("Value.FromValue is None")

    }
  }

  private[swaydb] sealed trait FromValue extends Value with FromValueOption {
    def isPut: Boolean
    def cut(): FromValue
    def toMemory(key: Slice[Byte]): Memory.Fixed
    def toPutMayBe(key: Slice[Byte]): Option[Memory.Put]

    def getS: Value.FromValue =
      this

    override def isNoneS: Boolean = false
  }

  private[swaydb] sealed trait Apply extends RangeValue {
    def cut(): Apply
    def time: Time
  }

  case class Remove(deadline: Option[Deadline],
                    time: Time) extends RangeValue with Apply {

    def isPut: Boolean = false

    override val mightContainRemove: Boolean = true

    def cut(): Value.Remove =
      Remove(deadline = deadline, time = time.cut())

    override def isCut: Boolean =
      time.time.isOriginalFullSlice

    def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    override def toMemory(key: Slice[Byte]): Memory.Remove =
      Memory.Remove(
        key = key,
        deadline = deadline,
        time = time
      )

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      scala.None

  }

  case class Put(value: SliceOption[Byte],
                 deadline: Option[Deadline],
                 time: Time) extends FromValue {

    def isPut: Boolean = true

    override val mightContainRemove: Boolean = false

    def cut(): Value.Put =
      Put(value = value.cutToSliceOption(), deadline, time.cut())

    override def isCut: Boolean =
      value.isNullOrNonEmptyCut && time.time.isOriginalFullSlice

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

  case class Update(value: SliceOption[Byte],
                    deadline: Option[Deadline],
                    time: Time) extends RangeValue with Apply {

    def isPut: Boolean = false

    override val mightContainRemove: Boolean = false

    def cut(): Value.Update =
      Update(value = value.cutToSliceOption(), deadline, time.cut())

    override def isCut: Boolean =
      value.isNullOrNonEmptyCut && time.time.isOriginalFullSlice

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
      scala.None

  }

  case class Function(function: Slice[Byte],
                      time: Time) extends RangeValue with Apply {

    def isPut: Boolean = false

    override val mightContainRemove: Boolean = true

    def cut(): Function =
      Function(function.cut(), time.cut())

    override def isCut: Boolean =
      function.isOriginalFullSlice && time.time.isOriginalFullSlice

    def toMemory(key: Slice[Byte]): Memory.Function =
      Memory.Function(
        key = key,
        function = function,
        time = time
      )

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      scala.None
  }

  /**
   * Applies are in ascending order where the head apply is the oldest.
   */
  case class PendingApply(applies: Slice[Value.Apply]) extends RangeValue {
    def isPut: Boolean = false

    override def mightContainRemove: Boolean = applies.exists(_.mightContainRemove)

    override def time = Time.fromApplies(applies)

    def cut(): Value.PendingApply =
      PendingApply(applies.mapToSlice(_.cut()))

    override def isCut: Boolean =
      applies.forall(_.isCut)

    def toMemory(key: Slice[Byte]): Memory.PendingApply =
      Memory.PendingApply(
        key = key,
        applies = applies
      )

    override def toPutMayBe(key: Slice[Byte]): Option[Memory.Put] =
      scala.None

  }
}
