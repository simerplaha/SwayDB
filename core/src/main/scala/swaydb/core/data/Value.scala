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

import swaydb.core.function.util.FunctionInvoker
import swaydb.data.slice.Slice

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.Try

private[swaydb] sealed trait Value {

  val isRemove: Boolean

  def notRemove: Boolean = !isRemove

  val deadline: Option[Deadline]

  def hasTimeLeft(): Boolean

  def isOverdue(): Boolean =
    !hasTimeLeft()

  def hasTimeLeftWithGrace(grace: FiniteDuration): Boolean
}

private[swaydb] object Value {

  private[swaydb] sealed trait FromValue extends Value
  private[swaydb] sealed trait RangeValue extends Value

  implicit class UnSliceFromValue(value: Value.FromValue) {

    /**
      * @return An Value key-value with it's byte arrays sliced.
      *         If the sliced byte array is empty, it set the value to None.
      */
    def unslice: Value.FromValue =
      value match {
        case _: Value.Remove =>
          value

        case put: Value.Put =>
          put.unslice()

        case update: Value.Update =>
          update.unslice()

        case update: Value.UpdateFunction =>
          update.unslice()
      }
  }

  implicit class UnSliceRangeValue(value: Value.RangeValue) {
    /**
      * @return An Value key-value with it's byte arrays sliced.
      *         If the sliced byte array is empty, it set the value to None.
      */
    def unslice: RangeValue =
      value match {
        case _: Value.Remove =>
          value

        case update: Value.Update =>
          update.unslice()

        case update: Value.UpdateFunction =>
          update.unslice()
      }

    def toMemory(key: Slice[Byte]): Memory.Fixed =
      value match {
        case Value.Remove(deadline) =>
          Memory.Remove(key, deadline)

        case Value.Update(value, deadline) =>
          Memory.Update(key, value, deadline)

        case Value.UpdateFunction(function, deadline) =>
          Memory.UpdateFunction(key, function, deadline)
      }
  }

  implicit class ValueImplicits(value: Value) {
    /**
      * @return An Value key-value with it's byte arrays sliced.
      *         If the sliced byte array is empty, it set the value to None.
      */
    def toMemory(key: Slice[Byte]): Memory.Fixed =
      value match {
        case Remove(deadline) =>
          Memory.Remove(key, deadline)
        case Put(value, deadline) =>
          Memory.Put(key, value, deadline)
        case Update(value, deadline) =>
          Memory.Update(key, value, deadline)
        case UpdateFunction(value, deadline) =>
          Memory.UpdateFunction(key, value, deadline)
      }
  }

  object Remove {
    def apply(deadline: Deadline): Remove =
      new Remove(Some(deadline))
  }

  case class Remove(deadline: Option[Deadline]) extends FromValue with RangeValue {

    override val isRemove: Boolean = true

    override def hasTimeLeft(): Boolean =
      deadline.exists(_.hasTimeLeft())

    override def hasTimeLeftWithGrace(grace: FiniteDuration): Boolean =
      deadline.exists(deadline => (deadline - grace).hasTimeLeft())
  }

  object Put {
    def apply(value: Slice[Byte]): Put =
      new Put(Some(value), None)

    def apply(value: Slice[Byte], removeAfter: Deadline): Put =
      new Put(Some(value), Some(removeAfter))

    def apply(value: Slice[Byte], duration: FiniteDuration): Put =
      new Put(Some(value), Some(duration.fromNow))

    def apply(value: Option[Slice[Byte]], duration: FiniteDuration): Put =
      new Put(value, Some(duration.fromNow))

    def apply(value: Option[Slice[Byte]])(removeAfter: Deadline): Put =
      new Put(value, Some(removeAfter))
  }

  case class Put(value: Option[Slice[Byte]],
                 deadline: Option[Deadline]) extends FromValue {

    override val isRemove: Boolean = false

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def hasTimeLeftWithGrace(grace: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - grace).hasTimeLeft())

    def unslice(): Value.Put = {
      val unslicedValue = value.map(_.unslice())
      if (unslicedValue.exists(_.isEmpty))
        Value.Put(None, deadline)
      else
        Value.Put(unslicedValue, deadline)
    }
  }

  object Update {
    def apply(value: Slice[Byte]): Update =
      new Update(Some(value), None)

    def apply(value: Slice[Byte], deadline: Option[Deadline]): Update =
      new Update(Some(value), deadline)

    def apply(value: Slice[Byte], removeAfter: Deadline): Update =
      new Update(Some(value), Some(removeAfter))

    def apply(value: Slice[Byte], duration: FiniteDuration): Update =
      new Update(Some(value), Some(duration.fromNow))

    def apply(value: Option[Slice[Byte]], duration: FiniteDuration): Update =
      new Update(value, Some(duration.fromNow))

    def apply(value: Option[Slice[Byte]])(removeAfter: Deadline): Update =
      new Update(value, Some(removeAfter))
  }

  case class Update(value: Option[Slice[Byte]],
                    deadline: Option[Deadline]) extends FromValue with RangeValue {

    override val isRemove: Boolean = false

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def hasTimeLeftWithGrace(grace: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - grace).hasTimeLeft())

    def unslice(): Value.Update = {
      val unslicedValue = value.map(_.unslice())
      if (unslicedValue.exists(_.isEmpty))
        Value.Update(None, deadline)
      else
        Value.Update(unslicedValue, deadline)
    }
  }

  case class UpdateFunction(function: Slice[Byte],
                            deadline: Option[Deadline]) extends FromValue with RangeValue {

    override val isRemove: Boolean = false

    final def applyFunction(value: Option[Slice[Byte]]): Try[Option[Slice[Byte]]] =
      FunctionInvoker(value, function)

    override def hasTimeLeft(): Boolean =
      deadline.forall(_.hasTimeLeft())

    override def hasTimeLeftWithGrace(grace: FiniteDuration): Boolean =
      deadline.forall(deadline => (deadline - grace).hasTimeLeft())

    def unslice(): Value.UpdateFunction =
      copy(function.unslice(), deadline)
  }
}
