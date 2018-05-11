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

package swaydb.core.segment

import swaydb.core.data.{KeyValue, Memory, Persistent, Value}
import swaydb.data.slice.Slice

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

private[core] object KeyValueMerger {

  /**
    * Applies rules for merging Range key-values.
    *
    * If both new & old values have deadline set, a check is performed to see if new value's deadline is smaller then old.
    * If true, new deadline is accepted or else if the new value's deadline is greater than old, then a hasTimeLeftAtLeast
    * check is made on old value's deadline to see if enough time is available on old value to apply new deadline which accounts
    * for time required until the key-values are written to disk or in-memory.
    *
    * For example:
    * If old key-value is reaching deadline in 1.second and new key-values deadline is 1.day.
    * And suppose if the merge process took 2 seconds to write to disk, clients concurrently reading the old
    * record after the first 1.second would start receiving None for the expired value and then
    * after writing the new key-value to disk this key-value would re-appear again.
    * Therefore the time required to merge & writing key-values to disk or memory should also be considered
    * when merging key-values with deadlines set.
    */
  def applyValue(newValue: KeyValue.ReadOnly.Fixed,
                 oldValue: Value,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.FromValue] =
    applyValue(newValue, oldValue.toMemory(Slice.emptyByteSlice), hasTimeLeftAtLeast).flatMap(_.toFromValue)

  def applyValue(newValue: Value,
                 oldValue: KeyValue.ReadOnly.Fixed,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.FromValue] =
    applyValue(newValue.toMemory(Slice.emptyByteSlice), oldValue, hasTimeLeftAtLeast).flatMap(_.toFromValue)

  def applyValue(newValue: Value.RangeValue,
                 oldValue: Value.RangeValue,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.RangeValue] =
    applyValue(newValue.toMemory(Slice.emptyByteSlice), oldValue.toMemory(Slice.emptyByteSlice), hasTimeLeftAtLeast).flatMap(_.toRangeValue)

  def applyValue(newValue: Value.FromValue,
                 oldValue: Value.RangeValue,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.FromValue] =
    applyValue(newValue.toMemory(Slice.emptyByteSlice), oldValue.toMemory(Slice.emptyByteSlice), hasTimeLeftAtLeast).flatMap(_.toFromValue)

  def applyValue(newValue: Value.RangeValue,
                 oldValue: Value.FromValue,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.FromValue] =
    applyValue(newValue.toMemory(Slice.emptyByteSlice), oldValue.toMemory(Slice.emptyByteSlice), hasTimeLeftAtLeast).flatMap(_.toFromValue)

  def applyValue(newValue: Value,
                 oldValue: Value.FromValue,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.FromValue] =
    applyValue(newValue.toMemory(Slice.emptyByteSlice), oldValue.toMemory(Slice.emptyByteSlice), hasTimeLeftAtLeast).flatMap(_.toFromValue)

  def applyValue(newValue: Value,
                 oldValue: Value,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.FromValue] =
    applyValue(newValue.toMemory(Slice.emptyByteSlice), oldValue.toMemory(Slice.emptyByteSlice), hasTimeLeftAtLeast).flatMap(_.toFromValue)

  def applyValue(newValue: Value.FromValue,
                 oldValue: Value,
                 hasTimeLeftAtLeast: FiniteDuration): Try[Value.FromValue] =
    applyValue(newValue.toMemory(Slice.emptyByteSlice), oldValue.toMemory(Slice.emptyByteSlice), hasTimeLeftAtLeast).flatMap(_.toFromValue)

  def applyValue(newKeyValue: KeyValue.ReadOnly.Fixed,
                 oldKeyValue: KeyValue.ReadOnly.Fixed,
                 hasTimeLeftAtLeast: FiniteDuration): Try[KeyValue.ReadOnly.Fixed] =
    Try {
      newKeyValue match {
        //*** Put combinations ***
        case _: Memory.Put | _: Persistent.Put => //put always overwrites older key-value
          newKeyValue

        //*** Remove combinations ***
        case Memory.Remove(_, None) | Persistent.Remove(_, None, _, _, _) => //Remove without deadline always overwrites old key-value
          newKeyValue

        case Memory.Remove(_, Some(_)) | Persistent.Remove(_, Some(_), _, _, _) => //Remove Some (when deadline exists)
          //*** Remove combinations when deadline exits in new key-value***
          oldKeyValue match {
            case Memory.Remove(_, None) | Persistent.Remove(_, None, _, _, _) => // Remove Some - Remove None
              oldKeyValue

            case Memory.Remove(_, Some(_)) | Persistent.Remove(_, Some(_), _, _, _) => // Remove Some - Remove Some
              if (newKeyValue.deadline.get <= oldKeyValue.deadline.get || oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                newKeyValue
              else
                oldKeyValue

            case Memory.Put(_, _, None) | Persistent.Put(_, None, _, _, _, _, _, _) => //Remove Some - Put None
              oldKeyValue.updateDeadline(deadline = newKeyValue.deadline.get)

            case Memory.Put(_, _, Some(_)) | Persistent.Put(_, Some(_), _, _, _, _, _, _) => //Remove Some - Put Some
              if (newKeyValue.deadline.get <= oldKeyValue.deadline.get || oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                oldKeyValue.updateDeadline(deadline = newKeyValue.deadline.get)
              else
                oldKeyValue

            case Memory.Update(_, _, None) | Persistent.Update(_, None, _, _, _, _, _, _) => //Remove Some - Update None
              oldKeyValue.updateDeadline(deadline = newKeyValue.deadline.get)

            case Memory.Update(_, _, Some(_)) | Persistent.Update(_, Some(_), _, _, _, _, _, _) => //Remove Some - Update Some
              if (newKeyValue.deadline.get <= oldKeyValue.deadline.get || oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                oldKeyValue.updateDeadline(deadline = newKeyValue.deadline.get)
              else
                oldKeyValue
          }

        //*** Update combinations ***
        case Memory.Update(_, _, None) | Persistent.Update(_, None, _, _, _, _, _, _) => //Update None without deadline always overwrites old key-value
          oldKeyValue match {
            case Memory.Remove(_, None) | Persistent.Remove(_, None, _, _, _) => // Remove Some - Remove None
              oldKeyValue

            case Memory.Remove(_, Some(_)) | Persistent.Remove(_, Some(_), _, _, _) => // Remove Some - Remove Some
              if (oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                newKeyValue.updateDeadline(oldKeyValue.deadline.get)
              else
                oldKeyValue

            case Memory.Put(_, _, None) | Persistent.Put(_, None, _, _, _, _, _, _) => //Put Some - Put None
              Memory.Put(newKeyValue.key, newKeyValue.getOrFetchValue.get, oldKeyValue.deadline)

            case Memory.Put(_, _, Some(_)) | Persistent.Put(_, Some(_), _, _, _, _, _, _) => //Put Some - Put Some
              if (oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                Memory.Put(newKeyValue.key, newKeyValue.getOrFetchValue.get, oldKeyValue.deadline)
              else
                oldKeyValue

            case Memory.Update(_, _, None) | Persistent.Update(_, None, _, _, _, _, _, _) => //Update Some - Update None
              newKeyValue

            case Memory.Update(_, _, Some(_)) | Persistent.Update(_, Some(_), _, _, _, _, _, _) => //Update Some - Update Some
              if (oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                newKeyValue.updateDeadline(oldKeyValue.deadline.get)
              else
                oldKeyValue
          }

        case Memory.Update(_, _, Some(_)) | Persistent.Update(_, Some(_), _, _, _, _, _, _) => //Update Some without deadline always overwrites old key-value
          oldKeyValue match {
            case Memory.Remove(_, None) | Persistent.Remove(_, None, _, _, _) => // Remove Some - Remove None
              oldKeyValue

            case Memory.Remove(_, Some(_)) | Persistent.Remove(_, Some(_), _, _, _) => // Remove Some - Remove Some
              if (newKeyValue.deadline.get <= oldKeyValue.deadline.get || oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                newKeyValue
              else
                oldKeyValue

            case Memory.Put(_, _, None) | Persistent.Put(_, None, _, _, _, _, _, _) => //Put Some - Put None
              Memory.Put(newKeyValue.key, newKeyValue.getOrFetchValue.get, newKeyValue.deadline)

            case Memory.Put(_, _, Some(_)) | Persistent.Put(_, Some(_), _, _, _, _, _, _) => //Put Some - Put Some
              if (newKeyValue.deadline.get <= oldKeyValue.deadline.get || oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                Memory.Put(newKeyValue.key, newKeyValue.getOrFetchValue.get, newKeyValue.deadline)
              else
                oldKeyValue

            case Memory.Update(_, _, None) | Persistent.Update(_, None, _, _, _, _, _, _) => //Update Some - Update None
              newKeyValue

            case Memory.Update(_, _, Some(_)) | Persistent.Update(_, Some(_), _, _, _, _, _, _) => //Update Some - Update Some
              if (newKeyValue.deadline.get <= oldKeyValue.deadline.get || oldKeyValue.hasTimeLeftAtLeast(hasTimeLeftAtLeast))
                newKeyValue
              else
                oldKeyValue
          }
      }
    }
}