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

package swaydb.core.merge

import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

private[core] object ApplyMerger {

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Put)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                       functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          IO(RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Update =>
          IO(UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      IO.Right(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          IO(RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Update =>
          IO(UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      IO.Right(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Update)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          IO(RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Update =>
          IO(UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      IO.Right(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Update =>
          UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      IO.Right(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Function)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Update =>
          UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      IO.Right(oldKeyValue)

  def apply(newApplies: Slice[Value.Apply],
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    newApplies.foldLeftIO((oldKeyValue, 0)) {
      case ((oldMerged, count), newApply) =>
        oldMerged match {
          case old: ReadOnly.Put =>
            ApplyMerger(newApply, old) map {
              merged =>
                (merged, count + 1)
            }
          case old: ReadOnly.Remove =>
            ApplyMerger(newApply, old) map {
              merged =>
                (merged, count + 1)
            }
          case old: ReadOnly.Function =>
            ApplyMerger(newApply, old) map {
              merged =>
                (merged, count + 1)
            }

          case old: ReadOnly.Update =>
            ApplyMerger(newApply, old) map {
              merged =>
                (merged, count + 1)
            }
          case old: ReadOnly.PendingApply =>
            return old.getOrFetchApplies map {
              oldMergedApplies =>
                val resultApplies = oldMergedApplies ++ newApplies.drop(count)
                if (resultApplies.size == 1)
                  resultApplies.head.toMemory(oldKeyValue.key)
                else
                  Memory.PendingApply(old.key, resultApplies)
            }
        }
    } map (_._1)
}
