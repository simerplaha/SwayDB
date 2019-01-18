package swaydb.core.merge

import scala.util.{Success, Try}
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.util.TryUtil._
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object ApplyMerger {

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Put)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                       functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          Try(RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Update =>
          Try(UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      Success(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          Try(RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Update =>
          Try(UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      Success(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Update)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          Try(RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Update =>
          Try(UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue))

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      Success(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): Try[ReadOnly.Fixed] =
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
      Success(oldKeyValue)

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: ReadOnly.Function)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): Try[ReadOnly.Fixed] =
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
      Success(oldKeyValue)

  def apply(newApplies: Slice[Value.Apply],
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    newApplies.tryFoldLeft((oldKeyValue, 0)) {
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
