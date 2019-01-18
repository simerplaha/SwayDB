package swaydb.core.merge

import scala.util.Try
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.util.TryUtil._
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object FixedMerger {

  def apply(newer: ReadOnly.Fixed,
            older: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    older.getOrFetchApplies flatMap {
      oldApplies =>
        FixedMerger(newer, oldApplies)
    }

  def apply(newer: ReadOnly.Fixed,
            oldApplies: Slice[Value.Apply])(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    oldApplies.reverse.toIterable.tryFoldLeft((newer, 0)) {
      case ((newerMerged, count), olderApply) =>
        newerMerged match {
          case newer: ReadOnly.Put =>
            Try(PutMerger(newer, olderApply)) map {
              merged =>
                (merged, count + 1)
            }

          case newer: ReadOnly.Function =>
            FunctionMerger(newer, olderApply) map {
              merged =>
                (merged, count + 1)
            }

          case newer: ReadOnly.Remove =>
            RemoveMerger(newer, olderApply) map {
              merged =>
                (merged, count + 1)
            }

          case newer: ReadOnly.Update =>
            UpdateMerger(newer, olderApply) map {
              merged =>
                (merged, count + 1)
            }

          case newer: ReadOnly.PendingApply =>
            return newer.getOrFetchApplies map {
              newerApplies =>
                val newMergedApplies = oldApplies.dropRight(count) ++ newerApplies
                if (newMergedApplies.size == 1)
                  newMergedApplies.head.toMemory(newer.key)
                else
                  Memory.PendingApply(newer.key, newMergedApplies)
            }
        }
    } map (_._1)

  def apply(newKeyValue: ReadOnly.Fixed,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    newKeyValue match {
      case newKeyValue: ReadOnly.Put =>
        Try(PutMerger(newKeyValue, oldKeyValue))

      case newKeyValue: ReadOnly.Remove =>
        RemoveMerger(newKeyValue, oldKeyValue)

      case newKeyValue: ReadOnly.Function =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case newKeyValue: ReadOnly.Update =>
        UpdateMerger(newKeyValue, oldKeyValue)

      case newKeyValue: ReadOnly.PendingApply =>
        PendingApplyMerger(newKeyValue, oldKeyValue)
    }
}
