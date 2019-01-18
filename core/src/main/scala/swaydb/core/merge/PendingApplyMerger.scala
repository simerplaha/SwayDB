package swaydb.core.merge

import scala.util.{Success, Try}
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.Value
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object PendingApplyMerger {

  /**
    * PendingApply
    */
  def apply(newKeyValue: ReadOnly.PendingApply,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue match {
        case oldKeyValue: ReadOnly.Remove if oldKeyValue.deadline.isEmpty =>
          Success(oldKeyValue.copyWithTime(newKeyValue.time))

        case _: ReadOnly.Fixed =>
          newKeyValue.getOrFetchApplies flatMap {
            newApplies =>
              ApplyMerger(newApplies, oldKeyValue)
          }
      }
    else
      Success(oldKeyValue)

  def apply(newKeyValue: ReadOnly.PendingApply,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue.deadline match {
        case None =>
          Success(oldKeyValue)

        case Some(_) =>
          PendingApplyMerger(newKeyValue, oldKeyValue: ReadOnly.Fixed)
      }
    else
      Success(oldKeyValue)

  def apply(newKeyValue: ReadOnly.PendingApply,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      PendingApplyMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))
    else
      Success(oldKeyValue.toMemory(newKeyValue.key))
}
