package swaydb.core.merge

import scala.util.{Success, Try}
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object UpdateMerger {

  def apply(newKeyValue: ReadOnly.Update,
            oldKeyValue: ReadOnly.Put)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      (newKeyValue.deadline, oldKeyValue.deadline) match {
        case (None, None) =>
          newKeyValue.toPut()

        case (Some(_), None) =>
          newKeyValue.toPut()

        case (None, Some(_)) =>
          newKeyValue.toPut(oldKeyValue.deadline)

        case (Some(_), Some(_)) =>
          newKeyValue.toPut()
      }
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Update,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      (newKeyValue.deadline, oldKeyValue.deadline) match {
        case (None, None) =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case (Some(_), None) =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case (None, Some(_)) =>
          newKeyValue.copyWithDeadline(oldKeyValue.deadline)

        case (Some(_), Some(_)) =>
          newKeyValue
      }
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Update,
            oldKeyValue: ReadOnly.Update)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Update =
    if (newKeyValue.time > oldKeyValue.time)
      (newKeyValue.deadline, oldKeyValue.deadline) match {
        case (None, None) =>
          newKeyValue

        case (Some(_), None) =>
          newKeyValue

        case (None, Some(_)) =>
          newKeyValue.copyWithDeadline(oldKeyValue.deadline)

        case (Some(_), Some(_)) =>
          newKeyValue
      }
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Update,
            oldKeyValue: ReadOnly.Function)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      for {
        oldValue <- oldKeyValue.toFromValue()
        newValue <- newKeyValue.toFromValue()
      } yield {
        Memory.PendingApply(newKeyValue.key, Slice(oldValue, newValue))
      }
    else
      Success(oldKeyValue)

  def apply(newKeyValue: ReadOnly.Update,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue match {
        case oldKeyValue: Value.Remove =>
          Try(UpdateMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key)))

        case oldKeyValue: Value.Update =>
          Try(UpdateMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key)))

        case oldKeyValue: Value.Function =>
          UpdateMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))
      }
    else
      Try(oldKeyValue.toMemory(newKeyValue.key))

  def apply(newKeyValue: ReadOnly.Update,
            oldKeyValue: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): Try[ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue.getOrFetchApplies flatMap {
        olderApplies =>
          FixedMerger(
            newer = newKeyValue,
            oldApplies = olderApplies
          )
      }
    else
      Success(oldKeyValue)

  def apply(newKeyValue: ReadOnly.Update,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): Try[ReadOnly.Fixed] =
  //@formatter:off
    oldKeyValue match {
      case oldKeyValue: ReadOnly.Put =>             Try(UpdateMerger(newKeyValue, oldKeyValue))
      case oldKeyValue: ReadOnly.Remove =>          Try(UpdateMerger(newKeyValue, oldKeyValue))
      case oldKeyValue: ReadOnly.Update =>          Try(UpdateMerger(newKeyValue, oldKeyValue))
      case oldKeyValue: ReadOnly.Function =>        UpdateMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.PendingApply =>    UpdateMerger(newKeyValue, oldKeyValue)
    }
  //@formatter:on

}
