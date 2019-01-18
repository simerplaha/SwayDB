package swaydb.core.merge

import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.Value
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object PutMerger {

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Put)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Put =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Update)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =

    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Function)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue.toMemory(newKeyValue.key)

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
  //@formatter:off
    oldKeyValue match {
      case oldKeyValue: ReadOnly.Put =>             PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.Remove =>          PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.Update =>          PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.Function =>        PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.PendingApply =>    PutMerger(newKeyValue, oldKeyValue)
    }
  //@formatter:on

}
