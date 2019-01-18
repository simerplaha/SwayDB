package swaydb.core.merge

import scala.util.Try
import swaydb.core.data.Value
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object ValueMerger {

  def apply(key: Slice[Byte],
            newRangeValue: Value.RangeValue,
            oldFromValue: Value.FromValue)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore): Try[Value.FromValue] =
    FixedMerger(
      newKeyValue = newRangeValue.toMemory(key),
      oldKeyValue = oldFromValue.toMemory(key)
    ) flatMap (_.toFromValue())

  def apply(key: Slice[Byte],
            newRangeValue: Value.FromValue,
            oldFromValue: Value.FromValue)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore): Try[Value.FromValue] =
    FixedMerger(
      newKeyValue = newRangeValue.toMemory(key),
      oldKeyValue = oldFromValue.toMemory(key)
    ) flatMap (_.toFromValue())

  def apply(newRangeValue: Value.RangeValue,
            oldRangeValue: Value.RangeValue)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                             functionStore: FunctionStore): Try[Value.RangeValue] =
    FixedMerger(
      newKeyValue = newRangeValue.toMemory(Slice.emptyBytes),
      oldKeyValue = oldRangeValue.toMemory(Slice.emptyBytes)
    ) flatMap (_.toRangeValue())
}
