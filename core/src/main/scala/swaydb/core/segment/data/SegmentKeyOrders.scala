package swaydb.core.segment.data

import swaydb.slice.order.KeyOrder
import swaydb.slice.Slice

object SegmentKeyOrders {

  def apply(keyOrder: KeyOrder[Slice[Byte]]): SegmentKeyOrders =
    new SegmentKeyOrders()(
      keyOrder = keyOrder,
      partialKeyOrder = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder)),
      persistentKeyOrder = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
    )

}

class SegmentKeyOrders private(implicit val keyOrder: KeyOrder[Slice[Byte]],
                               implicit val partialKeyOrder: KeyOrder[Persistent.Partial],
                               implicit val persistentKeyOrder: KeyOrder[Persistent])
