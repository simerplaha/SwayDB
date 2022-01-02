/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
