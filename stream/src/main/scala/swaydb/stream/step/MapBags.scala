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

package swaydb.stream.step

import swaydb.Bag
import swaydb.stream.StreamFree

private[swaydb] class MapBags[X[_], A, B](previousStream: StreamFree[A],
                                          f: A => X[B]) extends StreamFree[B] {

  var previousA: A = _

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[B] =
    bag.flatMap(previousStream.headOrNull) {
      previousAOrNull =>
        previousA = previousAOrNull
        if (previousAOrNull == null)
          bag.success(null.asInstanceOf[B])
        else
          f(previousAOrNull).asInstanceOf[BAG[B]]
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: B)(implicit bag: Bag[BAG]): BAG[B] =
    if (previousA == null)
      bag.success(null.asInstanceOf[B])
    else
      bag.flatMap(previousStream.nextOrNull(previousA)) {
        nextA =>
          previousA = nextA
          if (nextA == null)
            bag.success(null.asInstanceOf[B])
          else
            f(nextA).asInstanceOf[BAG[B]]
      }
}
