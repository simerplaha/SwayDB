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

private[swaydb] class FlatMap[A, B](previousStream: StreamFree[A],
                                    f: A => StreamFree[B]) extends StreamFree[B] {

  //cache stream and emits it's items.
  //next Stream is read only if the current cached stream is emitted.
  var innerStream: StreamFree[B] = _
  var previousA: A = _

  def streamNext[BAG[_]](nextA: A)(implicit bag: Bag[BAG]): BAG[B] = {
    innerStream = f(nextA)
    previousA = nextA
    innerStream.headOrNull
  }

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[B] =
    bag.flatMap(previousStream.headOrNull) {
      case null =>
        bag.success(null.asInstanceOf[B])

      case nextA =>
        streamNext(nextA)
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: B)(implicit bag: Bag[BAG]) =
    bag.flatMap(innerStream.nextOrNull(previous)) {
      case null =>
        bag.flatMap(previousStream.nextOrNull(previousA)) {
          case null =>
            bag.success(null.asInstanceOf[B])

          case nextA =>
            streamNext(nextA)
        }

      case some =>
        bag.success(some)
    }
}
