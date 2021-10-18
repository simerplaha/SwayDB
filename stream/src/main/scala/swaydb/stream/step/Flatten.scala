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

private[swaydb] class Flatten[A, B](previousStream: StreamFree[A]) extends StreamFree[B] {

  var previousA: A = _

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[B] =
    bag.flatMap(previousStream.headOrNull.asInstanceOf[BAG[BAG[A]]]) {
      case null =>
        bag.success(null.asInstanceOf[B])

      case previousBag: BAG[A] =>
        bag.transform(previousBag) {
          previous =>
            this.previousA = previousBag.asInstanceOf[A]
            previous.asInstanceOf[B]
        }
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: B)(implicit bag: Bag[BAG]) =
    bag.flatMap(previousStream.nextOrNull(previousA).asInstanceOf[BAG[BAG[A]]]) {
      case null =>
        bag.success(null.asInstanceOf[B])

      case previousBag: BAG[A] =>
        bag.transform(previousBag) {
          previous: A =>
            this.previousA = previousBag.asInstanceOf[A]
            previous.asInstanceOf[B]
        }
    }
}
