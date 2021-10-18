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

private[swaydb] class Take[A](previousStream: StreamFree[A],
                              take: Int) extends StreamFree[A] {

  var taken: Int = 0

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
    if (take <= 0) {
      bag.success(null.asInstanceOf[A])
    } else {
      taken += 1
      previousStream.headOrNull
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]) =
    if (taken == take)
      bag.success(null.asInstanceOf[A])
    else
      Step.foldLeft(null.asInstanceOf[A], previous, previousStream, 0, StreamFree.takeOne) {
        case (_, next) =>
          taken += 1
          bag.success(next)
      }
}
