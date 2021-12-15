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

private[swaydb] class Filter[A](previousStream: StreamFree[A],
                                condition: A => Boolean) extends StreamFree[A] {

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
    bag.flatMap(previousStream.headOrNull) {
      case null =>
        bag.success(null.asInstanceOf[A])

      case a =>
        if (condition(a))
          bag.success(a)
        else
          nextOrNull(a)
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]) =
    Step.collectFirst(previous, previousStream)(condition)
}
