/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.stream.step

import swaydb.Bag
import swaydb.data.stream.StreamFree

private[swaydb] class Collect[A, B](previousStream: StreamFree[A],
                                    pf: PartialFunction[A, B]) extends StreamFree[B] {

  var previousA: A = _

  def stepForward[BAG[_]](startFromOrNull: A)(implicit bag: Bag[BAG]): BAG[B] =
    if (startFromOrNull == null) {
      bag.success(null.asInstanceOf[B])
    } else {
      var nextMatch: B = null.asInstanceOf[B]

      //collectFirst is a stackSafe way reading the stream until a condition is met.
      //use collectFirst to stream until the first match.

      val collected =
        Step
          .collectFirst(startFromOrNull, previousStream) {
            nextAOrNull =>
              this.previousA = nextAOrNull

              if (this.previousA != null && pf.isDefinedAt(this.previousA))
                nextMatch = pf.apply(this.previousA)

              nextMatch != null
          }

      bag.transform(collected) {
        _ =>
          //return the matched result. This code could be improved if bag.collectFirst also took a pf instead of a function.
          nextMatch
      }
    }

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[B] =
    bag.flatMap(previousStream.headOrNull) {
      headOrNull =>
        //check if head satisfies the partial functions.
        this.previousA = headOrNull //also store A in the current Stream so next() invocation starts from this A.
        val previousAOrNull =
          if (previousA == null || !pf.isDefinedAt(previousA))
            null.asInstanceOf[B]
          else
            pf.apply(previousA)
        //check if headOption can be returned.

        if (previousAOrNull != null) //check if headOption satisfies the partial function.
          bag.success(previousAOrNull) //yes it does. Return!
        else if (headOrNull != null) //headOption did not satisfy the partial function but check if headOption was defined and step forward.
          stepForward(headOrNull) //headOption was defined so there might be more in the stream so step forward.
        else //if there was no headOption then stream must be empty.
          bag.success(null.asInstanceOf[B]) //empty stream.
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: B)(implicit bag: Bag[BAG]) =
    stepForward(previousA) //continue from previously read A.
}
