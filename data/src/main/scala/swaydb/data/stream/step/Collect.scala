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
 */

package swaydb.data.stream.step

import swaydb.{Bag, Stream}

private[swaydb] class Collect[A, B](previousStream: Stream[A],
                                    pf: PartialFunction[A, B]) extends Stream[B] {

  var previousA: Option[A] = Option.empty

  def stepForward[T[_]](startFrom: Option[A])(implicit bag: Bag[T]): T[Option[B]] =
    startFrom match {
      case Some(startFrom) =>
        var nextMatch = Option.empty[B]

        //collectFirst is a stackSafe way reading the stream until a condition is met.
        //use collectFirst to stream until the first match.

        val collected =
          Step
            .collectFirst(startFrom, previousStream) {
              nextA =>
                this.previousA = Some(nextA)
                nextMatch = this.previousA.collectFirst(pf)
                nextMatch.isDefined
            }

        bag.map(collected) {
          _ =>
            //return the matched result. This code could be improved if tag.collectFirst also took a pf instead of a function.
            nextMatch
        }

      case None =>
        bag.none
    }

  override def headOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[B]] =
    bag.flatMap(previousStream.headOption) {
      headOption =>
        //check if head satisfies the partial functions.
        this.previousA = headOption //also store A in the current Stream so next() invocation starts from this A.
        val previousAMayBe = previousA.collectFirst(pf) //check if headOption can be returned.

        if (previousAMayBe.isDefined) //check if headOption satisfies the partial function.
          bag.success(previousAMayBe) //yes it does. Return!
        else if (headOption.isDefined) //headOption did not satisfy the partial function but check if headOption was defined and step forward.
          stepForward(headOption) //headOption was defined so there might be more in the stream so step forward.
        else //if there was no headOption then stream must be empty.
          bag.none //empty stream.
    }

  override private[swaydb] def next[BAG[_]](previous: B)(implicit bag: Bag[BAG]): BAG[Option[B]] =
    stepForward(previousA) //continue from previously read A.
}
