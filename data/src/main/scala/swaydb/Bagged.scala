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

package swaydb


trait Bagged[A, BAG[_]] {
  def get: BAG[A]

  @inline final def isSuccess[B](b: BAG[B])(implicit bag: Bag.Sync[BAG]): Boolean =
    bag.isSuccess(b)

  @inline final def isFailure[B](b: BAG[B])(implicit bag: Bag.Sync[BAG]): Boolean =
    bag.isFailure(b)

  @inline final def getOrElse[B >: A](b: => B)(implicit bag: Bag.Sync[BAG]): B =
    bag.getOrElse[A, B](get)(b)

  @inline final def orElse[B >: A](b: => BAG[B])(implicit bag: Bag.Sync[BAG]): BAG[B] =
    bag.orElse[A, B](get)(b)

  @inline final def exception(a: BAG[A])(implicit bag: Bag.Sync[BAG]): Option[Throwable] =
    bag.exception(a)
}
