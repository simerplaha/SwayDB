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
