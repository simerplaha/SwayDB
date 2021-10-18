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

package swaydb.utils

import swaydb.utils.Tagged.@@

import scala.util.Try

sealed trait MaybeTag

object Maybe {

  type Maybe[A] = @@[A, MaybeTag]

  @inline final def none[A]: Maybe[A] =
    null.asInstanceOf[Maybe[A]]

  val noneInt: Maybe[Int] =
    null.asInstanceOf[Maybe[Int]]

  @inline final def some[A](value: A): Maybe[A] =
    value.asInstanceOf[@@[A, MaybeTag]]

  def find[A](array: Array[A], p: A => Boolean): Maybe[A] = {
    var i = 0
    while (i < array.length) {
      val item = array(i)
      if (p(item))
        return Maybe.some(item)
      else
        i += 1
    }

    Maybe.none[A]
  }

  implicit class MaybeImplicits[A](value: Maybe[A]) {
    @inline final def isNone: Boolean =
      value == null

    @inline final def isSome: Boolean =
      value != null

    @inline final def mapMayBe[B](f: A => B): Maybe[B] =
      map(f)

    @inline final def map[B](f: A => B): Maybe[B] =
      if (value == null)
        none[B]
      else
        some(f(value))

    @inline final def exists(f: A => Boolean): Boolean =
      value != null && f(value)

    @inline final def notExists(f: A => Boolean): Boolean =
      !exists(f)

    @inline final def flatMapMayBe[B](f: A => Maybe[B]): Maybe[B] =
      flatMap(f)

    @inline final def flatMap[B](f: A => Maybe[B]): Maybe[B] =
      if (value == null)
        none[B]
      else
        f(value)

    @inline final def foreachMayBe(f: A => Unit): Unit =
      foreach(f)

    @inline final def foreach(f: A => Unit): Unit =
      if (value != null)
        f(value)

    @inline final def getUnsafe: A =
      if (value == null)
        throw new NoSuchElementException("MayBe.getUnsafe")
      else
        value

    @inline final def orElseMayBe[B >: A](other: => Maybe[B]): Maybe[B] =
      orElse[B](other)

    @inline final def orElse[B >: A](other: => Maybe[B]): Maybe[B] =
      if (value == null)
        other
      else
        value.asInstanceOf[Maybe[B]]

    @inline final def applyOrElse[T](onSome: A => T, onNone: => T): T =
      if (value == null)
        onNone
      else
        onSome(value)

    @inline final def getOrElseMayBe[B >: A](other: => B): B =
      getOrElse[B](other)

    @inline final def getOrElse[B >: A](other: => B): B =
      if (value == null)
        other
      else
        value

    @inline final def foldLeftMayBe[B](initial: B)(f: (A, B) => B): B =
      foldLeft[B](initial)(f)

    @inline final def foldLeft[B](initial: B)(f: (A, B) => B): B =
      if (value == null)
        initial
      else
        f(value, initial)

    @inline final def toOption: Option[A] =
      Option(value)

    @inline final def toTry: Try[A] =
      Try(value.getUnsafe)
  }
}
