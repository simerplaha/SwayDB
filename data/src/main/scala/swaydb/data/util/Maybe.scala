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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.util

import swaydb.data.util.Tagged.@@

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
