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

import swaydb.data.Functions

import scala.concurrent.duration.Deadline
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

sealed trait PureFunction[+K, +V, +R <: Apply[V]] {
  /**
   * This unique [[id]] of this function.
   *
   * It is stored in the database and should be unique to each function.
   *
   * This can simply be the full class name if your application does not
   * have conflict package names and class names.
   *
   * NOTE - Missing functions are reported/logged along with this [[id]] to
   * make debugging easier.
   *
   * @return a unique id for each function.
   */
  def id: String =
    this.getClass.getName
}

/**
 * Function types for SwayDB.
 *
 * Your registered functions should implement one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunction {

  type Map[K, V] = PureFunction[K, V, Apply.Map[V]]

  type Set[A] = PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]

  trait OnValue[V, R <: Apply[V]] extends (V => R) with PureFunction[Nothing, V, R] {
    override def apply(value: V): R
  }

  trait OnKey[K, +V, R <: Apply[V]] extends ((K, Option[Deadline]) => R) with PureFunction[K, V, R] {
    override def apply(key: K, deadline: Option[Deadline]): R
  }

  trait OnKeyValue[K, V, R <: Apply[V]] extends ((K, V, Option[Deadline]) => R) with PureFunction[K, V, R] {
    override def apply(key: K, value: V, deadline: Option[Deadline]): R
  }

  def isOff[F](implicit classTag: ClassTag[F]): Boolean =
    classTag == ClassTag.Nothing || classTag == ClassTag.Unit || classTag == ClassTag.Null || classTag == ClassTag(classOf[Void])

  def isOn[F](implicit classTag: ClassTag[F]): Boolean =
    !isOff(classTag)

  implicit class VoidToNothing[A](voidOnKey: PureFunction.OnKey[A, Void, Apply.Set[Void]]) {
    def castToNothing: PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]] =
      voidOnKey.asInstanceOf[PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]]
  }

  implicit class VoidToNothingIterable[A](voidOnKey: java.lang.Iterable[PureFunction.OnKey[A, Void, Apply.Set[Void]]]) {
    def castToNothing: Iterable[OnKey[A, Nothing, Apply.Set[Nothing]]] =
      voidOnKey.asScala.map(_.castToNothing)

    def castToNothingFunctions: Functions[OnKey[A, Nothing, Apply.Set[Nothing]]] =
      Functions(voidOnKey.castToNothing)
  }

}
