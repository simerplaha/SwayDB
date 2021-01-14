/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb

import java.util.Optional

import swaydb.data.Functions

import scala.concurrent.duration.Deadline
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

sealed trait PureFunction[+K, +V, R <: Apply[V]] {
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

object PureFunction {

  //Default function type for Map
  type Map[K, V] = PureFunction[K, V, Apply.Map[V]]

  //Default function type for Set
  type Set[K] = PureFunction[K, Nothing, Apply.Set[Nothing]]

  /** *********************************
   * **********************************
   * ******* FOR INTERNAL USE *********
   * **********************************
   * ********************************** */

  private[swaydb] def isOff[F](implicit classTag: ClassTag[F]): Boolean =
    classTag == ClassTag.Nothing || classTag == ClassTag.Unit || classTag == ClassTag.Null || classTag == ClassTag(classOf[Void])

  private[swaydb] def isOn[F](implicit classTag: ClassTag[F]): Boolean =
    !isOff(classTag)

  /**
   * Used in Java API to convert Void types to Nothing.
   */
  private[swaydb] implicit class VoidToNothing[A](voidOnKey: PureFunction[A, Void, Apply.Set[Void]]) {
    def castToNothing: PureFunction[A, Nothing, Apply.Set[Nothing]] =
      voidOnKey.asInstanceOf[PureFunction[A, Nothing, Apply.Set[Nothing]]]
  }

  private[swaydb] implicit class VoidToNothingIterable[A](voidOnKey: java.lang.Iterable[PureFunction[A, Void, Apply.Set[Void]]]) {
    def castToNothing: Iterable[PureFunction[A, Nothing, Apply.Set[Nothing]]] =
      voidOnKey.asScala.map(_.castToNothing)

    def castToNothingFunctions: Functions[PureFunction[A, Nothing, Apply.Set[Nothing]]] =
      Functions(voidOnKey.castToNothing)
  }
}

/**
 * Function types for Scala.
 *
 * Your registered functions implements one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunctionScala {

  //For Set only function. Set do not have values (Nothing).
  trait OnEntry[A] extends (A => Apply.Set[Nothing]) with PureFunction[A, Nothing, Apply.Set[Nothing]] {
    override def apply(elem: A): Apply.Set[Nothing]
  }

  trait OnEntryDeadline[A] extends ((A, Option[Deadline]) => Apply.Set[Nothing]) with PureFunction[A, Nothing, Apply.Set[Nothing]] {
    override def apply(elem: A, deadline: Option[Deadline]): Apply.Set[Nothing]
  }

  //For Map OnKey function.
  trait OnKey[K, V] extends (K => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K): Apply.Map[V]
  }

  //For Map OnKey function.
  trait OnKeyDeadline[K, V] extends ((K, Option[Deadline]) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, deadline: Option[Deadline]): Apply.Map[V]
  }

  //For Map OnKeyValue function.
  trait OnKeyValue[K, V] extends ((K, V) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, value: V): Apply.Map[V]
  }

  //For Map OnValue function.
  trait OnValue[V] extends (V => Apply.Map[V]) with PureFunction[Nothing, V, Apply.Map[V]] {
    override def apply(value: V): Apply.Map[V]
  }

  //For Map OnValue function.
  trait OnValueDeadline[V] extends ((V, Option[Deadline]) => Apply.Map[V]) with PureFunction[Nothing, V, Apply.Map[V]] {
    override def apply(value: V, deadline: Option[Deadline]): Apply.Map[V]
  }

  //For Map OnValue function.
  trait OnKeyValueDeadline[K, V] extends ((K, V, Option[Deadline]) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, value: V, deadline: Option[Deadline]): Apply.Map[V]
  }
}

/**
 * Helper function types Java so that specifying the return type is required for every function.
 *
 * Uses Expiration and Optional instead of Scala's native Deadline and Option types.
 *
 * The above scala function can still be used in Java because these extend the same parent PureFunction[K, V, R] type.
 */
object PureFunctionJava {
  /**
   * Applies to Set entries.
   */
  trait OnEntry[A] extends (A => Apply.Set[Void]) with PureFunction[A, Void, Apply.Set[Void]] {
    override def apply(elem: A): Apply.Set[Void]
  }

  trait OnEntryExpiration[A] extends ((A, Optional[Expiration]) => Apply.Set[Void]) with PureFunction[A, Void, Apply.Set[Void]] {
    override def apply(elem: A, expiration: Optional[Expiration]): Apply.Set[Void]
  }

  /**
   * Applies to a Map's key. Value is not read.
   */
  trait OnKey[K, V] extends (K => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K): Apply.Map[V]
  }

  /**
   * Applies to a Map's key. Value is not read.
   */
  trait OnKeyExpiration[K, V] extends ((K, Optional[Expiration]) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, expiration: Optional[Expiration]): Apply.Map[V]
  }

  /**
   * Applies to a Map's key. Value is not read.
   */
  trait OnValue[K, V] extends (V => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(value: V): Apply.Map[V]
  }

  /**
   * Applies to a Map's key. Value is not read.
   */
  trait OnValueExpiration[K, V] extends ((V, Optional[Expiration]) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(value: V, expiration: Optional[Expiration]): Apply.Map[V]
  }

  /**
   * Applies to a Map's key and value.
   */
  trait OnKeyValue[K, V] extends ((K, V) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, value: V): Apply.Map[V]
  }

  /**
   * Applies to a Map's key and value.
   */
  trait OnKeyValueExpiration[K, V] extends ((K, V, Optional[Expiration]) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, value: V, expiration: Optional[Expiration]): Apply.Map[V]
  }
}
