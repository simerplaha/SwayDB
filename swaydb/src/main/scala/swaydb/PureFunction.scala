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

/**
 * Function types for Scala.
 *
 * Your registered functions implements one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunction {

  //Default function type for Map
  type Map[K, V] = PureFunction[K, V, Apply.Map[V]]

  //Default function type for Set
  type Set[K] = PureFunction[K, Nothing, Apply.Set[Nothing]]

  //type alias for Set OnKey function. Set do not have values (Nothing).
  type OnSetEntry[A] = Key[A, Nothing, Apply.Set[Nothing]]

  //type alias for Map OnKey function.
  type OnMapKey[K, V] = Key[K, V, Apply.Map[V]]

  //type alias for Map OnValue function.
  type OnMapValue[V] = Value[V, Apply.Map[V]]

  //type alias for Map OnKeyValue function.
  type OnMapKeyValue[K, V] = KeyValue[K, V, Apply.Map[V]]

  /** ******************************************************************************
   * *******************************************************************************
   * ******* Use the types prefixed with On* for a shorter function syntax. ********
   * *******************************************************************************
   * * ***************************************************************************** */

  /**
   * Fetches only the key and deadline ignoring the value.
   */
  trait Key[K, V, R <: Apply[V]] extends ((K, Option[Deadline]) => R) with PureFunction[K, V, R] {
    override def apply(key: K, deadline: Option[Deadline]): R
  }

  /**
   * Applies to value.
   */
  trait Value[V, R <: Apply[V]] extends (V => R) with PureFunction[Nothing, V, R] {
    override def apply(value: V): R
  }


  /**
   * Fetches the key, value and deadline.
   */
  trait KeyValue[K, V, R <: Apply[V]] extends ((K, V, Option[Deadline]) => R) with PureFunction[K, V, R] {
    override def apply(key: K, value: V, deadline: Option[Deadline]): R
  }


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
  trait OnSet[K] extends ((K, Optional[Expiration]) => Apply.Set[Void]) with PureFunction[K, Void, Apply.Set[Void]] {
    override def apply(key: K, expiration: Optional[Expiration]): Apply.Set[Void]
  }

  /**
   * Applies to a Map's key. Value is not read.
   */
  trait OnMapKey[K, V] extends ((K, Optional[Expiration]) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, expiration: Optional[Expiration]): Apply.Map[V]
  }

  /**
   * Applies to a Map's key. Value is not read.
   */
  trait OnMapValue[K, V] extends (V => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(value: V): Apply.Map[V]
  }

  /**
   * Applies to a Map's key and value.
   */
  trait OnMapKeyValue[K, V] extends ((K, V, Optional[Expiration]) => Apply.Map[V]) with PureFunction[K, V, Apply.Map[V]] {
    override def apply(key: K, value: V, expiration: Optional[Expiration]): Apply.Map[V]
  }
}
