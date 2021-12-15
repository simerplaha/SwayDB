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

import swaydb.config.Functions

import java.util.Optional
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
