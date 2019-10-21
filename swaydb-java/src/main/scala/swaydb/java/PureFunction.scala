/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.java

import java.util.Optional

import swaydb.Apply
import swaydb.java.data.slice.Slice
import swaydb.java.data.util.Java._

import scala.concurrent.duration

sealed trait PureFunction[+K, +V, +R <: Return[V]] {
  /**
   * This unique [[id]] of this function.
   *
   * It is stored in the database and should be unique to each function.
   *
   * This can simply be the full class name if your application does not
   * have conflict package names and class names.
   *
   * For example:
   * {{{
   *   Slice.writeString(this.getClass.getCanonicalName)
   * }}}
   *
   * @return a unique id for each function.
   */
  def id: Slice[java.lang.Byte] =
    Slice.writeString(this.getClass.getName)
}

/**
 * Function types for SwayDB.
 *
 * Your registered functions ([[MapIO.registerFunction]]) should implement one of the these functions that
 * informs SwayDB of target data for the on the applied key should be read to execute the function.
 */
object PureFunction {

  def asScala[K, V, R <: Return.Map[V]](function: PureFunction[K, V, R]): swaydb.PureFunction[K, V, Apply.Map[V]] =
    function match {
      case _: VoidM[_, _] =>

        /**
         * currently there is no type-safe way to omit this type from [[MapIO]] & [[SetIO]] classes.
         *
         * These functions work very well in Scala but in Scala to add safety to user's APIs [[VoidM]] type is required.
         */
        throw new Exception("Disabled function type cannot be applied.")

      case function: Enabled[K, V, R] =>
        function match {
          case function: PureFunction.OnValue[K, V, Return.Map[V]] =>
            new swaydb.PureFunction.OnValue[V, Apply.Map[V]] {
              override def id: ScalaSlice[Byte] =
                function.id.asScala.asInstanceOf[ScalaSlice[Byte]]

              override def apply(value: V): Apply.Map[V] =
                Return.toScalaMap(function.apply(value))
            }

          case function: PureFunction.OnKey[K, V, Return.Map[V]] =>
            new swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] {
              override def id: ScalaSlice[Byte] =
                function.id.asScala.asInstanceOf[ScalaSlice[Byte]]

              override def apply(key: K, deadline: Option[scala.concurrent.duration.Deadline]): Apply.Map[V] =
                Return.toScalaMap(function.apply(key, deadline.asJavaMap(_.asJava)))
            }

          case function: PureFunction.OnKeyValue[K, V, Return.Map[V]] =>
            new swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] {
              override def id: ScalaSlice[Byte] =
                function.id.asScala.asInstanceOf[ScalaSlice[Byte]]

              override def apply(key: K, value: V, deadline: Option[scala.concurrent.duration.Deadline]): Apply.Map[V] =
                Return.toScalaMap(function.apply(key, value, deadline.asJavaMap(_.asJava)))
            }
        }

    }

  def asScala[K, R <: Return.Set[Void]](function: PureFunction.OnKey[K, Void, R]): swaydb.PureFunction.OnKey[K, Nothing, Apply.Set[Nothing]] =
    new swaydb.PureFunction.OnKey[K, Nothing, Apply.Set[Nothing]] {
      override def id: ScalaSlice[Byte] =
        function.id.asScala.asInstanceOf[ScalaSlice[Byte]]

      override def apply(key: K, deadline: Option[duration.Deadline]): Apply.Set[Nothing] =
        Return.toScalaSet(function.apply(key, deadline.asJavaMap(_.asJava)))
    }

  /**
   * Type used to indicate disabled functions for [[swaydb.java.MapIO]]
   */
  final abstract class VoidM[K, V] extends PureFunction[K, V, Return.Map[V]]

  /**
   * Type used to indicate disabled functions for [[swaydb.java.SetIO]]
   */
  final abstract class VoidS[K] extends PureFunction.OnKey[K, Void, Return.Set[Void]]

  sealed trait Enabled[K, V, R <: Return[V]] extends PureFunction[K, V, R]

  @FunctionalInterface
  trait OnValue[K, V, R <: Return[V]] extends Enabled[K, V, R] { self =>
    def apply(value: V): R
  }

  @FunctionalInterface
  trait OnKey[K, V, R <: Return[V]] extends Enabled[K, V, R] { self =>
    def apply(key: K, deadline: Optional[swaydb.java.Deadline]): R
  }

  @FunctionalInterface
  trait OnKeyValue[K, V, R <: Return[V]] extends Enabled[K, V, R] { self =>
    def apply(key: K, value: V, deadline: Optional[Deadline]): R
  }
}
