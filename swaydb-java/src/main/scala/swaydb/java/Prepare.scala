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

import java.time.Duration
import java.util.Optional

import swaydb.java.data.util.Java._

import scala.compat.java8.DurationConverters._

object Prepare {

  sealed trait Map[+K, +V, +F]
  sealed trait Set[+K, +F]

  /**
   * Convert Prepare statements created in Java to of the type acceptable in Scala.
   *
   * This is required because of the [[PureFunction]] types in Java are different to Scala which
   * accept Scala vars.
   */
  def toScala[K, V, F <: swaydb.java.PureFunction[K, V, swaydb.Apply.Map[V]]](prepare: Prepare.Map[K, V, F]): swaydb.Prepare[K, V, swaydb.PureFunction[K, V, swaydb.Apply.Map[V]]] =
    prepare match {
      case Map.Put(key, value, expireAfter) =>
        new swaydb.Prepare.Put(key, value, expireAfter.asScalaMap(_.toScala.fromNow))

      case Map.Remove(from, to, expireAfter) =>
        new swaydb.Prepare.Remove(from, to.asScala, expireAfter.asScalaMap(_.toScala.fromNow))

      case Map.Update(from, to, value) =>
        new swaydb.Prepare.Update(from, to.asScala, value)

      case Map.ApplyFunction(from, to, function) =>
        new swaydb.Prepare.ApplyFunction(from, to.asScala, PureFunction.asScala(function))
    }

  /**
   * Converts java prepare statements with [[swaydb.java.PureFunction.OnKey]] to scala prepare statements with [[swaydb.PureFunction.OnKey]]
   */
  def toScala[K, F <: swaydb.java.PureFunction.OnKey[K, Void, swaydb.Apply.Set[Void]]](prepare: Prepare.Set[K, F]): swaydb.Prepare[K, Nothing, swaydb.PureFunction.OnKey[K, Nothing, swaydb.Apply.Set[Nothing]]] =
    prepare match {
      case Set.Add(elem, expireAfter) =>
        new swaydb.Prepare.Add(elem, expireAfter.asScalaMap(_.toScala.fromNow))

      case Set.Remove(from, to, expireAfter) =>
        new swaydb.Prepare.Remove(from, to.asScala, expireAfter.asScalaMap(_.toScala.fromNow))

      case Set.ApplyFunction(from, to, function) =>
        new swaydb.Prepare.ApplyFunction(from, to.asScala, PureFunction.asScala(function))
    }

  object Map {
    case class Put[K, V, F](key: K, value: V, expireAfter: Optional[Duration]) extends Prepare.Map[K, V, F]
    case class Remove[K, V, F](from: K, to: Optional[K], expireAfter: Optional[Duration]) extends Prepare.Map[K, V, F]
    case class Update[K, V, F](from: K, to: Optional[K], value: V) extends Prepare.Map[K, V, F]
    case class ApplyFunction[K, V, F](from: K, to: Optional[K], function: F) extends Prepare.Map[K, V, F]
  }

  object Set {
    case class Add[T, F <: swaydb.java.PureFunction.OnKey[T, Void, swaydb.Apply.Set[Void]]](elem: T, expireAfter: Optional[Duration]) extends Prepare.Set[T, F]
    case class Remove[T, F <: swaydb.java.PureFunction.OnKey[T, Void, swaydb.Apply.Set[Void]]](from: T, to: Optional[T], expireAfter: Optional[Duration]) extends Prepare.Set[T, F]
    case class ApplyFunction[T, F <: swaydb.java.PureFunction.OnKey[T, Void, swaydb.Apply.Set[Void]]](from: T, to: Optional[T], function: F) extends Prepare.Set[T, F]
  }
}
