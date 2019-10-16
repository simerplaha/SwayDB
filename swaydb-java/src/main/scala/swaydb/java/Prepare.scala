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

import swaydb.Apply
import swaydb.java.data.util.Java._

import scala.compat.java8.DurationConverters._

object Prepare {

  sealed trait Map[+K, +V, +F]
  sealed trait Set[+K, +F]

  def putInMap[K, V, F](key: K, value: V): Map.Put[K, V, F] =
    Map.Put(key, value, Optional.empty())

  def putInMap[K, V, F](key: K, value: V, expireAfter: Duration): Map.Put[K, V, F] =
    Map.Put(key, value, Optional.of(expireAfter))

  def removeFromMap[K, V, F](key: K): Map.Remove[K, V, F] =
    Map.Remove(key, Optional.empty(), Optional.empty())

  def removeFromMap[K, V, F](fromKey: K, toKey: K): Map.Remove[K, V, F] =
    Map.Remove(fromKey, Optional.of(toKey), Optional.empty())

  def expireFromMap[K, V, F](key: K, after: Duration): Map.Remove[K, V, F] =
    Map.Remove(key, Optional.empty(), Optional.of(after))

  def expireFromMap[K, V, F](fromKey: K, toKey: K, after: Duration): Map.Remove[K, V, F] =
    Map.Remove(fromKey, Optional.of(toKey), Optional.of(after))

  def updateInMap[K, V, F](key: K, value: V): Map.Update[K, V, F] =
    Map.Update(key, Optional.empty(), value)

  def updateInMap[K, V, F](fromKey: K, toKey: K, value: V): Map.Update[K, V, F] =
    Map.Update(fromKey, Optional.of(toKey), value)

  def applyFunctionInMap[K, V, F](key: K, function: F): Map.ApplyFunction[K, V, F] =
    Map.ApplyFunction(key, Optional.empty(), function)

  def applyFunctionInMap[K, V, F](fromKey: K, toKey: K, function: F): Map.ApplyFunction[K, V, F] =
    Map.ApplyFunction(fromKey, Optional.of(toKey), function)

  def addToSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T): Set.Add[T, F] =
    Set.Add[T, F](elem, Optional.empty())

  def addToSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T, expireAfter: Duration): Set.Add[T, F] =
    Set.Add[T, F](elem, Optional.of(expireAfter))

  def removeFromSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T): Set.Remove[T, F] =
    Set.Remove[T, F](elem, Optional.empty(), Optional.empty())

  def removeFromSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](fromElem: T, toElem: T): Set.Remove[T, F] =
    Set.Remove[T, F](fromElem, Optional.of(toElem), Optional.empty())

  def expireFromSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T, after: Duration): Set.Remove[T, F] =
    Set.Remove[T, F](elem, Optional.empty(), Optional.of(after))

  def expireFromSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](fromElem: T, toElem: T, after: Duration): Set.Remove[T, F] =
    Set.Remove[T, F](fromElem, Optional.of(toElem), Optional.of(after))

  def applyFunctionInSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T, function: F): Set.ApplyFunction[T, F] =
    Set.ApplyFunction[T, F](elem, Optional.empty(), function)

  def applyFunctionInSet[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](fromElem: T, toElem: T, function: F): Set.ApplyFunction[T, F] =
    Set.ApplyFunction[T, F](fromElem, Optional.of(toElem), function)

  /**
   * Convert Prepare statements created in Java to of the type acceptable in Scala.
   *
   * This is required because of the [[PureFunction]] types in Java are different to Scala which
   * accept Scala vars.
   */
  def toScala[K, V, F <: swaydb.java.PureFunction[K, V, Return.Map[V]]](prepare: Prepare.Map[K, V, F]): swaydb.Prepare[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]] =
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
  def toScala[K, F <: swaydb.java.PureFunction.OnKey[K, Void, Return.Set[Void]]](prepare: Prepare.Set[K, F]): swaydb.Prepare[K, Nothing, swaydb.PureFunction.OnKey[K, Nothing, Apply.Set[Nothing]]] =
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
    case class Add[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](elem: T, expireAfter: Optional[Duration]) extends Prepare.Set[T, F]
    case class Remove[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](from: T, to: Optional[T], expireAfter: Optional[Duration]) extends Prepare.Set[T, F]
    case class ApplyFunction[T, F <: swaydb.java.PureFunction.OnKey[T, Void, Return.Set[Void]]](from: T, to: Optional[T], function: F) extends Prepare.Set[T, F]
  }
}
