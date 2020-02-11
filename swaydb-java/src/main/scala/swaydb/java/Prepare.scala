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

  def putInMap[K, V, F](key: K, value: V): PutInMap[K, V, F] =
    PutInMap(key, value, Optional.empty())

  def putInMap[K, V, F](key: K, value: V, expireAfter: Duration): PutInMap[K, V, F] =
    PutInMap(key, value, Optional.of(expireAfter))

  def removeFromMap[K, V, F](key: K): RemoveFromMap[K, V, F] =
    RemoveFromMap(key, Optional.empty(), Optional.empty())

  def removeFromMap[K, V, F](fromKey: K, toKey: K): RemoveFromMap[K, V, F] =
    RemoveFromMap(fromKey, Optional.of(toKey), Optional.empty())

  def expireFromMap[K, V, F](key: K, after: Duration): RemoveFromMap[K, V, F] =
    RemoveFromMap(key, Optional.empty(), Optional.of(after))

  def expireFromMap[K, V, F](fromKey: K, toKey: K, after: Duration): RemoveFromMap[K, V, F] =
    RemoveFromMap(fromKey, Optional.of(toKey), Optional.of(after))

  def updateInMap[K, V, F](key: K, value: V): UpdateInMap[K, V, F] =
    UpdateInMap(key, Optional.empty(), value)

  def updateInMap[K, V, F](fromKey: K, toKey: K, value: V): UpdateInMap[K, V, F] =
    UpdateInMap(fromKey, Optional.of(toKey), value)

  def applyFunctionInMap[K, V, F](key: K, function: F): ApplyFunctionInMap[K, V, F] =
    ApplyFunctionInMap(key, Optional.empty(), function)

  def applyFunctionInMap[K, V, F](fromKey: K, toKey: K, function: F): ApplyFunctionInMap[K, V, F] =
    ApplyFunctionInMap(fromKey, Optional.of(toKey), function)

  def addToSet[T, F](elem: T): AddToSet[T, F] =
    AddToSet[T, F](elem, Optional.empty())

  def addToSet[T, F](elem: T, expireAfter: Duration): AddToSet[T, F] =
    AddToSet[T, F](elem, Optional.of(expireAfter))

  def removeFromSet[T, F](elem: T): RemoveFromSet[T, F] =
    RemoveFromSet[T, F](elem, Optional.empty(), Optional.empty())

  def removeFromSet[T, F](fromElem: T, toElem: T): RemoveFromSet[T, F] =
    RemoveFromSet[T, F](fromElem, Optional.of(toElem), Optional.empty())

  def expireFromSet[T, F](elem: T, after: Duration): RemoveFromSet[T, F] =
    RemoveFromSet[T, F](elem, Optional.empty(), Optional.of(after))

  def expireFromSet[T, F](fromElem: T, toElem: T, after: Duration): RemoveFromSet[T, F] =
    RemoveFromSet[T, F](fromElem, Optional.of(toElem), Optional.of(after))

  def applyFunctionToSet[T, F](elem: T, function: F): ApplyFunctionToSet[T, F] =
    ApplyFunctionToSet[T, F](elem, Optional.empty(), function)

  def applyFunctionToSet[T, F](fromElem: T, toElem: T, function: F): ApplyFunctionToSet[T, F] =
    ApplyFunctionToSet[T, F](fromElem, Optional.of(toElem), function)

  /**
   * Convert Prepare statements created in Java to of the type acceptable in Scala.
   *
   * This is required because of the [[PureFunction]] types in Java are different to Scala which
   * accept Scala vars.
   */
  def toScala[K, V, F <: swaydb.java.PureFunction[K, V, Return.Map[V]]](prepare: Prepare.Map[K, V, F]): swaydb.Prepare[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]] =
    prepare match {
      case PutInMap(key, value, expireAfter) =>
        new swaydb.Prepare.Put(key, value, expireAfter.asScalaMap(_.toScala.fromNow))

      case RemoveFromMap(from, to, expireAfter) =>
        new swaydb.Prepare.Remove(from, to.asScala, expireAfter.asScalaMap(_.toScala.fromNow))

      case UpdateInMap(from, to, value) =>
        new swaydb.Prepare.Update(from, to.asScala, value)

      case ApplyFunctionInMap(from, to, function) =>
        new swaydb.Prepare.ApplyFunction(from, to.asScala, PureFunction.asScala(function))
    }

  /**
   * Converts java prepare statements with [[swaydb.java.PureFunction.OnKey]] to scala prepare statements with [[swaydb.PureFunction.OnKey]]
   */
  def toScala[K, F <: swaydb.java.PureFunction.OnKey[K, Void, Return.Set[Void]]](prepare: Prepare.Set[K, F]): swaydb.Prepare[K, Nothing, swaydb.PureFunction.OnKey[K, Nothing, Apply.Set[Nothing]]] =
    prepare match {
      case AddToSet(elem, expireAfter) =>
        new swaydb.Prepare.Add(elem, expireAfter.asScalaMap(_.toScala.fromNow))

      case RemoveFromSet(from, to, expireAfter) =>
        new swaydb.Prepare.Remove(from, to.asScala, expireAfter.asScalaMap(_.toScala.fromNow))

      case ApplyFunctionToSet(from, to, function) =>
        new swaydb.Prepare.ApplyFunction(from, to.asScala, PureFunction.asScala(function))
    }

  case class PutInMap[K, V, F](key: K, value: V, expireAfter: Optional[Duration]) extends Prepare.Map[K, V, F]
  case class RemoveFromMap[K, V, F](from: K, to: Optional[K], expireAfter: Optional[Duration]) extends Prepare.Map[K, V, F]
  case class UpdateInMap[K, V, F](from: K, to: Optional[K], value: V) extends Prepare.Map[K, V, F]
  case class ApplyFunctionInMap[K, V, F](from: K, to: Optional[K], function: F) extends Prepare.Map[K, V, F]

  case class AddToSet[T, F](elem: T, expireAfter: Optional[Duration]) extends Prepare.Set[T, F]
  case class RemoveFromSet[T, F](from: T, to: Optional[T], expireAfter: Optional[Duration]) extends Prepare.Set[T, F]
  case class ApplyFunctionToSet[T, F](from: T, to: Optional[T], function: F) extends Prepare.Set[T, F]
}
