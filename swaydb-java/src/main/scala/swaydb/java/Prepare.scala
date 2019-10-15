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

import swaydb.{Prepare => ScalaPrepare}

import scala.compat.java8.DurationConverters._

object Prepare {

  def put[K, V](key: K, value: V): ScalaPrepare.Put[K, V] =
    ScalaPrepare.Put(key, value)

  def put[K, V](key: K, value: V, expireAfter: Duration): ScalaPrepare.Put[K, V] =
    ScalaPrepare.Put(key, value, expireAfter.toScala)

  def remove[K](key: K): ScalaPrepare.Remove[K] =
    ScalaPrepare.Remove(key)

  def remove[K](fromKey: K, toKey: K): ScalaPrepare.Remove[K] =
    ScalaPrepare.Remove(fromKey, toKey)

  def expire[K](key: K, after: Duration): ScalaPrepare.Remove[K] =
    ScalaPrepare.Expire(key, after.toScala)

  def expire[K](fromKey: K, toKey: K, after: Duration): ScalaPrepare.Remove[K] =
    ScalaPrepare.Expire(fromKey, toKey, after.toScala)

  def update[K, V](key: K, value: V): ScalaPrepare.Update[K, V] =
    ScalaPrepare.Update(key, value)

  def update[K, V](fromKey: K, toKey: K, value: V): ScalaPrepare.Update[K, V] =
    ScalaPrepare.Update(fromKey, toKey, value)

  def applyFunction[K, F](key: K, function: F): ScalaPrepare.ApplyFunction[K, F] =
    ScalaPrepare.ApplyFunction(key, function)

  def applyFunction[K, F](fromKey: K, toKey: K, function: F): ScalaPrepare.ApplyFunction[K, F] =
    ScalaPrepare.ApplyFunction(fromKey, toKey, function)

  /**
   * Convert Prepare statements created in Java to of the type acceptable in Scala.
   *
   * This is required because of the [[PureFunction]] types in Java are different to Scala which
   * accept Scala vars.
   */
  def toScala[K, V, F <: swaydb.java.PureFunction[K, V]](prepare: swaydb.Prepare[K, V, F]): swaydb.Prepare[K, V, swaydb.PureFunction[K, V]] =
    prepare match {
      case prepare: swaydb.Prepare.Put[K, V] =>
        prepare

      case prepare: swaydb.Prepare.Remove[K] =>
        prepare

      case prepare: swaydb.Prepare.Update[K, V] =>
        prepare

      case prepare: swaydb.Prepare.ApplyFunction[K, F] =>
        val scalaFunction = PureFunction.asScala(prepare.function)
        swaydb.Prepare.ApplyFunction(prepare.from, prepare.to, scalaFunction)

      case prepare: swaydb.Prepare.Add[K] =>
        prepare
    }

  /**
   * Converts java prepare statements with [[swaydb.java.PureFunction.OnKey]] to scala prepare statements with [[swaydb.PureFunction.OnKey]]
   */
  def toScalaForSet[K, F <: swaydb.java.PureFunction.OnKey[K, java.lang.Void]](prepare: swaydb.Prepare[K, java.lang.Void, F]): swaydb.Prepare[K, Nothing, swaydb.PureFunction.OnKey[K, Nothing]] =
    prepare match {
      case prepare: swaydb.Prepare.Put[K, _] =>
        prepare.asInstanceOf[swaydb.Prepare.Put[K, Nothing]]

      case prepare: swaydb.Prepare.Remove[K] =>
        prepare.asInstanceOf[swaydb.Prepare.Remove[K]]

      case prepare: swaydb.Prepare.Update[K, _] =>
        prepare.asInstanceOf[swaydb.Prepare.Update[K, Nothing]]

      case prepare: swaydb.Prepare.ApplyFunction[K, F] =>
        val scalaFunction: swaydb.PureFunction.OnKey[K, Nothing] = PureFunction.asScala(prepare.function)
        swaydb.Prepare.ApplyFunction(prepare.from, prepare.to, scalaFunction)

      case prepare: swaydb.Prepare.Add[K] =>
        prepare
    }
}
