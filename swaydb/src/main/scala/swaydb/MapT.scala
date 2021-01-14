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

import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Base trait for a basic SwayDB Map type.
 */
trait MapT[K, V, F, BAG[_]] extends SetMapT[K, V, BAG] { self =>

  def remove(from: K, to: K): BAG[OK]

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK]

  def expire(from: K, to: K, at: Deadline): BAG[OK]

  def expire(keys: (K, Deadline)*): BAG[OK]

  def expire(keys: Stream[(K, Deadline), BAG]): BAG[OK]

  def expire(keys: Iterable[(K, Deadline)]): BAG[OK]

  def expire(keys: Iterator[(K, Deadline)]): BAG[OK]

  def update(key: K, value: V): BAG[OK]

  def update(from: K, to: K, value: V): BAG[OK]

  def update(keyValues: (K, V)*): BAG[OK]

  def update(keyValues: Stream[(K, V), BAG]): BAG[OK]

  def update(keyValues: Iterable[(K, V)]): BAG[OK]

  def update(keyValues: Iterator[(K, V)]): BAG[OK]

  def clearAppliedFunctions(): BAG[Iterable[String]]

  def clearAppliedAndRegisteredFunctions(): BAG[Iterable[String]]

  //using evidence instance of defining the sub-type in type param F so that we can
  //use void in java and provide better interop with Scala and Java conversions.
  def isFunctionApplied(function: F)(implicit evd: F <:< PureFunction.Map[K, V]): Boolean

  def applyFunction(key: K, function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[OK]

  def applyFunction(from: K, to: K, function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[OK]

  def commit(prepare: Prepare[K, V, F]*): BAG[OK]

  def commit(prepare: Stream[Prepare[K, V, F], BAG]): BAG[OK]

  def commit(prepare: Iterable[Prepare[K, V, F]]): BAG[OK]

  def commit(prepare: Iterator[Prepare[K, V, F]]): BAG[OK]

  def getKeyDeadline(key: K): BAG[Option[(K, Option[Deadline])]]

  def getKeyValueDeadline(key: K): BAG[Option[((K, V), Option[Deadline])]]

  def mightContainFunction(function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[Boolean]

  def keySize(key: K): Int

  def valueSize(value: V): Int
}
