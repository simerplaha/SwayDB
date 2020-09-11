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

package swaydb.java

import java.time.Duration
import java.util.Optional

import swaydb.{KeyVal, OK, Pair, Prepare}

/**
 * Base trait for a basic SwayDB Map type.
 */
trait MapT[K, V, F] extends SetMapT[K, V] { self =>

  def remove(from: K, to: K): OK

  def expire(from: K, to: K, after: Duration): OK

  def expire(keys: Stream[Pair[K, java.time.Duration]]): OK

  def expire(keys: java.lang.Iterable[Pair[K, java.time.Duration]]): OK

  def expire(keys: java.util.Iterator[Pair[K, java.time.Duration]]): OK

  def update(key: K, value: V): OK

  def update(from: K, to: K, value: V): OK

  def update(keyValues: Stream[KeyVal[K, V]]): OK

  def update(keyValues: java.lang.Iterable[KeyVal[K, V]]): OK

  def update(keyValues: java.util.Iterator[KeyVal[K, V]]): OK

  def clearAppliedFunctions(): java.lang.Iterable[String]

  def clearAppliedAndRegisteredFunctions(): java.lang.Iterable[String]

  def isFunctionApplied(functionId: F): java.lang.Boolean

  def applyFunction(key: K, function: F): OK

  def applyFunction(from: K, to: K, function: F): OK

  def commit(prepare: Stream[Prepare[K, V, F]]): OK

  def commit(prepare: java.lang.Iterable[Prepare[K, V, F]]): OK

  def getKeyDeadline(key: K): Optional[Pair[K, Optional[Expiration]]]

  def getKeyValueDeadline(key: K): Optional[Pair[KeyVal[K, V], Optional[Expiration]]]

  def mightContainFunction(function: F): java.lang.Boolean

  def keySize(key: K): Int

  def valueSize(value: V): Int
}
