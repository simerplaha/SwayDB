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

package swaydb.java

import swaydb.utils.{KeyVal, Pair}
import swaydb.{Expiration, Prepare}

import java.time.Duration
import java.util.Optional

/**
 * Base trait for a basic SwayDB Map type.
 */
trait MapT[K, V, F] extends SetMapT[K, V] { self =>

  def remove(from: K, to: K): Unit

  def expire(from: K, to: K, after: Duration): Unit

  def expire(keys: Stream[Pair[K, java.time.Duration]]): Unit

  def expire(keys: java.lang.Iterable[Pair[K, java.time.Duration]]): Unit

  def expire(keys: java.util.Iterator[Pair[K, java.time.Duration]]): Unit

  def update(key: K, value: V): Unit

  def update(from: K, to: K, value: V): Unit

  def update(keyValues: Stream[KeyVal[K, V]]): Unit

  def update(keyValues: java.lang.Iterable[KeyVal[K, V]]): Unit

  def update(keyValues: java.util.Iterator[KeyVal[K, V]]): Unit

  def clearAppliedFunctions(): java.lang.Iterable[String]

  def clearAppliedAndRegisteredFunctions(): java.lang.Iterable[String]

  def isFunctionApplied(functionId: F): Boolean

  def applyFunction(key: K, function: F): Unit

  def applyFunction(from: K, to: K, function: F): Unit

  def commit(prepare: Stream[Prepare[K, V, F]]): Unit

  def commit(prepare: java.lang.Iterable[Prepare[K, V, F]]): Unit

  def getKeyDeadline(key: K): Optional[Pair[K, Optional[Expiration]]]

  def getKeyValueDeadline(key: K): Optional[Pair[KeyVal[K, V], Optional[Expiration]]]

  def mightContainFunction(function: F): Boolean

  def keySize(key: K): Int

  def valueSize(value: V): Int
}
