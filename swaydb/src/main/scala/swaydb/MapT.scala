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

import swaydb.OK

import scala.collection.compat.IterableOnce
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

  def expire(keys: IterableOnce[(K, Deadline)]): BAG[OK]

  def update(key: K, value: V): BAG[OK]

  def update(from: K, to: K, value: V): BAG[OK]

  def update(keyValues: (K, V)*): BAG[OK]

  def update(keyValues: Stream[(K, V), BAG]): BAG[OK]

  def update(keyValues: IterableOnce[(K, V)]): BAG[OK]

  def clearAppliedFunctions(): BAG[Iterable[String]]

  def clearAppliedAndRegisteredFunctions(): BAG[Iterable[String]]

  //using evidence instance of defining the sub-type in type param F so that we can
  //use void in java and provide better interop with Scala and Java conversions.
  def isFunctionApplied(function: F)(implicit evd: F <:< PureFunction.Map[K, V]): Boolean

  def applyFunction(key: K, function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[OK]

  def applyFunction(from: K, to: K, function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[OK]

  def commit(prepare: Prepare[K, V, F]*): BAG[OK]

  def commit(prepare: Stream[Prepare[K, V, F], BAG]): BAG[OK]

  def commit(prepare: IterableOnce[Prepare[K, V, F]]): BAG[OK]

  def getKeyDeadline(key: K): BAG[Option[(K, Option[Deadline])]]

  def getKeyValueDeadline(key: K): BAG[Option[((K, V), Option[Deadline])]]

  def mightContainFunction(function: F)(implicit evd: F <:< PureFunction.Map[K, V]): BAG[Boolean]

  def keySize(key: K): Int

  def valueSize(value: V): Int
}
