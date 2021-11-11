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

import java.time.Duration
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration}

sealed trait Prepare[+K, +V, +F]

object Prepare {

  /**
   * Map [[Prepare]] statements to be used from Java.
   */
  def put[K, V, F](key: K, value: V): Prepare[K, V, F] =
    swaydb.Prepare.Put(key, value)

  def put[K, V, F](key: K, value: V, expireAfter: Duration): Prepare[K, V, F] =
    swaydb.Prepare.Put(key, value, expireAfter.toScala)

  def removeFromMap[K, V, F](key: K): Prepare[K, V, F] =
    swaydb.Prepare.Remove(key)

  def removeFromMap[K, V, F](fromKey: K, toKey: K): Prepare[K, V, F] =
    swaydb.Prepare.Remove(fromKey, toKey)

  def expireFromMap[K, V, F](key: K, after: Duration): Prepare[K, V, F] =
    swaydb.Prepare.Expire(key, after.toScala)

  def expireFromMap[K, V, F](fromKey: K, toKey: K, after: Duration): Prepare[K, V, F] =
    swaydb.Prepare.Expire(fromKey, toKey, after.toScala)

  def update[K, V, F](key: K, value: V): Prepare[K, V, F] =
    swaydb.Prepare.Update(key, value)

  def update[K, V, F](fromKey: K, toKey: K, value: V): Prepare[K, V, F] =
    swaydb.Prepare.Update(fromKey, toKey, value)

  def applyMapFunction[K, V, F](key: K, function: F): Prepare[K, V, F] =
    swaydb.Prepare.ApplyFunction(key, function)

  def applyMapFunction[K, V, F](fromKey: K, toKey: K, function: F): Prepare[K, V, F] =
    swaydb.Prepare.ApplyFunction(fromKey, toKey, function)

  /**
   * Set [[Prepare]] statements to be used from Java.
   */
  def add[T, F](elem: T): Prepare[T, Void, F] =
    swaydb.Prepare.Add(elem)

  def add[T, F](elem: T, expireAfter: Duration): Prepare[T, Void, F] =
    swaydb.Prepare.Add(elem, expireAfter.toScala)

  def removeFromSet[T, F](elem: T): Prepare[T, Void, F] =
    swaydb.Prepare.Remove(elem)

  def removeFromSet[T, F](fromElem: T, toElem: T): Prepare[T, Void, F] =
    swaydb.Prepare.Remove(fromElem, toElem)

  def expireFromSet[T, F](elem: T, after: Duration): Prepare[T, Void, F] =
    swaydb.Prepare.Expire(elem, after.toScala)

  def expireFromSet[T, F](fromElem: T, toElem: T, after: Duration): Prepare[T, Void, F] =
    swaydb.Prepare.Expire(fromElem, toElem, after.toScala)

  def applySetFunction[T, F](elem: T, function: F): Prepare[T, Void, F] =
    swaydb.Prepare.ApplyFunction(elem, function)

  def applySetFunction[T, F](fromElem: T, toElem: T, function: F): Prepare[T, Void, F] =
    swaydb.Prepare.ApplyFunction(fromElem, toElem, function)

  object Put {
    def apply[K, V](key: K, value: V): Put[K, V] =
      new Put(key, value, None)

    def apply[K, V](key: K, value: V, expireAfter: FiniteDuration): Put[K, V] =
      new Put(key, value, Some(expireAfter.fromNow))

    def apply[K, V](key: K, value: V, expireAt: Deadline): Put[K, V] =
      new Put(key, value, Some(expireAt))
  }

  /**
   * Scala API for creating [[Prepare]] statements.
   */
  object Remove {
    def apply[K](key: K): Remove[K] =
      new Remove(key, None, None)

    def apply[K](from: K, to: K): Remove[K] =
      new Remove(from, Some(to), None)
  }

  object Expire {
    def apply[K](key: K, after: FiniteDuration): Remove[K] =
      new Remove(key, None, Some(after.fromNow))

    def apply[K](from: K, to: K, after: FiniteDuration): Remove[K] =
      new Remove(from, Some(to), Some(after.fromNow))

    def apply[K](key: K, at: Deadline): Remove[K] =
      new Remove(key, None, Some(at))

    def apply[K](from: K, to: K, at: Deadline): Remove[K] =
      new Remove(from, Some(to), Some(at))
  }

  object Update {
    def apply[K, V](key: K, value: V): Update[K, V] =
      new Update(key, None, value)

    def apply[K, V](from: K, to: K, value: V): Update[K, V] =
      new Update(from, Some(to), value)
  }

  object ApplyFunction {
    def apply[K, F](key: K, function: F): ApplyFunction[K, F] =
      new ApplyFunction(key, None, function)

    def apply[K, F](from: K, to: K, function: F): ApplyFunction[K, F] =
      new ApplyFunction(from, Some(to), function)
  }

  object Add {
    def apply[T](elem: T): Add[T] =
      new Add(elem, None)

    def apply[T](elem: T, expireAfter: FiniteDuration): Add[T] =
      new Add(elem, Some(expireAfter.fromNow))

    def apply[T](elem: T, expireAt: Deadline): Add[T] =
      new Add(elem, Some(expireAt))
  }

  case class Put[K, V](key: K, value: V, deadline: Option[Deadline]) extends Prepare[K, V, Nothing]
  case class Remove[K](from: K, to: Option[K], deadline: Option[Deadline]) extends Prepare[K, Nothing, Nothing]
  case class Update[K, V](from: K, to: Option[K], value: V) extends Prepare[K, V, Nothing]
  case class ApplyFunction[K, F](from: K, to: Option[K], function: F) extends Prepare[K, Nothing, F]
  case class Add[T](elem: T, deadline: Option[Deadline]) extends Prepare[T, Nothing, Nothing]
}
