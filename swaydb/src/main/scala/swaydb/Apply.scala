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

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Output for [[PureFunction]] instances.
 */
sealed trait Apply[+V]
object Apply {

  def nothingOnMap[V]: Apply.Map[V] =
    Nothing

  def removeFromMap[V]: Apply.Map[V] =
    Remove

  def update[V](value: V): Apply.Update[V] =
    Apply.Update(value)

  def update[V](value: V, expire: java.time.Duration): Apply.Update[V] =
    Apply.Update(value, expire.toScala)

  def expireFromMap[V](expire: java.time.Duration): Apply.Map[V] =
    Expire(expire.toScala)

  def nothingOnSet[V]: Apply.Set[V] =
    Nothing.asInstanceOf[Apply.Set[V]]

  def removeFromSet[V]: Apply.Set[V] =
    Remove.asInstanceOf[Apply.Set[V]]

  def expireFromSet[V](expire: java.time.Duration): Apply.Set[V] =
    Expire(expire.toScala).asInstanceOf[Apply.Set[V]]

  /**
   * Function outputs for Map
   */
  object Map {
    @inline def toOption[V](value: Map[V]): Map[Option[V]] =
      value match {
        case Apply.Nothing =>
          Apply.Nothing

        case Apply.Remove =>
          Apply.Remove

        case expire @ Apply.Expire(_) =>
          expire

        case Apply.Update(value, deadline) =>
          Apply.Update(Some(value), deadline)
      }
  }

  sealed trait Map[+V] extends Apply[V] {
    @inline def mapValue[B](f: V => B): Apply.Map[B] =
      this match {
        case Nothing =>
          Nothing

        case Remove =>
          Remove

        case expire: Expire =>
          expire

        case Update(value, deadline) =>
          Apply.Update(f(value), deadline)
      }
  }

  implicit class SetNothingToMap[S](set: Set[Nothing]) {
    @inline def toMap[V]: Apply.Map[V] =
      set match {
        case Nothing =>
          Nothing

        case Remove =>
          Remove

        case expire: Expire =>
          expire
      }
  }

  implicit class SetVoidToMap(set: Set[Void]) {
    @inline def toMap[V]: Apply.Map[V] =
      set
        .asInstanceOf[Set[Nothing]]
        .toMap
  }

  /**
   * Function outputs for Set
   */
  sealed trait Set[V] extends Apply[V]

  case object Nothing extends Map[Nothing] with Set[Nothing]
  case object Remove extends Map[Nothing] with Set[Nothing]
  object Expire {
    def apply(after: FiniteDuration): Expire =
      new Expire(after.fromNow)
  }

  final case class Expire(deadline: Deadline) extends Map[Nothing] with Set[Nothing]

  object Update {
    def apply[V](value: V): Update[V] =
      new Update(value, None)

    def apply[V](value: V, expireAfter: FiniteDuration): Update[V] =
      new Update(value, Some(expireAfter.fromNow))

    def apply[V](value: V, expireAt: Deadline): Update[V] =
      new Update(value, Some(expireAt))
  }

  final case class Update[V](value: V, deadline: Option[Deadline]) extends Map[V]
}
