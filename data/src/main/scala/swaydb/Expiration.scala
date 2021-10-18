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
import java.util.Optional

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.Deadline

object Expiration {
  def of(scalaDeadline: Deadline) =
    new Expiration(scalaDeadline)

  @inline def apply(scalaDeadline: Option[Deadline]): Optional[Expiration] =
    of(scalaDeadline)

  def of(scalaDeadline: Option[Deadline]): Optional[Expiration] =
    scalaDeadline match {
      case Some(deadline) =>
        Optional.of(new Expiration(deadline))

      case None =>
        Optional.empty()
    }
}

case class Expiration(asScala: Deadline) {

  def timeLeft: Duration =
    asScala.timeLeft.toJava

  def time: Duration =
    asScala.time.toJava

  def plus(time: Duration): Expiration =
    Expiration(asScala + time.toScala)

  def minus(time: Duration): Expiration =
    Expiration(asScala - time.toScala)

  def compare(expiration: Expiration): Int =
    asScala.compare(expiration.asScala)

  def hasTimeLeft: Boolean =
    asScala.hasTimeLeft()

  def isOverdue: Boolean =
    asScala.isOverdue()
}
