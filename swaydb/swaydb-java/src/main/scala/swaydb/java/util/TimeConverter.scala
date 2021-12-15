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

package swaydb.java.util

import swaydb.Expiration

import java.time.Duration
import java.util.Optional
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration}

object TimeConverter {

  def toDuration(finiteDuration: FiniteDuration): java.time.Duration =
    finiteDuration.toJava

  def toDuration(finiteDuration: Option[FiniteDuration]): Optional[Duration] =
    finiteDuration match {
      case Some(value) =>
        Optional.of(value.toJava)

      case None =>
        Optional.empty()
    }

  def toExpiration(deadline: Deadline): Expiration =
    new Expiration(deadline)

  def toExpiration(scalaDeadline: Option[Deadline]): Optional[Expiration] =
    scalaDeadline match {
      case Some(deadline) =>
        Optional.of(new Expiration(deadline))

      case None =>
        Optional.empty()
    }
}
