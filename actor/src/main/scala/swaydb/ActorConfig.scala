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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

sealed trait ActorConfig {
  def ec: ExecutionContext
}

object ActorConfig {

  case class Basic(name: String,
                   ec: ExecutionContext) extends ActorConfig

  case class Timer(name: String,
                   delay: FiniteDuration,
                   ec: ExecutionContext) extends ActorConfig

  case class TimeLoop(name: String,
                      delay: FiniteDuration,
                      ec: ExecutionContext) extends ActorConfig

  sealed trait QueueOrder[+T]

  object QueueOrder {

    case object FIFO extends QueueOrder[Nothing]
    case class Ordered[T](ordering: Ordering[T]) extends QueueOrder[T]

  }
}
