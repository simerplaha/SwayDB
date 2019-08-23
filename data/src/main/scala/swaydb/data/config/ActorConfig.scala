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

package swaydb.data.config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

sealed trait ActorConfig {
  def maxMessagesToProcessAtOnce: Int
  def ec: ExecutionContext
}

object ActorConfig {

  case class Basic(maxMessagesToProcessAtOnce: Int,
                   ec: ExecutionContext) extends ActorConfig

  case class Timer(maxMessagesToProcessAtOnce: Int,
                   maxOverflow: Int,
                   delay: FiniteDuration,
                   ec: ExecutionContext) extends ActorConfig

  case class TimeLoop(maxMessagesToProcessAtOnce: Int,
                      maxOverflow: Int,
                      delay: FiniteDuration,
                      ec: ExecutionContext) extends ActorConfig
}
