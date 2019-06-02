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

package swaydb.core.brake

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._

private[core] class BrakePedal(private var brakeFor: FiniteDuration,
                               private val releaseRate: FiniteDuration) extends LazyLogging {

  /**
    * Blocking back-pressure.
    *
    * Blocks current thread and decrements the next block with the release rate.
    *
    * @return true if the brake is complete else false.
    */
  def applyBrakes(): Boolean = {
    logger.warn(s"Braking for: {}", brakeFor)
    Thread.sleep(brakeFor.toMillis)
    brakeFor = brakeFor - releaseRate
    brakeFor.fromNow.isOverdue()
  }
}