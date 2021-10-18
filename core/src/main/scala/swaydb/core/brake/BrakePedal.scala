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

package swaydb.core.brake

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._

private[core] class BrakePedal(private var brakeFor: FiniteDuration,
                               releaseRate: FiniteDuration,
                               logAsWarning: Boolean) extends LazyLogging {

  /**
   * Blocking back-pressure.
   *
   * Blocks current thread and decrements the next block with the release rate.
   *
   * @return true if the brake is complete else false.
   */
  def applyBrakes(): Boolean = {
    if (logAsWarning)
      logger.warn(s"Blocking-backpressure - Braking for: $brakeFor.")

    Thread.sleep(brakeFor.toMillis)
    brakeFor = brakeFor - releaseRate

    val isOverdue = brakeFor.fromNow.isOverdue()

    if (logAsWarning && isOverdue)
      logger.warn(s"Blocking-backpressure - Brake released!")

    isOverdue
  }

  def isReleased(): Boolean =
    brakeFor.fromNow.isOverdue()

  def isBraking(): Boolean =
    brakeFor.fromNow.hasTimeLeft()
}
