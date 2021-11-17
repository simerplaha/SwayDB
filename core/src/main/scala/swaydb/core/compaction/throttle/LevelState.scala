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

package swaydb.core.compaction.throttle

import swaydb.utils.FiniteDurations.FiniteDurationImplicits

import scala.concurrent.Future
import scala.concurrent.duration._

private[compaction] object LevelState {

  val failureSleepDuration: FiniteDuration = 5.second

  def longSleep: Deadline = longSleepFiniteDuration.fromNow

  val longSleepFiniteDuration: FiniteDuration = 1.hour

  case class Sleeping(sleepDeadline: Deadline,
                      stateId: Long) {
    override def toString: String =
      this.productPrefix +
        s" - sleepDeadline: ${sleepDeadline.timeLeft.asString}, " +
        s"stateId: $stateId "

    def toFuture: Future[Sleeping] =
      Future.successful(this)
  }
}
