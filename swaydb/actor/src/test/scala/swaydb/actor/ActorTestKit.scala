/*
 * Copyright (c) 18/12/21, 4:43 am Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package swaydb.actor

import swaydb.testkit.TestKit.randomBoolean
import swaydb.ActorConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object ActorTestKit {

  implicit class ActorConfigImplicits(actorConfig: ActorConfig.type) {
    def random(delay: FiniteDuration = 2.seconds)(implicit ec: ExecutionContext): ActorConfig =
      if (randomBoolean())
        ActorConfig.Basic("Random Basic config", ec)
      else if (randomBoolean())
        ActorConfig.Timer("Random Timer config", delay, ec)
      else
        ActorConfig.TimeLoop("Random TimeLoop config", delay, ec)
  }

}
