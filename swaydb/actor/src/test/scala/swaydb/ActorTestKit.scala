package swaydb

import swaydb.testkit.TestKit.randomBoolean

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

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
