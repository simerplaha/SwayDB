package swaydb.core.util

import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.duration._
import FiniteDurationUtil._
import java.util.{Timer, TimerTask}

class FiniteDurationUtilSpec extends FlatSpec with Matchers {

  it should "return duration as string" in {
    (1 to 100) foreach {
      i =>
        i.second.asString shouldBe s"$i.0 seconds"
    }
  }

  it should "convert TimeTask to deadline" in {
    val timer = new Timer(true)
    (10 to 20) foreach {
      i =>
        val task =
          new TimerTask {
            def run() =
              ()
          }
        val delay = i.seconds
        timer.schedule(task, delay.toMillis)

        val timeLeft = task.timeLeft()
        println(timeLeft.asString)

        //timeLeft should be nearly the same
        timeLeft should be <= delay
        timeLeft should be > delay - 10.millisecond

        //also fetch the deadline
        val deadlineDiff = delay.fromNow - task.deadline()
        deadlineDiff should be < 100.millisecond
    }

    timer.cancel()
  }
}
