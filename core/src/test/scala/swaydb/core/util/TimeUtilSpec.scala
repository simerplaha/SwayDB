package swaydb.core.util

import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.duration._
import TimeUtil._
import swaydb.data.slice.Slice

class TimeUtilSpec extends WordSpec with Matchers {

  "toNanos" should {
    "convert deadline to nanos" in {
      val duration = 10.seconds
      val deadline = Deadline(duration)
      deadline.toNanos shouldBe duration.toNanos

      Some(deadline).toNanos shouldBe duration.toNanos
    }

    "convert none deadline to 0" in {
      Option.empty[Deadline].toNanos shouldBe 0L
    }
  }

  "toLongUnsignedBytes" should {
    "convert deadline unsigned long" in {
      val duration = 10.seconds
      val deadline = Deadline(duration)
      deadline.toLongUnsignedBytes shouldBe Slice.writeLongUnsigned(duration.toNanos)
    }
  }

  "toBytes" should {
    "convert deadline long bytes" in {
      val duration = 10.seconds
      val deadline = Deadline(duration)
      deadline.toBytes shouldBe Slice.writeLong(duration.toNanos)
    }
  }

  "toDeadline" should {
    "convert long to deadline" in {
      val duration = 10.seconds
      duration.toNanos.toDeadline shouldBe Deadline(duration)
    }

    "convert 0 to deadline" in {
      0L.toDeadline shouldBe Deadline(0.nanosecond)
    }

    "convert 0 to None" in {
      0L.toDeadlineOption shouldBe empty
    }
  }

}
