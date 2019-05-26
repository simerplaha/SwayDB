package swaydb.core.util

import org.scalatest.{FlatSpec, Matchers}
import swaydb.core.RunThis._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class ReserveRangeSpec extends FlatSpec with Matchers {

  implicit val ordering = swaydb.data.order.KeyOrder.default

  it should "reserve a ranges and not allow overwrite until freed" in {
    implicit val state = ReserveRange.create[String]()
    ReserveRange.reserveOrGet(1, 10, "first") shouldBe empty

    ReserveRange.get(1, 10) should contain("first")
    ReserveRange.reserveOrGet(1, 10, "does not register") should contain("first")

    (0 to 10) foreach {
      i =>
        //check all overlapping keys are not allowed (0, 1)
        ReserveRange.reserveOrGet(i, i + 1, "does not register") should contain("first")
    }
  }

  it should "complete futures when freed" in {
    implicit val state = ReserveRange.create[String]()
    ReserveRange.reserveOrGet(1, 10, "first") shouldBe empty

    val futures =
      (0 to 10) map {
        i =>
          ReserveRange.reserveOrListen(1, 10, "does not register").left.get map {
            _ =>
              i
          }
      }

    Future {
      sleep(Random.nextInt(1000).millisecond)
      ReserveRange.free(1)
    }

    Future.sequence(futures).await shouldBe (0 to 10)
    state.ranges shouldBe empty
  }
}
