package swaydb.data

import org.scalatest.{FlatSpec, Matchers}
import Base._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

class ReserveSpec extends FlatSpec with Matchers {

  it should "complete futures if not already busy" in {
    val busy = Reserve()
    val futures =
      (1 to 100) map {
        i =>
          Reserve.future(busy) map { _ => i }
      }

    Future.sequence(futures).await should contain theSameElementsInOrderAs (1 to 100)
  }

  it should "complete futures when freed" in {
    val busy = Reserve(info = ())
    val futures =
      (1 to 10000) map {
        i =>
          Reserve.future(busy) map { _ => i }
      }

    Future {
      (1 to 10000) foreach {
        i =>
          if (i == 10000 || Random.nextBoolean())
            Reserve.setFree(busy)
          else
            Reserve.setBusyOrGet((), busy)
      }
    }

    Future.sequence(futures).await should contain theSameElementsInOrderAs (1 to 10000)
  }
}
