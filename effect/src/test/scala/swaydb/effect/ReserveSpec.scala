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

package swaydb.effect

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.Random
import swaydb.testkit.RunThis._

class ReserveSpec extends AnyFlatSpec with Matchers {

  def getPromises(reserve: Reserve[Unit]): ConcurrentLinkedQueue[Promise[Unit]] = {
    import org.scalatest.PrivateMethodTester._
    val function = PrivateMethod[ConcurrentLinkedQueue[Promise[Unit]]](Symbol("promises"))
    reserve.invokePrivate(function())
  }

  it should "complete futures if not already busy" in {
    val busy: Reserve[Unit] = Reserve.free[Unit](name = "test")
    busy.isBusy shouldBe false
    val futures =
      (1 to 100) map {
        i =>
          Reserve.promise(busy).future map { _ => i }
      }

    Future.sequence(futures).await should contain theSameElementsInOrderAs (1 to 100)
    getPromises(busy) shouldBe empty
  }

  it should "complete futures when freed" in {
    val busy = Reserve.busy(info = (), name = "test")
    val futures =
      (1 to 10000) map {
        i =>
          Reserve.promise(busy).future map { _ => i }
      }

    Future {
      (1 to 10000) foreach {
        i =>
          if (i == 10000 || Random.nextBoolean())
            Reserve.setFree(busy)
          else
            Reserve.compareAndSet(Some(()), busy)
      }
    }

    Future.sequence(futures).await should contain theSameElementsInOrderAs (1 to 10000)
    getPromises(busy) shouldBe empty
  }
}
