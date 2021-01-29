/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import swaydb.data.Base._

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.Random

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
