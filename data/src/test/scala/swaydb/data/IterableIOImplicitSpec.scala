/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IO
import swaydb.IO._
import RunThis._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class IterableIOImplicitSpec extends AnyWordSpec with Matchers {

  "mapRecoverIOParallel" should {
    "fail" when {
      "one job fails" in {
        runThis(100.times, log = true) {
          val seq = 0 to 20

          val failAt = Math.abs(Random.nextInt(seq.size))

          println("failAt: " + failAt)

          def block(int: Int): IO[Throwable, Unit] = {
            if (int == failAt)
              IO(throw new Exception("Kaboom!"))
            else
              IO.unit
          }

          @volatile var recoverSuccesses: Iterable[Unit] = null
          @volatile var failed: Throwable = null

          val result =
            seq.mapRecoverIOParallel[Unit](parallelism = 10)(
              block = block,
              recover = {
                case (success, error) =>
                  //ensure recover is executed
                  recoverSuccesses = success
                  failed = error.exception
              }
            )

          failed.getMessage shouldBe "Kaboom!"
          recoverSuccesses.size shouldBe (seq.size - 1)

          result.left.get shouldBe failed
        }

      }
    }

    "succeed" when {
      "no job fails" in {
        runThis(100.times, log = true) {
          val seq = 0 to 20

          val failAt = Math.abs(Random.nextInt(seq.size))

          println("failAt: " + failAt)

          def block(int: Int): IO[Throwable, Unit] =
            IO.unit

          @volatile var recoverSuccesses: Iterable[Unit] = null
          @volatile var failed: Throwable = null

          val result =
            seq.mapRecoverIOParallel[Unit](parallelism = 10)(
              block = block,
              recover = {
                case (success, error) =>
                  //ensure recover is executed
                  recoverSuccesses = success
                  failed = error.exception
              }
            )

          failed shouldBe null
          recoverSuccesses shouldBe null

          result.get.size shouldBe seq.size
        }

      }
    }
  }
}
