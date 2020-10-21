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

package swaydb.core.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IO
import swaydb.core.TestExecutionContext
import swaydb.core.util.ParallelCollection._
import swaydb.data.RunThis._

import scala.concurrent.duration._
import scala.util.Random
import swaydb.core.TestData._

class ParallelCollectionSpec extends AnyWordSpec with Matchers {

  implicit val ec = TestExecutionContext.executionContext

  "mapRecoverIOParallel" should {
    "fail" when {
      "one job fails" in {
        runThis(100.times, log = true) {
          val seq = 0 to 20

          val failAt = Math.abs(Random.nextInt(seq.size))
          val parallelism = randomMaxParallelism()

          println(s"failAt: $failAt. parallelism = $parallelism")

          def block(int: Int): IO[Throwable, String] = {
            if (int == failAt)
              IO(throw new Exception("Kaboom!"))
            else
              IO(s"success $int")
          }

          @volatile var recoverSuccesses: Iterable[String] = null
          @volatile var failed: Throwable = null

          val result =
            seq.mapParallel[String](parallelism = randomMaxParallelism(), timeout = 5.seconds)(
              block = block,
              recover = {
                case (success, error) =>
                  //ensure recover is executed
                  recoverSuccesses = success
                  failed = error.exception
              }
            )

          failed.getMessage shouldBe "Kaboom!"
          recoverSuccesses.size should be <= (seq.size - 1)

          result.left.get shouldBe failed
        }

      }
    }

    "succeed" when {
      "no job fails" in {
        runThis(100.times, log = true) {
          val seq = 0 to 100

          def block(int: Int): IO[Throwable, String] =
            IO(int.toString)

          @volatile var recoverSuccesses: Iterable[String] = null
          @volatile var failed: Throwable = null

          val result =
            seq.mapParallel[String](parallelism = randomMaxParallelism(), timeout = 5.seconds)(
              block = block,
              recover = {
                case (success, error) =>
                  //recovery should not get executed.
                  recoverSuccesses = success
                  failed = error.exception
              }
            )

          failed shouldBe null
          recoverSuccesses shouldBe null

          result.get.map(_.toInt) shouldBe seq
        }
      }
    }
  }
}
