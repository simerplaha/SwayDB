/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.data

import org.scalatest.{FlatSpec, Matchers}
import swaydb.data.Base._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

class ReserveSpec extends FlatSpec with Matchers {

  it should "complete futures if not already busy" in {
    val busy = Reserve[Unit](name = "test")
    busy.isBusy shouldBe false
    val futures =
      (1 to 100) map {
        i =>
          Reserve.promise(busy).future map { _ => i }
      }

    Future.sequence(futures).await should contain theSameElementsInOrderAs (1 to 100)
  }

  it should "complete futures when freed" in {
    val busy = Reserve(info = (), name = "test")
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
            Reserve.setBusyOrGet((), busy)
      }
    }

    Future.sequence(futures).await should contain theSameElementsInOrderAs (1 to 10000)
  }
}
