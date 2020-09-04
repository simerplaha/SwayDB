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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.api

import swaydb.IOValues._
import swaydb.api.{TestBaseEmbedded, repeatTest}
import swaydb.data.RunThis._
import swaydb.core.TestCaseSweeper
import TestCaseSweeper._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb._

class SwayDBReverse_Persistent_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class SwayDBReverse_Memory_Spec extends SwayDBReverseSpec {
  implicit val order: KeyOrder[Slice[Byte]] = KeyOrder.reverse

  val keyValueCount: Int = 100000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

sealed trait SwayDBReverseSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO]

  implicit val bag = Bag.apiIO

  "Do reverse ordering" in {
    runThis(times = repeatTest, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          (1 to keyValueCount) foreach {
            i =>
              db.put(i, i.toString).right.value
          }

          db
            .keys
            .stream
            .foldLeft(keyValueCount + 1) {
              case (expected, actual) =>
                actual shouldBe expected - 1
                actual
            }
      }
    }
  }
}
