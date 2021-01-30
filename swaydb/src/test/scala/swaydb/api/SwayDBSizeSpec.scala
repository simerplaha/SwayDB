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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.api

import swaydb.IOValues._
import swaydb._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.serializers.Default._
import swaydb.testkit.RunThis._

class SwayDBSize_Persistent_Spec extends SwayDBSizeSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class SwayDBSize_Memory_Spec extends SwayDBSizeSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

class MultiMapSizeSpec4 extends SwayDBSizeSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep(_.delete().get)
}

class MultiMapSizeSpec5 extends SwayDBSizeSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
}

sealed trait SwayDBSizeSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  override def deleteFiles = false

  def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO]

  "return the size of key-values" in {
    runThis(times = repeatTest, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          (1 to keyValueCount) foreach {
            i =>
              db.put(i, i.toString).right.value
          }

          db.count.value shouldBe keyValueCount
      }
    }
  }
}
