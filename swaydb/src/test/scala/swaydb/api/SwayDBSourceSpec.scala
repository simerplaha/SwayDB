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
import swaydb.testkit.RunThis.runThis

class SwayDBSourceSpec0 extends SwayDBSourceSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir).right.value.sweep(_.delete().get)

  override val keyValueCount: Int = 100
}

class SwayDBSource_SetMap_Spec0 extends SwayDBSourceSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMap[Int, String, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, IO.ApiIO](randomDir).right.value.sweep(_.delete().get)

  override val keyValueCount: Int = 100
}

class SwayDBSourceSpec1 extends SwayDBSourceSpec {

  override val keyValueCount: Int = 100

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBSourceSpec2 extends SwayDBSourceSpec {

  override val keyValueCount: Int = 100

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBSourceSpec3 extends SwayDBSourceSpec {

  override val keyValueCount: Int = 100

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

class MultiMapSwayDBSourceSpec4 extends SwayDBSourceSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep(_.delete().get)
}

class MultiMapSwayDBSourceSpec5 extends SwayDBSourceSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
}

sealed trait SwayDBSourceSpec extends TestBaseEmbedded {

  def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, IO.ApiIO]

  implicit val bag = Bag.glass

  "it" should {

    "transformValue" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to 100) foreach {
              i =>
                db.put(i, i.toString).right.value
            }

            val result =
              db
                .from(10)
                .transformValue {
                  item =>
                    item._2
                }

            result.materialize.value shouldBe (10 to 100).toList.map(_.toString)

        }
      }
    }
  }
}
