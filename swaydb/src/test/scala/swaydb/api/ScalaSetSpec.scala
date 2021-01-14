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
import swaydb.data.RunThis._
import swaydb.core.TestCaseSweeper
import swaydb.serializers.Default._
import TestCaseSweeper._

class ScalaSetSpec0 extends ScalaSetSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.persistent.Set[Int, Nothing, IO.ApiIO](dir = randomDir).right.value.sweep(_.delete().get)
}

class ScalaSetSpec1 extends ScalaSetSpec {

  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.persistent.Set[Int, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte, segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig().copy(minSegmentSize = 10.bytes)).right.value.sweep(_.delete().get)
}

class ScalaSetSpec2 extends ScalaSetSpec {

  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.Set[Int, Nothing, IO.ApiIO](mapSize = 1.byte).right.value.sweep(_.delete().get)
}

class ScalaSetSpec3 extends ScalaSetSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.Set[Int, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

sealed trait ScalaSetSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): Set[Int, Nothing, IO.ApiIO]


  "Expire" when {
    "put" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            db.asScala.add(1)
            db.asScala.contains(1) shouldBe true
        }
      }
    }

    "putAll" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            db.asScala ++= Seq(1, 2)

            db.asScala.contains(1) shouldBe true
            db.asScala.contains(2) shouldBe true
        }
      }
    }

    "remove" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            db.asScala ++= Seq(1, 2)

            db.asScala.remove(1)

            db.asScala.contains(1) shouldBe false
            db.asScala.contains(2) shouldBe true
        }
      }
    }

    "removeAll" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            db.asScala ++= Seq(1, 2)

            db.asScala.clear()

            db.asScala.contains(1) shouldBe false
            db.asScala.contains(2) shouldBe false
        }
      }
    }

    "head, last, contains" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val db = newDB()

            db.asScala ++= Seq(1, 2)

            db.asScala.head shouldBe 1
            db.asScala.last shouldBe 2

            db.asScala.contains(1) shouldBe true
            db.asScala.contains(2) shouldBe true
            db.asScala.contains(3) shouldBe false
        }
      }
    }
  }
}
