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

package swaydb.api

import swaydb.IOValues._
import swaydb._
import swaydb.core.TestSweeper
import swaydb.core.TestSweeper._
import swaydb.serializers.Default._
import swaydb.testkit.RunThis._
import swaydb.core.file.CoreFileTestKit._

class ScalaSetSpec0 extends ScalaSetSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.persistent.Set[Int, Nothing, IO.ApiIO](dir = randomDir()).right.value.sweep(_.delete().get)
}

class ScalaSetSpec1 extends ScalaSetSpec {

  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.persistent.Set[Int, Nothing, IO.ApiIO](randomDir(), logSize = 1.byte, segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig().copy(minSegmentSize = 10.bytes)).right.value.sweep(_.delete().get)
}

class ScalaSetSpec2 extends ScalaSetSpec {

  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.Set[Int, Nothing, IO.ApiIO](logSize = 1.byte).right.value.sweep(_.delete().get)
}

class ScalaSetSpec3 extends ScalaSetSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestSweeper): Set[Int, Nothing, IO.ApiIO] =
    swaydb.memory.Set[Int, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

sealed trait ScalaSetSpec extends TestBaseAPI {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestSweeper): Set[Int, Nothing, IO.ApiIO]


  "Expire" when {
    "put" in {
      runThis(times = repeatTest, log = true) {
        TestSweeper {
          implicit sweeper =>

            val db = newDB()

            db.asScala.add(1)
            db.asScala.contains(1) shouldBe true
        }
      }
    }

    "putAll" in {
      runThis(times = repeatTest, log = true) {
        TestSweeper {
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
        TestSweeper {
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
        TestSweeper {
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
        TestSweeper {
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
