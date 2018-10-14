/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb

import swaydb.{Map, SwayDB}
import swaydb.core.TestBase
import swaydb.serializers.Default._

import scala.concurrent.Future
import scala.concurrent.duration._

class SwayDBStressSpec0 extends SwayDBStressSpec {
  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.persistent[Int, String](dir = randomDir, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SwayDBStressSpec1 extends SwayDBStressSpec {

  val keyValueCount: Int = 100000

  import swaydb._

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.persistent[Int, String](randomDir, mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SwayDBStressSpec2 extends SwayDBStressSpec {

  val keyValueCount: Int = 100000

  import swaydb._

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.memory[Int, String](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

class SwayDBStressSpec3 extends SwayDBStressSpec {
  val keyValueCount: Int = 100000

  override def newDB(minTimeLeftToUpdateExpiration: FiniteDuration): Map[Int, String] =
    SwayDB.memory[Int, String](minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
}

sealed trait SwayDBStressSpec extends TestBase with TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds): Map[Int, String]

  "Test case that eventually fails due to collapsing of small Segments" in {
    val db = newDB()
    val deadline = 1.hour.fromNow

    runThis(100.times) {
      //add multiple Levels to Memory databases and the get fails
      //SwayDB.memory[Int, String](mapSize = 1.byte, minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration).assertGet
      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.remove(i).assertGet),
        right = db.remove(1, keyValueCount).assertGet
      )
      (1 to keyValueCount) foreach (i => db.update(i, value = i.toString).assertGet)
      (1 to keyValueCount) foreach { i => db.put(i, i.toString).assertGet }

      eitherOne(
        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
        right = db.expire(1, keyValueCount, deadline).assertGet
      )

      Future((1 to keyValueCount) foreach (i => db.update(i, value = i.toString).assertGet))
      Future(
        eitherOne(
          left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).assertGet),
          right = db.expire(1, keyValueCount, deadline).assertGet
        )
      )

      println("db.level0Meter.mapsCount: " + db.level0Meter.mapsCount)
      println("db.level1Meter.segmentsCount: " + db.level1Meter.segmentsCount)

      //      db.foldLeft(0) {
      //        case (previous, (nextKey, nextValue)) =>
      //          //          println(s"previous: $previous -> next: $nextKey")
      //          previous shouldBe (nextKey - 1)
      //          db.deadline(nextKey).assertGet shouldBe deadline
      //          //          println(db.level0Meter.mapsCount)
      //          nextKey
      //      }

      anyOrder(
        execution1 = db.head shouldBe(1, "1"),
        execution2 = db.last shouldBe(keyValueCount, keyValueCount.toString)
      )

      (1 to keyValueCount) foreach {
        i =>
          anyOrder(
            execution1 = db.expiration(i).assertGet shouldBe deadline,
            execution2 = db.get(i).assertGet shouldBe i.toString
          )
      }

      //      db.foldRight(keyValueCount + 1) {
      //        case ((previousKey, previousValue), next) =>
      //          println(s"previousKey: $previousKey -> previousValue: $previousValue")
      //          db.deadline(previousKey).assertGet shouldBe deadline
      //          (previousKey + 1) shouldBe next
      //          previousKey
      //      }
    }
  }
}