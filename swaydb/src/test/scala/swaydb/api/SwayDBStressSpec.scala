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

///*
// * Copyright (C) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.api
//
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import swaydb.core.TestBase
//import swaydb.serializers.Default._
//import swaydb.core.IOAssert._
//import swaydb.core.CommonAssertions._
//import swaydb.data.RunThis._
//
//class SwayDBStressSpec0 extends SwayDBStressSpec {
//  val keyValueCount: Int = 100000
//
//  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, IO] =
//    swaydb.persistent.Map[Int, String](dir = randomDir).runIO
//}
//
//class SwayDBStressSpec1 extends SwayDBStressSpec {
//
//  val keyValueCount: Int = 100000
//
//  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, IO] =
//    swaydb.persistent.Map[Int, String](randomDir, mapSize = 1.byte).runIO
//}
//
//class SwayDBStressSpec2 extends SwayDBStressSpec {
//
//  val keyValueCount: Int = 100000
//
//  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, IO] =
//    swaydb.memory.Map[Int, String](mapSize = 1.byte).runIO
//}
//
//class SwayDBStressSpec3 extends SwayDBStressSpec {
//  val keyValueCount: Int = 100000
//
//  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, IO] =
//    swaydb.memory.Map[Int, String]().runIO
//}
//
//sealed trait SwayDBStressSpec extends TestBase with TestBaseEmbedded {
//
//  val keyValueCount: Int
//
//  def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, IO]
//
//  "Test case that eventually fails due to collapsing of small Segments" in {
//    val db = newDB()
//    val deadline = 1.hour.fromNow
//
//    runThis(100.times) {
//      //add multiple Levels to Memory databases and the value fails
//      //swaydb.memory.Map[Int, String](mapSize = 1.byte).runIO
//      eitherOne(
//        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
//        right = db.remove(1, keyValueCount).runIO
//      )
//      (1 to keyValueCount) foreach (i => db.update(i, value = i.toString).runIO)
//      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }
//
//      eitherOne(
//        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
//        right = db.expire(1, keyValueCount, deadline).runIO
//      )
//
//      Future((1 to keyValueCount) foreach (i => db.update(i, value = i.toString).runIO))
//      Future(
//        eitherOne(
//          left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
//          right = db.expire(1, keyValueCount, deadline).runIO
//        )
//      )
//
//      println("db.level0Meter.mapsCount: " + db.level0Meter.mapsCount)
//      println("db.level1Meter.segmentsCount: " + db.level1Meter.segmentsCount)
//
//      //      db.foldLeft(0) {
//      //        case (previous, (nextKey, nextValue)) =>
//      //          //          println(s"previous: $previous -> next: $nextKey")
//      //          previous shouldBe (nextKey - 1)
//      //          db.deadline(nextKey).runIO shouldBe deadline
//      //          //          println(db.level0Meter.mapsCount)
//      //          nextKey
//      //      }
//
//      anyOrder(
//        left = db.head shouldBe(1, "1"),
//        right = db.last shouldBe(keyValueCount, keyValueCount.toString)
//      )
//
//      (1 to keyValueCount) foreach {
//        i =>
//          anyOrder(
//            left = db.expiration(i).runIO shouldBe deadline,
//            right = db.get(i).runIO shouldBe i.toString
//          )
//      }
//
//      //      db.foldRight(keyValueCount + 1) {
//      //        case ((previousKey, previousValue), next) =>
//      //          println(s"previousKey: $previousKey -> previousValue: $previousValue")
//      //          db.deadline(previousKey).runIO shouldBe deadline
//      //          (previousKey + 1) shouldBe next
//      //          previousKey
//      //      }
//    }
//  }
//}
