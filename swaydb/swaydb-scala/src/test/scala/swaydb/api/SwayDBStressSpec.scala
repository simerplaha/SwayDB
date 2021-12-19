///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
////
////package swaydb.api
////
////import scala.concurrent.Future
////import scala.concurrent.duration._
////import swaydb.core.TestBase
////import swaydb.serializers.Default._
////import swaydb.core.IOAssert._
////import swaydb.core.CommonAssertions._
////import swaydb.testkit.RunThis._
////
////class SwayDBStressSpec0 extends SwayDBStressSpec {
////  val keyValueCount: Int = 100000
////
////  override def newDB()(implicit sweeper: CoreTestSweeper): Map[Int, String, IO] =
////    swaydb.persistent.Map[Int, String](dir = genDirPath()).runIO
////}
////
////class SwayDBStressSpec1 extends SwayDBStressSpec {
////
////  val keyValueCount: Int = 100000
////
////  override def newDB()(implicit sweeper: CoreTestSweeper): Map[Int, String, IO] =
////    swaydb.persistent.Map[Int, String](genDirPath(), logSize = 1.byte).runIO
////}
////
////class SwayDBStressSpec2 extends SwayDBStressSpec {
////
////  val keyValueCount: Int = 100000
////
////  override def newDB()(implicit sweeper: CoreTestSweeper): Map[Int, String, IO] =
////    swaydb.memory.Map[Int, String](logSize = 1.byte).runIO
////}
////
////class SwayDBStressSpec3 extends SwayDBStressSpec {
////  val keyValueCount: Int = 100000
////
////  override def newDB()(implicit sweeper: CoreTestSweeper): Map[Int, String, IO] =
////    swaydb.memory.Map[Int, String]().runIO
////}
////
////sealed trait SwayDBStressSpec extends TestBase with TestBaseEmbedded {
////
////  val keyValueCount: Int
////
////  def newDB()(implicit sweeper: CoreTestSweeper): Map[Int, String, IO]
////
////  "Test case that eventually fails due to collapsing of small Segments" in {
////    val db = newDB()
////    val deadline = 1.hour.fromNow
////
////    runThis(100.times) {
////      //add multiple Levels to Memory databases and the value fails
////      //swaydb.memory.Map[Int, String](logSize = 1.byte).runIO
////      eitherOne(
////        left = (1 to keyValueCount) foreach (i => db.remove(i).runIO),
////        right = db.remove(1, keyValueCount).runIO
////      )
////      (1 to keyValueCount) foreach (i => db.update(i, value = i.toString).runIO)
////      (1 to keyValueCount) foreach { i => db.put(i, i.toString).runIO }
////
////      eitherOne(
////        left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
////        right = db.expire(1, keyValueCount, deadline).runIO
////      )
////
////      Future((1 to keyValueCount) foreach (i => db.update(i, value = i.toString).runIO))
////      Future(
////        eitherOne(
////          left = (1 to keyValueCount) foreach (i => db.expire(i, deadline).runIO),
////          right = db.expire(1, keyValueCount, deadline).runIO
////        )
////      )
////
////      println("db.level0Meter.logsCount: " + db.level0Meter.logsCount)
////      println("db.level1Meter.segmentsCount: " + db.level1Meter.segmentsCount)
////
////      //      db.foldLeft(0) {
////      //        case (previous, (nextKey, nextValue)) =>
////      //          //          println(s"previous: $previous -> next: $nextKey")
////      //          previous shouldBe (nextKey - 1)
////      //          db.deadline(nextKey).runIO shouldBe deadline
////      //          //          println(db.level0Meter.logsCount)
////      //          nextKey
////      //      }
////
////      anyOrder(
////        left = db.head shouldBe(1, "1"),
////        right = db.last shouldBe(keyValueCount, keyValueCount.toString)
////      )
////
////      (1 to keyValueCount) foreach {
////        i =>
////          anyOrder(
////            left = db.expiration(i).runIO shouldBe deadline,
////            right = db.get(i).runIO shouldBe i.toString
////          )
////      }
////
////      //      db.foldRight(keyValueCount + 1) {
////      //        case ((previousKey, previousValue), next) =>
////      //          println(s"previousKey: $previousKey -> previousValue: $previousValue")
////      //          db.deadline(previousKey).runIO shouldBe deadline
////      //          (previousKey + 1) shouldBe next
////      //          previousKey
////      //      }
////    }
////  }
////}
