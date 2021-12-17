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
//
//package swaydb.api
//
//import swaydb.effect.IOValues._
//import swaydb._
//import swaydb.core.TestSweeper
//import swaydb.core.TestSweeper._
//import swaydb.serializers.Default._
//import swaydb.testkit.RunThis._
//import swaydb.core.file.CoreFileTestKit._
//
//class ScalaMapSpec0 extends ScalaMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO] =
//    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](dir = randomDir()).right.value.sweep(_.delete().get)
//}
//
//class ScalaSetMapSpec0 extends ScalaMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO] =
//    swaydb.persistent.SetMap[Int, String, IO.ApiIO](dir = randomDir()).right.value.sweep(_.delete().get)
//}
//
//class ScalaMapSpec1 extends ScalaMapSpec {
//
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO] =
//    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir(), logSize = 1.byte, segmentConfig = swaydb.persistent.DefaultConfigs.segmentConfig().copy(minSegmentSize = 10.bytes)).right.value.sweep(_.delete().get)
//}
//
//class ScalaMapSpec2 extends ScalaMapSpec {
//
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO] =
//    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](logSize = 1.byte).right.value.sweep(_.delete().get)
//}
//
//class ScalaMapSpec3 extends ScalaMapSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO] =
//    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
//}
//
//class MultiMapSpec4 extends ScalaMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO] =
//    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir()).get.sweep(_.delete().get))
//}
//
//class MultiMapSpec5 extends ScalaMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO] =
//    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get.sweep(_.delete().get))
//}
//
//sealed trait ScalaMapSpec extends TestBaseAPI {
//
//  val keyValueCount: Int
//
//  def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO]
//
//  "Expire" when {
//    "put" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db.asScala.put(1, "one")
//
//            db.asScala.get(1) should contain("one")
//        }
//      }
//    }
//
//    "putAll" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db.asScala ++= Seq((1, "one"), (2, "two"))
//
//            db.asScala.get(1) should contain("one")
//            db.asScala.get(2) should contain("two")
//        }
//      }
//    }
//
//    "remove" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db.asScala ++= Seq((1, "one"), (2, "two"))
//
//            db.asScala.remove(1)
//
//            db.asScala.get(1) shouldBe empty
//            db.asScala.get(2) should contain("two")
//        }
//      }
//    }
//
//    "removeAll" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db.asScala ++= Seq((1, "one"), (2, "two"))
//
//            db.asScala.clear()
//
//            db.asScala.get(1) shouldBe empty
//            db.asScala.get(2) shouldBe empty
//        }
//      }
//    }
//
//    "keySet, head, last, contains" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db.asScala ++= Seq((1, "one"), (2, "two"))
//
//            if (db.isInstanceOf[swaydb.MultiMap[_, _, _, _, IO.ApiIO]])
//              assertThrows[NotImplementedError](db.asScala.keySet)
//            else
//              db.asScala.keySet should contain only(1, 2)
//
//            db.asScala.head shouldBe ((1, "one"))
//            db.asScala.last shouldBe ((2, "two"))
//
//            db.asScala.contains(1) shouldBe true
//            db.asScala.contains(2) shouldBe true
//            db.asScala.contains(3) shouldBe false
//        }
//      }
//    }
//  }
//}
