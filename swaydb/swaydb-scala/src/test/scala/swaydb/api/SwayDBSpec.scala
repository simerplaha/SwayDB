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
//import org.scalatest.OptionValues._
//import swaydb.Exception.InvalidDirectoryType
//import swaydb.effect.IOValues._
//import swaydb._
//import swaydb.config.DataType
//import swaydb.core.TestSweeper._
//import swaydb.core.{Core, TestSweeper}
//import swaydb.serializers.Default._
//import swaydb.testkit.RunThis.runThis
//
//import java.nio.file.Files
//import swaydb.core.file.CoreFileTestKit._
//
//class SwayDBSpec0 extends SwayDBSpec {
//  override def newDB()(implicit sweeper: TestSweeper): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir()).value.sweep(_.delete().get)
//
//  override val keyValueCount: Int = 100
//}
//
//class SwayDB_SetMap_Spec0 extends SwayDBSpec {
//  override def newDB()(implicit sweeper: TestSweeper): SetMap[Int, String, IO.ApiIO] =
//    swaydb.persistent.SetMap[Int, String, IO.ApiIO](randomDir()).value.sweep(_.delete().get)
//
//  override val keyValueCount: Int = 100
//}
//
//class SwayDBSpec1 extends SwayDBSpec {
//
//  override val keyValueCount: Int = 100
//
//  override def newDB()(implicit sweeper: TestSweeper): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir(), logSize = 1.byte).value.sweep(_.delete().get)
//}
//
//class SwayDBSpec2 extends SwayDBSpec {
//
//  override val keyValueCount: Int = 100
//
//  override def newDB()(implicit sweeper: TestSweeper): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](logSize = 1.byte).value.sweep(_.delete().get)
//}
//
//class SwayDBSpec3 extends SwayDBSpec {
//
//  override val keyValueCount: Int = 100
//
//  override def newDB()(implicit sweeper: TestSweeper): Map[Int, String, Nothing, IO.ApiIO] =
//    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().value.sweep(_.delete().get)
//}
//
//class MultiMapSwayDBSpec4 extends SwayDBSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: TestSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
//    generateRandomNestedMaps(swaydb.persistent.MultiMap[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir()).get).sweep(_.delete().get)
//}
//
//class MultiMapSwayDBSpec5 extends SwayDBSpec {
//  val keyValueCount: Int = 10000
//
//  override def newDB()(implicit sweeper: TestSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
//    generateRandomNestedMaps(swaydb.memory.MultiMap[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
//}
//
//sealed trait SwayDBSpec extends TestBaseAPI {
//
//  def newDB()(implicit sweeper: TestSweeper): SetMapT[Int, String, IO.ApiIO]
//
//  implicit val bag = Bag.apiIO
//
//  "SwayDB" should {
//
//    "remove all but first and last" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            (1 to 1000) foreach {
//              i =>
//                db.put(i, i.toString).value
//            }
//            println("Removing .... ")
//            doRemove(2, 999, db)
//            println("Removed .... ")
//
//            db.materialize.value should contain only((1, "1"), (1000, "1000"))
//            db.head.value.value shouldBe ((1, "1"))
//            db.last.value.value shouldBe ((1000, "1000"))
//        }
//      }
//    }
//
//    "update only key-values that are not removed" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db match {
//              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
//                (1 to 100) foreach {
//                  i =>
//                    db.put(i, i.toString).value
//                }
//
//                (2 to 99) foreach {
//                  i =>
//                    db.remove(i).value
//                }
//
//                db.update(1, 100, value = "updated").value
//
//                db.materialize.value should contain only((1, "updated"), (100, "updated"))
//                db.head.value.value shouldBe ((1, "updated"))
//                db.last.value.value shouldBe ((100, "updated"))
//
//              case SetMap(set) =>
//              //todo
//            }
//        }
//      }
//    }
//
//    "update only key-values that are not removed by remove range" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db match {
//              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
//                (1 to 100) foreach {
//                  i =>
//                    db.put(i, i.toString).value
//                }
//
//                db.remove(2, 99).value
//
//                db.update(1, 100, value = "updated").value
//
//                db.materialize.value should contain only((1, "updated"), (100, "updated"))
//                db.head.value.value shouldBe ((1, "updated"))
//                db.last.value.value shouldBe ((100, "updated"))
//
//              case SetMap(set) =>
//              //todo
//            }
//        }
//      }
//    }
//
//    "return only key-values that were not removed from remove range" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db match {
//              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
//                (1 to 100) foreach {
//                  i =>
//                    db.put(i, i.toString).value
//                }
//
//                db.update(10, 90, value = "updated").value
//
//                db.remove(50, 99).value
//
//                val expectedUnchanged =
//                  (1 to 9) map {
//                    i =>
//                      (i, i.toString)
//                  }
//
//                val expectedUpdated =
//                  (10 to 49) map {
//                    i =>
//                      (i, "updated")
//                  }
//
//                val expected = expectedUnchanged ++ expectedUpdated :+ (100, "100")
//
//                db.materialize.value shouldBe expected
//                db.head.value.value shouldBe ((1, "1"))
//                db.last.value.value shouldBe ((100, "100"))
//
//              case SetMap(set) =>
//              //todo
//            }
//        }
//      }
//    }
//
//    "return empty for an empty database with update range" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//            db match {
//              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
//
//                db.update(1, Int.MaxValue, value = "updated").value
//
//                db.isEmpty.get shouldBe true
//
//                db.materialize.value shouldBe empty
//
//                db.head.get shouldBe empty
//                db.last.get shouldBe empty
//
//              case SetMap(set) =>
//              //todo
//            }
//        }
//      }
//    }
//
//    "return empty for an empty database with remove range" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//            db.remove(1, Int.MaxValue).value
//
//            db.isEmpty.get shouldBe true
//
//            db.materialize.value shouldBe empty
//
//            db.head.get shouldBe empty
//            db.last.get shouldBe empty
//        }
//      }
//    }
//
//    "batch put, remove, update & range remove key-values" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db match {
//              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
//                db.commit(Prepare.Put(1, "one"), Prepare.Remove(1)).value
//                db.get(1).value shouldBe empty
//
//                //      remove and then put should return Put's value
//                db.commit(Prepare.Remove(1), Prepare.Put(1, "one")).value
//                db.get(1).value.value shouldBe "one"
//
//                //remove range and put should return Put's value
//                db.commit(Prepare.Remove(1, 100), Prepare.Put(1, "one")).value
//                db.get(1).value.value shouldBe "one"
//
//                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one one"), Prepare.Update(1, 100, "updated"), Prepare.Remove(1, 100)).value
//                db.get(1).value shouldBe empty
//                db.isEmpty.get shouldBe true
//                //
//                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one one"), Prepare.Remove(1, 100), Prepare.Update(1, 100, "updated")).value
//                db.get(1).value shouldBe empty
//                db.isEmpty.get shouldBe true
//
//                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one again"), Prepare.Update(1, 100, "updated")).value
//                db.get(1).value.value shouldBe "updated"
//                db.materialize.map(_.toMap).value.values should contain only "updated"
//
//                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(100, "hundred"), Prepare.Remove(1, 100), Prepare.Update(1, 1000, "updated")).value
//                db.materialize.value shouldBe empty
//
//              case SetMap(set) =>
//              //TODO
//            }
//        }
//      }
//    }
//
//    "perform from, until, before & after" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            (1 to 10000) foreach {
//              i =>
//                db.put(i, i.toString).value
//            }
//
//            db.from(9999).materialize.value should contain only((9999, "9999"), (10000, "10000"))
//            db.from(9998).drop(2).take(1).materialize.value should contain only ((10000, "10000"))
//            db.before(9999).take(1).materialize.value should contain only ((9998, "9998"))
//            db.after(9999).take(1).materialize.value should contain only ((10000, "10000"))
//            db.after(9999).drop(1).materialize.value shouldBe empty
//
//            db.after(10).takeWhile(_._1 <= 11).materialize.value should contain only ((11, "11"))
//            db.after(10).takeWhile(_._1 <= 11).drop(1).materialize.value shouldBe empty
//
//            db.fromOrBefore(0).materialize.value shouldBe empty
//            db.fromOrAfter(0).take(1).materialize.value should contain only ((1, "1"))
//        }
//      }
//    }
//
//    "perform mightContain & contains" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            (1 to 10000) foreach {
//              i =>
//                db.put(i, i.toString).value
//            }
//
//            (1 to 10000) foreach {
//              i =>
//                db.mightContain(i).value shouldBe true
//                db.contains(i).value shouldBe true
//            }
//
//            //      db.mightContain(Int.MaxValue).runIO shouldBe false
//            //      db.mightContain(Int.MinValue).runIO shouldBe false
//            db.contains(20000).value shouldBe false
//        }
//      }
//    }
//
//    "contains on removed should return false" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            (1 to 100000) foreach {
//              i =>
//                db.put(i, i.toString).value
//            }
//
//            (1 to 100000) foreach {
//              i =>
//                db.remove(i).value
//            }
//
//            (1 to 100000) foreach {
//              i =>
//                db.contains(i).value shouldBe false
//            }
//
//            db.contains(100001).value shouldBe false
//        }
//      }
//    }
//
//    "delete" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            (1 to 100000) foreach {
//              i =>
//                db.put(i, i.toString).value
//            }
//
//            db.delete().value
//
//            Files.exists(db.path) shouldBe false
//        }
//      }
//    }
//
//    "return valueSize" in {
//      //      val db = newDB()
//      //
//      //      (1 to 10000) foreach {
//      //        i =>
//      //          db.put(i, i.toString).runIO
//      //      }
//
//      //      (1 to 10000) foreach {
//      //        i =>
//      //          db.valueSize(i.toString).runIO shouldBe i.toString.getBytes(StandardCharsets.UTF_8).length
//      //      }
//    }
//
//    "not allow api calls" when {
//      "closed" in {
//        runThis(times = repeatTest, log = true) {
//          TestSweeper {
//            implicit sweeper =>
//
//              val db = newDB()
//
//              (1 to 100000) foreach {
//                i =>
//                  db.put(i, i.toString).value
//              }
//
//              db.close().value
//
//              db.get(1).left.value.exception.getMessage shouldBe Core.closedMessage()
//          }
//        }
//      }
//
//      "deleted" in {
//        runThis(times = repeatTest, log = true) {
//          TestSweeper {
//            implicit sweeper =>
//
//              val db = newDB()
//
//              (1 to 100000) foreach {
//                i =>
//                  db.put(i, i.toString).value
//              }
//
//              db.delete().value
//
//              db.get(1).left.value.exception.getMessage shouldBe Core.closedMessage()
//          }
//        }
//      }
//    }
//
//    "disallow different data-types on same directory" in {
//      TestSweeper {
//        implicit sweeper =>
//
//          val dir = randomDir()
//          val map = swaydb.persistent.Map[Int, Int, Nothing, Glass](dir)
//          map.path shouldBe dir
//          map.put(1, 1)
//          map.close()
//
//          val set = swaydb.persistent.Set[Int, Nothing, IO.ApiIO](dir)
//          set.left.value.exception shouldBe InvalidDirectoryType(DataType.Set.name, DataType.Map.name)
//
//          //reopen it as a map
//          val reopened = swaydb.persistent.Map[Int, Int, Nothing, Glass](dir).sweep(_.delete())
//          reopened.get(1).value shouldBe 1
//
//      }
//    }
//
//    //    "eventually remove all Segments from the database when remove range is submitted" in {
//    //      val db = newDB()
//    //
//    //      (1 to 2000000) foreach {
//    //        i =>
//    //          db.put(i, i.toString).runIO
//    //      }
//    //
//    //      db.remove(1, 2000000).runIO
//    //
//    //      assertLevelsAreEmpty(db, submitUpdates = true)
//    //
//    //      db.closeDatabase().get
//    //    }
//    //
//    //    "eventually remove all Segments from the database when expire range is submitted" in {
//    //      val db = newDB()
//    //
//    //      (1 to 2000000) foreach {
//    //        i =>
//    //          db.put(i, i.toString).runIO
//    //      }
//    //
//    //      db.expire(1, 2000000, 5.minutes).runIO
//    //      println("Expiry submitted.")
//    //
//    //      assertLevelsAreEmpty(db, submitUpdates = true)
//    //
//    //      db.closeDatabase().get
//    //    }
//    //
//    //    "eventually remove all Segments from the database when put is submitted with expiry" in {
//    //      val db = newDB()
//    //
//    //      (1 to 2000000) foreach {
//    //        i =>
//    //          db.put(i, i.toString, 5.minute).runIO
//    //      }
//    //
//    //      assertLevelsAreEmpty(db, submitUpdates = false)
//    //
//    //      db.closeDatabase().get
//    //    }
//    //  }
//
//    //  "debug" in {
//    //    val db = newDB()
//    //
//    //    def doRead() =
//    //      Future {
//    //        while (true) {
//    //          (1 to 2000000) foreach {
//    //            i =>
//    //              db.get(i).runIO shouldBe i.toString
//    //              if (i % 100000 == 0)
//    //                println(s"Read $i")
//    //          }
//    //        }
//    //      }
//    //
//    //    def doWrite() = {
//    //      println("writing")
//    //      (1 to 2000000) foreach {
//    //        i =>
//    //          db.put(i, i.toString).runIO
//    //          if (i % 100000 == 0)
//    //            println(s"Write $i")
//    //      }
//    //      println("writing end")
//    //    }
//    //
//    //    doWrite()
//    //    doRead()
//    //    sleep(1.minutes)
//    //    doWrite()
//    //    sleep(1.minutes)
//    //    doWrite()
//    //    sleep(1.minutes)
//    //    doWrite()
//    //    assertLevelsAreEmpty(db, submitUpdates = false)
//    //
//    //    db.closeDatabase().get
//    //  }
//    //
//    //  "debug before and mapRight" in {
//    //    val db = newDB()
//    //
//    //    (1 to 10) foreach {
//    //      i =>
//    //        db.put(i, i.toString).runIO
//    //    }
//    //
//    //    //    db.before(5).toList foreach println
//    //    db.before(5).reverse.map { case (k, v) => (k, v) } foreach println
//    //
//    //    db.closeDatabase().get
//    //  }
//  }
//}
