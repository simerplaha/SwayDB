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

import java.nio.file.Files

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb._
import swaydb.data.RunThis.runThis
import swaydb.core.{Core, TestCaseSweeper}
import swaydb.core.TestCaseSweeper._
import swaydb.serializers.Default._

class SwayDBSpec0 extends SwayDBSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir).right.value.sweep(_.delete().get)

  override val keyValueCount: Int = 100
}

class SwayDB_SetMap_Spec0 extends SwayDBSpec {
  override def newDB()(implicit sweeper: TestCaseSweeper): SetMap[Int, String, IO.ApiIO] =
    swaydb.persistent.SetMap[Int, String, IO.ApiIO](randomDir).right.value.sweep(_.delete().get)

  override val keyValueCount: Int = 100
}

class SwayDBSpec1 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.persistent.Map[Int, String, Nothing, IO.ApiIO](randomDir, mapSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBSpec2 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO](mapSize = 1.byte).right.value.sweep(_.delete().get)
}

class SwayDBSpec3 extends SwayDBSpec {

  override val keyValueCount: Int = 100

  override def newDB()(implicit sweeper: TestCaseSweeper): Map[Int, String, Nothing, IO.ApiIO] =
    swaydb.memory.Map[Int, String, Nothing, IO.ApiIO]().right.value.sweep(_.delete().get)
}

class MultiMapSwayDBSpec4 extends SwayDBSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.persistent.MultiMap_Experimental[Int, Int, String, Nothing, IO.ApiIO](dir = randomDir).get).sweep(_.delete().get)
}

class MultiMapSwayDBSpec5 extends SwayDBSpec {
  val keyValueCount: Int = 10000

  override def newDB()(implicit sweeper: TestCaseSweeper): MapT[Int, String, Nothing, IO.ApiIO] =
    generateRandomNestedMaps(swaydb.memory.MultiMap_Experimental[Int, Int, String, Nothing, IO.ApiIO]().get).sweep(_.delete().get)
}

sealed trait SwayDBSpec extends TestBaseEmbedded {

  def newDB()(implicit sweeper: TestCaseSweeper): SetMapT[Int, String, Nothing, IO.ApiIO]

  implicit val bag = Bag.apiIO

  "SwayDB" should {

    "remove all but first and last" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to 1000) foreach {
              i =>
                db.put(i, i.toString).right.value
            }
            println("Removing .... ")
            doRemove(2, 999, db)
            println("Removed .... ")

            db.stream.materialize.runRandomIO.right.value should contain only((1, "1"), (1000, "1000"))
            db.headOption.right.value.value shouldBe ((1, "1"))
            db.lastOption.right.value.value shouldBe ((1000, "1000"))
        }
      }
    }

    "update only key-values that are not removed" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                (1 to 100) foreach {
                  i =>
                    db.put(i, i.toString).right.value
                }

                (2 to 99) foreach {
                  i =>
                    db.remove(i).right.value
                }

                db.update(1, 100, value = "updated").right.value

                db.stream.materialize.runRandomIO.right.value should contain only((1, "updated"), (100, "updated"))
                db.headOption.right.value.value shouldBe ((1, "updated"))
                db.lastOption.right.value.value shouldBe ((100, "updated"))

              case SetMap(set) =>
              //todo
            }
        }
      }
    }

    "update only key-values that are not removed by remove range" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                (1 to 100) foreach {
                  i =>
                    db.put(i, i.toString).right.value
                }

                db.remove(2, 99).right.value

                db.update(1, 100, value = "updated").right.value

                db.stream.materialize.runRandomIO.right.value should contain only((1, "updated"), (100, "updated"))
                db.headOption.right.value.value shouldBe ((1, "updated"))
                db.lastOption.right.value.value shouldBe ((100, "updated"))

              case SetMap(set) =>
              //todo
            }
        }
      }
    }

    "return only key-values that were not removed from remove range" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                (1 to 100) foreach {
                  i =>
                    db.put(i, i.toString).right.value
                }

                db.update(10, 90, value = "updated").right.value

                db.remove(50, 99).right.value

                val expectedUnchanged =
                  (1 to 9) map {
                    i =>
                      (i, i.toString)
                  }

                val expectedUpdated =
                  (10 to 49) map {
                    i =>
                      (i, "updated")
                  }

                val expected = expectedUnchanged ++ expectedUpdated :+ (100, "100")

                db.stream.materialize.runRandomIO.right.value shouldBe expected
                db.headOption.right.value.value shouldBe ((1, "1"))
                db.lastOption.right.value.value shouldBe ((100, "100"))

              case SetMap(set) =>
              //todo
            }
        }
      }
    }

    "return empty for an empty database with update range" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()
            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>

                db.update(1, Int.MaxValue, value = "updated").right.value

                db.isEmpty.get shouldBe true

                db.stream.materialize.runRandomIO.right.value shouldBe empty

                db.headOption.get shouldBe empty
                db.lastOption.get shouldBe empty

              case SetMap(set) =>
              //todo
            }
        }
      }
    }

    "return empty for an empty database with remove range" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()
            db.remove(1, Int.MaxValue).right.value

            db.isEmpty.get shouldBe true

            db.stream.materialize.runRandomIO.right.value shouldBe empty

            db.headOption.get shouldBe empty
            db.lastOption.get shouldBe empty
        }
      }
    }

    "batch put, remove, update & range remove key-values" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            db match {
              case db: MapT[Int, String, Nothing, IO.ApiIO] =>
                db.commit(Prepare.Put(1, "one"), Prepare.Remove(1)).right.value
                db.get(1).right.value shouldBe empty

                //      remove and then put should return Put's value
                db.commit(Prepare.Remove(1), Prepare.Put(1, "one")).right.value
                db.get(1).right.value.value shouldBe "one"

                //remove range and put should return Put's value
                db.commit(Prepare.Remove(1, 100), Prepare.Put(1, "one")).right.value
                db.get(1).right.value.value shouldBe "one"

                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one one"), Prepare.Update(1, 100, "updated"), Prepare.Remove(1, 100)).right.value
                db.get(1).right.value shouldBe empty
                db.isEmpty.get shouldBe true
                //
                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one one"), Prepare.Remove(1, 100), Prepare.Update(1, 100, "updated")).right.value
                db.get(1).right.value shouldBe empty
                db.isEmpty.get shouldBe true

                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(1, "one again"), Prepare.Update(1, 100, "updated")).right.value
                db.get(1).right.value.value shouldBe "updated"
                db.stream.materialize.map(_.toMap).right.value.values should contain only "updated"

                db.commit(Prepare.Put(1, "one"), Prepare.Put(2, "two"), Prepare.Put(100, "hundred"), Prepare.Remove(1, 100), Prepare.Update(1, 1000, "updated")).right.value
                db.stream.materialize.runRandomIO.right.value shouldBe empty

              case SetMap(set) =>
              //TODO
            }
        }
      }
    }

    "perform from, until, before & after" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to 10000) foreach {
              i =>
                db.put(i, i.toString).right.value
            }

            db.stream.from(9999).materialize.runRandomIO.right.value should contain only((9999, "9999"), (10000, "10000"))
            db.stream.from(9998).drop(2).take(1).materialize.runRandomIO.right.value should contain only ((10000, "10000"))
            db.stream.before(9999).take(1).materialize.runRandomIO.right.value should contain only ((9998, "9998"))
            db.stream.after(9999).take(1).materialize.runRandomIO.right.value should contain only ((10000, "10000"))
            db.stream.after(9999).drop(1).materialize.runRandomIO.right.value shouldBe empty

            db.stream.after(10).takeWhile(_._1 <= 11).materialize.runRandomIO.right.value should contain only ((11, "11"))
            db.stream.after(10).takeWhile(_._1 <= 11).drop(1).materialize.runRandomIO.right.value shouldBe empty

            db.stream.fromOrBefore(0).materialize.runRandomIO.right.value shouldBe empty
            db.stream.fromOrAfter(0).take(1).materialize.runRandomIO.right.value should contain only ((1, "1"))
        }
      }
    }

    "perform mightContain & contains" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to 10000) foreach {
              i =>
                db.put(i, i.toString).right.value
            }

            (1 to 10000) foreach {
              i =>
                db.mightContain(i).right.value shouldBe true
                db.contains(i).right.value shouldBe true
            }

            //      db.mightContain(Int.MaxValue).runIO shouldBe false
            //      db.mightContain(Int.MinValue).runIO shouldBe false
            db.contains(20000).right.value shouldBe false
        }
      }
    }

    "contains on removed should return false" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to 100000) foreach {
              i =>
                db.put(i, i.toString).value
            }

            (1 to 100000) foreach {
              i =>
                db.remove(i).right.value
            }

            (1 to 100000) foreach {
              i =>
                db.contains(i).right.value shouldBe false
            }

            db.contains(100001).right.value shouldBe false
        }
      }
    }

    "delete" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val db = newDB()

            (1 to 100000) foreach {
              i =>
                db.put(i, i.toString).right.value
            }

            db.delete().value

            Files.exists(db.path) shouldBe false
        }
      }
    }

    "return valueSize" in {
      //      val db = newDB()
      //
      //      (1 to 10000) foreach {
      //        i =>
      //          db.put(i, i.toString).runIO
      //      }

      //      (1 to 10000) foreach {
      //        i =>
      //          db.valueSize(i.toString).runIO shouldBe i.toString.getBytes(StandardCharsets.UTF_8).length
      //      }
    }

    "not allow api calls" when {
      "closed" in {
        runThis(times = repeatTest, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              val db = newDB()

              (1 to 100000) foreach {
                i =>
                  db.put(i, i.toString).right.value
              }

              db.close().value

              db.get(1).left.value.exception.getMessage shouldBe Core.closedMessage
          }
        }
      }

      "deleted" in {
        runThis(times = repeatTest, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              val db = newDB()

              (1 to 100000) foreach {
                i =>
                  db.put(i, i.toString).right.value
              }

              db.delete().value

              db.get(1).left.value.exception.getMessage shouldBe Core.closedMessage
          }
        }
      }
    }

    //    "eventually remove all Segments from the database when remove range is submitted" in {
    //      val db = newDB()
    //
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString).runIO
    //      }
    //
    //      db.remove(1, 2000000).runIO
    //
    //      assertLevelsAreEmpty(db, submitUpdates = true)
    //
    //      db.closeDatabase().get
    //    }
    //
    //    "eventually remove all Segments from the database when expire range is submitted" in {
    //      val db = newDB()
    //
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString).runIO
    //      }
    //
    //      db.expire(1, 2000000, 5.minutes).runIO
    //      println("Expiry submitted.")
    //
    //      assertLevelsAreEmpty(db, submitUpdates = true)
    //
    //      db.closeDatabase().get
    //    }
    //
    //    "eventually remove all Segments from the database when put is submitted with expiry" in {
    //      val db = newDB()
    //
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString, 5.minute).runIO
    //      }
    //
    //      assertLevelsAreEmpty(db, submitUpdates = false)
    //
    //      db.closeDatabase().get
    //    }
    //  }

    //  "debug" in {
    //    val db = newDB()
    //
    //    def doRead() =
    //      Future {
    //        while (true) {
    //          (1 to 2000000) foreach {
    //            i =>
    //              db.get(i).runIO shouldBe i.toString
    //              if (i % 100000 == 0)
    //                println(s"Read $i")
    //          }
    //        }
    //      }
    //
    //    def doWrite() = {
    //      println("writing")
    //      (1 to 2000000) foreach {
    //        i =>
    //          db.put(i, i.toString).runIO
    //          if (i % 100000 == 0)
    //            println(s"Write $i")
    //      }
    //      println("writing end")
    //    }
    //
    //    doWrite()
    //    doRead()
    //    sleep(1.minutes)
    //    doWrite()
    //    sleep(1.minutes)
    //    doWrite()
    //    sleep(1.minutes)
    //    doWrite()
    //    assertLevelsAreEmpty(db, submitUpdates = false)
    //
    //    db.closeDatabase().get
    //  }
    //
    //  "debug before and mapRight" in {
    //    val db = newDB()
    //
    //    (1 to 10) foreach {
    //      i =>
    //        db.put(i, i.toString).runIO
    //    }
    //
    //    //    db.before(5).toList foreach println
    //    db.before(5).reverse.map { case (k, v) => (k, v) } foreach println
    //
    //    db.closeDatabase().get
    //  }
  }
}
