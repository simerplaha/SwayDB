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
//package swaydb.api.multimap
//
//import org.scalatest.OptionValues._
//import swaydb.api.{TestBaseAPI, repeatTest}
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestSweeper
//import swaydb.core.TestSweeper._
//import swaydb.core.CoreTestData._
//import swaydb.multimap.MultiPrepare
//import swaydb.serializers.Default._
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//import swaydb.utils.StorageUnits._
//import swaydb.{Bag, Glass, IO, MultiMap, Prepare}
//
//import scala.concurrent.duration._
//import scala.util.Random
//import swaydb.testkit.TestKit._
//import swaydb.core.file.CoreFileTestKit._
//
//class MultiMapSpec0 extends MultiMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
//    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Glass](dir = randomDir()).sweep(_.delete())
//}
//
//class MultiMapSpec1 extends MultiMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
//    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Glass](dir = randomDir(), logSize = 1.byte).sweep(_.delete())
//}
//
//class MultiMapSpec2 extends MultiMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
//    swaydb.memory.MultiMap[Int, Int, String, Nothing, Glass]().sweep(_.delete())
//}
//
//class MultiMapSpec3 extends MultiMapSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
//    swaydb.memory.MultiMap[Int, Int, String, Nothing, Glass](logSize = 1.byte).sweep(_.delete())
//}
//
//sealed trait MultiMapSpec extends TestBaseAPI {
//
//  val keyValueCount: Int
//
//  def newDB()(implicit sweeper: TestSweeper): MultiMap[Int, Int, String, Nothing, Glass]
//
//  implicit val bag = Bag.glass
//
//  //  implicit val mapKeySerializer = Key.serializer(IntSerializer)
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//
//  /**
//   * The following displays the MultiMap hierarchy existing for the Map.
//   */
//  //root                          - (0, "zero")
//  //  |______ child1              - (1, "one"), (2, "two")
//  //  |          |_______ child11 - (3, "three"), (4, "four")
//  //  |          |_______ child12 - (5, "five"), (6, "six")
//  //  |
//  //  |______ child2              - (7, "seven"), (8, "eight")
//  //  |          |_______ child21 - (9, "nine"), (10, "ten")
//  //  |          |_______ child22 - (11, "eleven"), (12, "twelve")
//
//  def buildRootMap()(implicit sweeper: TestSweeper): MultiMap[Int, Int, String, Nothing, Glass] = {
//    val root = newDB()
//    root.put(0, "zero")
//
//    /**
//     * Child1 hierarchy
//     */
//    val child1 = root.child(1)
//    child1.put(1, "one")
//    child1.put(2, "two")
//
//    val child11 = child1.child(11)
//    child11.put(3, "three")
//    child11.put(4, "four")
//
//    val child12 = child1.child(12)
//    child12.put(5, "five")
//    child12.put(6, "six")
//
//    /**
//     * Child2 hierarchy
//     */
//    val child2 = root.child(2)
//    child2.put(7, "seven")
//    child2.put(8, "eight")
//
//    val child21 = child2.child(21)
//    child21.put(9, "nine")
//    child21.put(10, "ten")
//
//    val child22 = child2.child(22)
//    child22.put(11, "eleven")
//    child22.put(12, "twelve")
//
//    root.get(0).value shouldBe "zero"
//    root.childrenKeys.materialize.toList shouldBe List(1, 2)
//
//    child1.get(1).value shouldBe "one"
//    child1.get(2).value shouldBe "two"
//    child1.childrenKeys.materialize.toList shouldBe List(11, 12)
//    child11.get(3).value shouldBe "three"
//    child11.get(4).value shouldBe "four"
//    child12.get(5).value shouldBe "five"
//    child12.get(6).value shouldBe "six"
//
//    root
//  }
//
//  "put" should {
//    "basic" in {
//      TestSweeper {
//        implicit sweeper =>
//          val root = buildRootMap()
//          root.put(key = 0, value = "zero1")
//          root.get(key = 0).value shouldBe "zero1"
//
//          val child1 = root.getChild(1).value
//          val child11 = child1.getChild(11).value
//          val child12 = child1.getChild(12).value
//          //update the last child's key-value only and everything else should remain the same
//          child12.put(6, "updated")
//          child12.get(5).value shouldBe "five"
//          child12.get(6).value shouldBe "updated"
//          child1.get(1).value shouldBe "one"
//          child1.get(2).value shouldBe "two"
//          child11.get(3).value shouldBe "three"
//          child11.get(4).value shouldBe "four"
//
//          val child2 = root.getChild(2).value
//          val child21 = child2.getChild(21).value
//          val child22 = child2.getChild(22).value
//          //update the last child's key-value only and everything else should remain the same
//          child22.put(12, "updated")
//          child22.get(11).value shouldBe "eleven"
//          child22.get(12).value shouldBe "updated"
//          child2.get(7).value shouldBe "seven"
//          child2.get(8).value shouldBe "eight"
//          child21.get(9).value shouldBe "nine"
//          child21.get(10).value shouldBe "ten"
//      }
//    }
//
//    "stream" in {
//      TestSweeper {
//        implicit sweeper =>
//          val root = newDB()
//
//          val stream = swaydb.Stream.range(1, 10).map(int => (int, int.toString))
//          root.put(stream)
//
//          root.materialize.toList shouldBe (1 to 10).map(int => (int, int.toString)).toList
//      }
//    }
//
//    //    "benchmark" in {
//    //      TestSweeper {
//    //        implicit sweeper =>
//    //          val root = newDB()
//    //
//    //          val child = root.init(2)
//    //
//    //          Benchmark("") {
//    //            (1 to 1000000) foreach {
//    //              i =>
//    //                child.put(i, i.toString)
//    //            }
//    //          }
//    //
//    //          Benchmark("") {
//    //            (1 to 1000000) foreach {
//    //              i =>
//    //                child.get(i).value shouldBe i.toString
//    //            }
//    //          }
//    //      }
//    //    }
//
//    "put a stream to an expired map and also submit key-values with deadline later than map's deadline" in {
//      TestSweeper {
//        implicit sweeper =>
//          val root = newDB()
//          val deadline = 2.seconds.fromNow
//
//          //expired map
//          val child = root.child(1, deadline)
//
//          //inserting a stream into child which has deadline set will also set deadline values for these key-values
//          val stream = swaydb.Stream.range(1, 10).map(int => (int, int.toString))
//          child.put(stream)
//
//          (1 to 10) foreach {
//            i =>
//              Seq(
//                //random overwrite the key-value with another deadline which should be ignored
//                //because the map expires earlier.
//                () => if (Random.nextBoolean()) child.put(i, i.toString, 1.hour),
//                //value is set
//                () => child.get(i).value shouldBe i.toString,
//                //child has values set
//                () => child.expiration(i).value shouldBe deadline
//              ).runThisRandomly
//          }
//
//          eventual(deadline.timeLeft) {
//            child.isEmpty shouldBe true
//            child.hasChildren shouldBe false
//          }
//      }
//    }
//
//    "put a stream to an expired map and also submit key-values with deadline earlier than map's deadline" in {
//      TestSweeper {
//        implicit sweeper =>
//          val root = newDB()
//          val mapDeadline = 1.hour.fromNow
//
//          //expired map
//          val child = root.child(1, mapDeadline)
//
//          //inserting a stream into child which has deadline set will also set deadline values for these key-values
//          val stream = swaydb.Stream.range(1, 10).map(int => (int, int.toString))
//          child.put(stream)
//
//          val keyValueDeadline = 2.seconds.fromNow
//
//          (1 to 10) foreach {
//            i =>
//              //write all key-values with an earlier deadline.
//              child.put(i, i.toString, keyValueDeadline)
//
//              Seq(
//                //value is set
//                () => child.get(i).value shouldBe i.toString,
//                //child has values set
//                () => child.expiration(i).value shouldBe keyValueDeadline
//              ).runThisRandomly
//          }
//
//          eventual(keyValueDeadline.timeLeft) {
//            child.isEmpty shouldBe true
//            child.hasChildren shouldBe false
//          }
//          //root has only 1 child
//          root.childrenKeys.materialize.toList should contain only 1
//      }
//    }
//
//    "put & expire" in {
//      runThis(times = repeatTest, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//
//            val root = newDB()
//            root.put(1, "one", 2.second)
//            root.get(1).value shouldBe "one"
//
//            //childMap with default expiration set expiration
//            val child1 = root.child(2, 2.second)
//            child1.put(1, "one") //inserting key-value without expiration uses default expiration.
//            child1.get(1).value shouldBe "one"
//            child1.expiration(1) shouldBe defined
//
//            child1.put((1, "one"), (2, "two"), (3, "three"))
//
//            eventual(3.seconds) {
//              root.get(1) shouldBe empty
//            }
//
//            //child map is expired
//            eventual(2.seconds) {
//              root.childrenKeys.materialize.toList shouldBe empty
//            }
//
//            child1.isEmpty shouldBe true
//        }
//      }
//    }
//  }
//
//  "remove" should {
//    "remove key-values and map" in {
//      TestSweeper {
//        implicit sweeper =>
//          //hierarchy - root --> child1 --> child2 --> child3
//          //                                       --> child4
//          val root = newDB()
//          root.put(1, "one")
//          root.put(2, "two")
//
//          val child1 = root.child(1)
//          child1.put(1, "one")
//          child1.put(2, "two")
//
//          val child2 = child1.child(2)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          val child3 = child2.child(3)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          val child4 = child2.child(4)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          //remove key-values should last child
//          eitherOne(child2.remove(Seq(1, 2)), child2.remove(from = 1, to = 2))
//          child2.materialize.toList shouldBe empty
//
//          //check 2nd child has it's key-values
//          child1.materialize.toList.map(_._1) shouldBe List(1, 2)
//          //all maps exists.
//          root.childrenKeys.materialize.toList shouldBe List(1)
//          child1.childrenKeys.materialize.toList shouldBe List(2)
//          child2.childrenKeys.materialize.toList shouldBe List(3, 4)
//
//          //remove child1
//          root.removeChild(1)
//          root.childrenKeys.materialize.toList shouldBe empty
//          child1.materialize.toList shouldBe empty
//          child2.materialize.toList shouldBe empty
//      }
//    }
//
//    "remove sibling map" in {
//      TestSweeper {
//        implicit sweeper =>
//          //hierarchy - root --> child1 --> child2 --> child3
//          //                                       --> child4 (remove this)
//          val root = newDB()
//          root.put(1, "one")
//          root.put(2, "two")
//
//          val child1 = root.child(1)
//          child1.put(1, "one")
//          child1.put(2, "two")
//
//          val child2 = child1.child(2)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          val child3 = child2.child(3)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          val child4 = child2.child(4)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          child2.removeChild(4)
//
//          //all maps exists.
//          root.childrenKeys.materialize.toList shouldBe List(1)
//          child1.childrenKeys.materialize.toList shouldBe List(2)
//          child2.childrenKeys.materialize.toList shouldBe List(3)
//      }
//    }
//
//    "remove map's key-values" in {
//      TestSweeper {
//        implicit sweeper =>
//          val root = newDB()
//          root.put(1, "one")
//          root.put(2, "two")
//
//          val child1 = root.child(1)
//          child1.put(1, "one")
//          child1.put(2, "two")
//
//          val child2 = child1.child(2)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          //remove child 2
//          child1.removeChild(2)
//          child1.getChild(2) shouldBe empty
//          child2.get(1) shouldBe empty
//          child2.get(2) shouldBe empty
//
//          child1.get(1).value shouldBe "one"
//          child1.get(2).value shouldBe "two"
//
//          root.get(1).value shouldBe "one"
//          root.get(2).value shouldBe "two"
//      }
//    }
//  }
//
//  "expire child map" in {
//    TestSweeper {
//      implicit sweeper =>
//
//        //hierarchy - root --> child1 --> child2
//        val root = newDB()
//        root.put(1, "one")
//        root.put(2, "two")
//
//        val child1 = root.child(1)
//        child1.put(1, "one")
//        child1.put(2, "two")
//
//        //create last child with expiration
//        val child11 = child1.child(11, 3.seconds)
//        child11.put(1, "one")
//        child11.put(2, "two", 1.hour)
//        //expiration should be set to 3.seconds and not 1.hour since the map expires is 3.seconds.
//        child11.expiration(2).value.timeLeft.minus(4.seconds).fromNow.hasTimeLeft() shouldBe false
//
//        //last child is not empty
//        child11.materialize.toList should not be empty
//
//        //last child is empty
//        eventual(4.seconds) {
//          child11.materialize.toList shouldBe empty
//        }
//
//        //parent child of last child is not empty
//        child1.materialize.toList should not be empty
//        //parent child of last child has no child maps.
//        child1.childrenKeys.materialize.toList shouldBe empty
//    }
//  }
//
//  "update & clearKeyValues" in {
//    TestSweeper {
//      implicit sweeper =>
//        //hierarchy - root --> child1 --> child2
//        val root = newDB()
//        root.put(1, "one")
//        root.put(2, "two")
//
//        val child1 = root.child(1)
//        child1.put(1, "one")
//        child1.put(2, "two")
//
//        val child11 = child1.child(11)
//        child11.put(1, "one")
//        child11.put(2, "two")
//
//        //update just the last child.
//        child1.update((1, "updated"), (2, "updated"))
//        child1.materialize.toList shouldBe List((1, "updated"), (2, "updated"))
//
//        //everything else remains the same.
//        root.materialize.toList shouldBe List((1, "one"), (2, "two"))
//        child11.materialize.toList shouldBe List((1, "one"), (2, "two"))
//
//        root.clearKeyValues()
//        root.isEmpty shouldBe true
//
//        child11.materialize.toList shouldBe List((1, "one"), (2, "two"))
//    }
//  }
//
//  "children" should {
//    "can be flatten into a single stream" in {
//      TestSweeper {
//        implicit sweeper =>
//          val root = newDB()
//
//          root
//            .child(1)
//            .child(2)
//            .child(3)
//            .child(4)
//
//          root.childrenFlatten.materialize.toList should have size 4
//
//          root.childrenFlatten.map(_.mapKey).materialize.toList shouldBe (1 to 4)
//      }
//    }
//
//
//    "replace" in {
//      TestSweeper {
//        implicit sweeper =>
//          //hierarchy - root --> child1 --> child2 --> child3
//          //                                       --> child4
//          val root = newDB()
//          root.put(1, "one")
//          root.put(2, "two")
//
//          val child1 = root.child(1)
//          child1.put(1, "one")
//          child1.put(2, "two")
//
//          var child2 = child1.child(2)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          val child3 = child2.child(3)
//          child3.put(1, "one")
//          child3.put(2, "two")
//
//          val child4 = child2.child(4)
//          child4.put(1, "one")
//          child4.put(2, "two")
//
//          child1.get(1).value shouldBe "one"
//          child1.get(2).value shouldBe "two"
//
//          //replace last with expiration
//          child2 = child1.replaceChild(2, 3.second)
//          //child1 now only have one child 2 removing 3 and 4
//          child1.childrenKeys.materialize.toList should contain only 2
//
//          child2.isEmpty shouldBe true //last has no key-values
//          child2.put(1, "one") //add a key-value
//          child2.isEmpty shouldBe false //has a key-value now
//
//          child3.isEmpty shouldBe true
//          child4.isEmpty shouldBe true
//
//          eventual(4.seconds)(child2.isEmpty shouldBe true) //which eventually expires
//          child1.childrenKeys.materialize.toList shouldBe empty
//      }
//    }
//
//    "replace sibling" in {
//      TestSweeper {
//        implicit sweeper =>
//          //root --> child1 (replace this)
//          //     --> child2
//          val root = newDB()
//          root.put(1, "one")
//          root.put(2, "two")
//
//          val child1 = root.child(1)
//          child1.put(1, "one")
//          child1.put(2, "two")
//          generateRandomNestedMaps(child1.toBag[IO.ApiIO])
//
//          val child2 = root.child(2)
//          child2.put(1, "one")
//          child2.put(2, "two")
//
//          root.replaceChild(1)
//
//          //child1 is replaced and is removed
//          child1.isEmpty shouldBe true
//
//          //child2 is untouched.
//          child2.isEmpty shouldBe false
//
//          child1.hasChildren shouldBe false
//          child1.get(1) shouldBe empty
//          child1.get(2) shouldBe empty
//      }
//    }
//
//    "replace removes all child maps" in {
//      runThis(10.times) {
//        TestSweeper {
//          implicit sweeper =>
//            val root = newDB()
//
//            //randomly generate 100 child of random hierarchy.
//            (1 to 100).foldLeft(root) {
//              case (parent, i) =>
//                val child = parent.child(i)
//
//                //randomly add data to the child
//                if (Random.nextBoolean())
//                  (3 to Random.nextInt(10)) foreach {
//                    i =>
//                      child.put(i, i.toString)
//                  }
//
//                //randomly return the child or the root to create randomly hierarchies
//                if (Random.nextBoolean())
//                  child
//                else
//                  parent
//            }
//            //100 children in total are created
//            root.childrenFlatten.materialize should have size 100
//            //root should have more than one child
//            root.childrenKeys.materialize.size should be >= 1
//
//            //random set a deadline
//            val deadline = randomDeadlineOption(false)
//            //replace all children of root map.
//            root.childrenKeys.materialize.foreach(child => root.replaceChild(child, deadline))
//
//            //root's children do not contain any children of their own.
//            root.children.materialize.foreach {
//              child =>
//                //has no children
//                child.hasChildren shouldBe false
//                //has no entries
//                child.isEmpty shouldBe true
//                //deadline is set
//                child.defaultExpiration shouldBe deadline
//            }
//        }
//      }
//    }
//
//    "init" when {
//      "updated expiration" in {
//        TestSweeper {
//          implicit sweeper =>
//            //hierarchy - root --> child1 --> child2
//            val root = newDB()
//            root.put(1, "one")
//            root.put(2, "two")
//
//            //initial child with expiration.
//            val deadline = 1.hour.fromNow
//            var child1 = root.child(1, deadline)
//            child1.put(1, "one")
//            child1.put(2, "two")
//
//            val child2 = child1.child(2)
//            child2.put(1, "one")
//            child2.put(2, "two")
//
//            //re-init child1 with the same expiration has no effect.
//            child1 = root.child(1, deadline)
//            child1.get(1).value shouldBe "one"
//            child1.get(2).value shouldBe "two"
//            child2.get(1).value shouldBe "one"
//            child2.get(2).value shouldBe "two"
//
//            //re-init with updated expiration should expire everything.
//            child1 = root.child(1, 0.second)
//            child1.childrenKeys.materialize.toList shouldBe empty
//            child1.isEmpty shouldBe true
//
//            //re-init the same expired map with a new map does not set expiration.
//            child1 = root.child(1)
//            child1.put(1, "one")
//            child1.get(1).value shouldBe "one"
//            child1.expiration(1) shouldBe empty
//        }
//      }
//    }
//  }
//
//  "transaction" in {
//    TestSweeper {
//      implicit sweeper =>
//        //hierarchy - root --> child1 --> child2 --> child3
//        val root = newDB()
//        root.put(1, "one")
//        root.put(2, "two")
//
//        val child1 = root.child(1)
//        child1.put(1, "one")
//        child1.put(2, "two")
//
//        val child2 = child1.child(2)
//        child2.put(1, "one")
//        child2.put(2, "two")
//
//        val child3 = child1.child(3)
//        child3.put(1, "one")
//        child3.put(2, "two")
//
//        val transaction =
//          MultiPrepare(child3, Prepare.Put(3, "three")) ++
//            MultiPrepare(child2, Prepare.Put(4, "four"))
//
//        child3.commitMultiPrepare(transaction)
//
//        child3.get(3).value shouldBe "three"
//        child2.get(4).value shouldBe "four"
//    }
//  }
//}
