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
//import swaydb.api.TestBaseAPI
//import swaydb.core.TestSweeper
//import swaydb.core.TestSweeper._
//import swaydb.core.CoreTestData._
//import swaydb.serializers.Default._
//import swaydb.{Glass, MultiMap}
//import swaydb.testkit.TestKit._
//import swaydb.core.file.CoreFileTestKit._
//
//class MultiMapPutSpec0 extends MultiMapPutSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): swaydb.MultiMap[Int, Int, String, Nothing, Glass] =
//    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Glass](dir = randomDir()).sweep(_.delete())
//}
//
//class MultiMapPutSpec1 extends MultiMapPutSpec {
//  val keyValueCount: Int = 1000
//
//  override def newDB()(implicit sweeper: TestSweeper): swaydb.MultiMap[Int, Int, String, Nothing, Glass] =
//    swaydb.memory.MultiMap[Int, Int, String, Nothing, Glass]().sweep(_.delete())
//}
//
//sealed trait MultiMapPutSpec extends TestBaseAPI {
//
//  val keyValueCount: Int
//
//  def newDB()(implicit sweeper: TestSweeper): swaydb.MultiMap[Int, Int, String, Nothing, Glass]
//
//  "Root" should {
//    "Initialise a RootMap & SubMap from Root" in {
//      TestSweeper {
//        implicit sweeper =>
//
//          val root = newDB()
//
//          var child1 = root.child(1)
//          var child2 = root.child(2)
//
//          if (randomBoolean()) child1 = root.getChild(1).value
//          if (randomBoolean()) child2 = root.getChild(2).value
//
//          child1.put(3, "three")
//          child1.put(4, "four")
//          child2.put(5, "five")
//          child2.put(4, "four again")
//
//          child1
//            .materialize
//            .toList should contain inOrderOnly((3, "three"), (4, "four"))
//
//          child2
//            .materialize
//            .toList should contain inOrderOnly((4, "four again"), (5, "five"))
//      }
//    }
//
//    "Initialise a RootMap & 2 SubMaps from Root" in {
//      TestSweeper {
//        implicit sweeper =>
//
//          val root = newDB()
//
//          def insert(firstMap: MultiMap[Int, Int, String, Nothing, Glass]) = {
//            firstMap.put(3, "three")
//            firstMap.put(4, "four")
//            firstMap.put(5, "five")
//            firstMap.put(4, "four again")
//          }
//
//          val child1 = root.child(1)
//
//          val child2 = child1.child(2)
//          insert(child2)
//
//          val child3 = child1.child(3)
//          insert(child3)
//
//          child1.isEmpty shouldBe true
//
//          child2
//            .materialize
//            .toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
//
//          child3
//            .materialize
//            .toList should contain inOrderOnly((3, "three"), (4, "four again"), (5, "five"))
//      }
//    }
//
//    "Initialise 2 RootMaps & 2 SubMaps under each SubMap" in {
//      TestSweeper {
//        implicit sweeper =>
//
//          val root = newDB()
//
//          var root1 = root.child(1)
//          var root2 = root.child(2)
//          if (randomBoolean()) root1 = root.getChild(1).value
//          if (randomBoolean()) root2 = root.getChild(2).value
//
//          var sub11 = root1.child(1)
//          var sub12 = root1.child(2)
//          if (randomBoolean()) sub11 = root1.getChild(1).value
//          if (randomBoolean()) sub12 = root1.getChild(2).value
//          sub11.put(1, "one")
//          sub12.put(2, "two")
//          if (randomBoolean()) sub11 = root1.getChild(1).value
//          if (randomBoolean()) sub12 = root1.getChild(2).value
//
//          var sub21 = root2.child(1)
//          var sub22 = root2.child(2)
//          if (randomBoolean()) sub21 = root2.getChild(1).value
//          if (randomBoolean()) sub22 = root2.getChild(2).value
//          sub21.put(1, "1")
//          sub22.put(2, "2")
//          if (randomBoolean()) sub21 = root2.getChild(1).value
//          if (randomBoolean()) sub22 = root2.getChild(2).value
//
//          sub11.get(1).value shouldBe "one"
//          sub12.get(2).value shouldBe "two"
//
//          sub21.get(1).value shouldBe "1"
//          sub22.get(2).value shouldBe "2"
//
//          root.childrenKeys.materialize.toList should have size 2
//
//          val rootSubMaps = root.children.materialize
//          rootSubMaps.foreach(_.isEmpty shouldBe true) //has no map entries
//
//          val subMaps = rootSubMaps.flatMap(_.children.materialize).toList
//          subMaps should have size 4
//
//          subMaps(0).get(1).value shouldBe "one"
//          subMaps(1).get(2).value shouldBe "two"
//          subMaps(2).get(1).value shouldBe "1"
//          subMaps(3).get(2).value shouldBe "2"
//      }
//    }
//  }
//}
