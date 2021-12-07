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

package swaydb.api.multimap

import org.scalatest.OptionValues._
import swaydb.api.TestBaseAPI
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.serializers.Default._
import swaydb.utils.StorageUnits._
import swaydb.{Bag, Glass, MultiMap}

class MultiMapIterationSpec0 extends MultiMapIterationSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Glass](dir = randomDir).sweep(_.delete())
}

class MultiMapIterationSpec1 extends MultiMapIterationSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Glass](dir = randomDir, logSize = 1.byte).sweep(_.delete())
}

class MultiMapIterationSpec2 extends MultiMapIterationSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
    swaydb.memory.MultiMap[Int, Int, String, Nothing, Glass]().sweep(_.delete())
}

class MultiMapIterationSpec3 extends MultiMapIterationSpec {
  val keyValueCount: Int = 1000

  override def newDB()(implicit sweeper: TestCaseSweeper): MultiMap[Int, Int, String, Nothing, Glass] =
    swaydb.memory.MultiMap[Int, Int, String, Nothing, Glass](logSize = 1.byte).sweep(_.delete())
}

sealed trait MultiMapIterationSpec extends TestBaseAPI {

  val keyValueCount: Int

  def newDB()(implicit sweeper: TestCaseSweeper): MultiMap[Int, Int, String, Nothing, Glass]

  implicit val bag = Bag.glass

  "Iteration" should {
    "exclude & include subMap by default" in {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          val firstMap = db.child(1)
          val secondMap = firstMap.child(2)
          val subMap1 = secondMap.child(3)
          val subMap2 = secondMap.child(4)

          firstMap.materialize.toList shouldBe empty
          firstMap.childrenKeys.materialize.toList should contain only 2

          secondMap.materialize.toList shouldBe empty
          secondMap.childrenKeys.materialize.toList should contain only(3, 4)

          subMap1.materialize.toList shouldBe empty
          subMap2.materialize.toList shouldBe empty
      }
    }
  }

  "Iteration" when {
    "the map contains 1 element" in {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          val firstMap = db.child(1)
          val secondMap = firstMap.child(2)

          firstMap.materialize.toList shouldBe empty
          firstMap.childrenKeys.materialize.toList should contain only 2

          secondMap.put(1, "one")
          secondMap.count shouldBe 1

          secondMap.head.value shouldBe ((1, "one"))
          secondMap.last.value shouldBe ((1, "one"))

          secondMap.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.toList should contain only ((2, "one"))
          secondMap.foldLeft(List.empty[(Int, String)]) { case (_, keyValue) => List(keyValue) } shouldBe List((1, "one"))
          secondMap.reverse.foldLeft(List.empty[(Int, String)]) { case (_, keyValue) => List(keyValue) } shouldBe List((1, "one"))
          secondMap.reverse.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.toList should contain only ((2, "one"))
          secondMap.reverse.take(100).materialize.toList should contain only ((1, "one"))
          secondMap.reverse.take(1).materialize.toList should contain only ((1, "one"))
          secondMap.take(100).materialize.toList should contain only ((1, "one"))
          secondMap.take(1).materialize.toList should contain only ((1, "one"))
          secondMap.reverse.drop(1).materialize.toList shouldBe empty
          secondMap.drop(1).materialize.toList shouldBe empty
          secondMap.reverse.drop(0).materialize.toList should contain only ((1, "one"))
          secondMap.drop(0).materialize.toList should contain only ((1, "one"))
      }
    }

    "the map contains 2 elements" in {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          val rootMap = db.child(1)
          val firstMap = rootMap.child(2)

          firstMap.put(1, "one")
          firstMap.put(2, "two")

          firstMap.count shouldBe 2
          firstMap.head.value shouldBe ((1, "one"))
          firstMap.last.value shouldBe ((2, "two"))

          firstMap.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.toList shouldBe List((2, "one"), (3, "two"))
          firstMap.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue } shouldBe List((1, "one"), (2, "two"))
          firstMap.reverse.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue } shouldBe List((2, "two"), (1, "one"))
          firstMap.reverse.map(keyValue => keyValue).materialize.toList shouldBe List((2, "two"), (1, "one"))
          firstMap.reverse.take(100).materialize.toList shouldBe List((2, "two"), (1, "one"))
          firstMap.reverse.take(2).materialize.toList shouldBe List((2, "two"), (1, "one"))
          firstMap.reverse.take(1).materialize.toList should contain only ((2, "two"))
          firstMap.take(100).materialize.toList should contain only((1, "one"), (2, "two"))
          firstMap.take(2).materialize.toList should contain only((1, "one"), (2, "two"))
          firstMap.take(1).materialize.toList should contain only ((1, "one"))
          firstMap.reverse.drop(1).materialize.toList should contain only ((1, "one"))
          firstMap.drop(1).materialize.toList should contain only ((2, "two"))
          firstMap.reverse.drop(0).materialize.toList shouldBe List((2, "two"), (1, "one"))
          firstMap.drop(0).materialize.toList shouldBe List((1, "one"), (2, "two"))
      }
    }

    "Sibling maps" in {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          val rootMap = db.child(1)

          val subMap1 = rootMap.child(2)
          subMap1.put(1, "one")
          subMap1.put(2, "two")

          val subMap2 = rootMap.child(3)
          subMap2.put(3, "three")
          subMap2.put(4, "four")

          rootMap.materialize.toList shouldBe empty
          rootMap.childrenKeys.materialize.toList should contain only(2, 3)

          //FIRST MAP ITERATIONS
          subMap1.count shouldBe 2
          subMap1.head.value shouldBe ((1, "one"))
          subMap1.last.value shouldBe ((2, "two"))
          subMap1.map(keyValue => (keyValue._1 + 1, keyValue._2)).materialize.toList shouldBe List((2, "one"), (3, "two"))
          subMap1.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue } shouldBe List((1, "one"), (2, "two"))
          subMap1.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous } shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.map(keyValue => keyValue).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.take(100).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.take(2).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.take(1).materialize.toList should contain only ((2, "two"))
          subMap1.take(100).materialize.toList should contain only((1, "one"), (2, "two"))
          subMap1.take(2).materialize.toList should contain only((1, "one"), (2, "two"))
          subMap1.take(1).materialize.toList should contain only ((1, "one"))
          subMap1.reverse.drop(1).materialize.toList should contain only ((1, "one"))
          subMap1.drop(1).materialize.toList should contain only ((2, "two"))
          subMap1.reverse.drop(0).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.drop(0).materialize.toList shouldBe List((1, "one"), (2, "two"))

          //SECOND MAP ITERATIONS
          subMap2.count shouldBe 2
          subMap2.head.value shouldBe ((3, "three"))
          subMap2.last.value shouldBe ((4, "four"))
          subMap2.map(keyValue => (keyValue._1, keyValue._2)).materialize.toList shouldBe List((3, "three"), (4, "four"))
          subMap2.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue } shouldBe List((3, "three"), (4, "four"))
          subMap2.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous } shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.map(keyValue => keyValue).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.take(100).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.take(2).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.take(1).materialize.toList should contain only ((4, "four"))
          subMap2.take(100).materialize.toList should contain only((3, "three"), (4, "four"))
          subMap2.take(2).materialize.toList should contain only((3, "three"), (4, "four"))
          subMap2.take(1).materialize.toList should contain only ((3, "three"))
          subMap2.reverse.drop(1).materialize.toList should contain only ((3, "three"))
          subMap2.drop(1).materialize.toList should contain only ((4, "four"))
          subMap2.reverse.drop(0).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.drop(0).materialize.toList shouldBe List((3, "three"), (4, "four"))
      }
    }

    "nested maps" in {
      TestCaseSweeper {
        implicit sweeper =>

          val db = newDB()

          val rootMap = db.child(1)

          val subMap1 = rootMap.child(2)
          subMap1.put(1, "one")
          subMap1.put(2, "two")

          val subMap2 = subMap1.child(3)
          subMap2.put(3, "three")
          subMap2.put(4, "four")

          rootMap.materialize.toList shouldBe empty
          rootMap.childrenKeys.materialize.toList should contain only 2

          //FIRST MAP ITERATIONS
          subMap1.count shouldBe 2
          subMap1.head.value shouldBe ((1, "one"))
          subMap1.last.value shouldBe ((2, "two"))
          subMap1.childrenKeys.last.value shouldBe 3
          subMap1.map(keyValue => (keyValue._1, keyValue._2)).materialize.toList shouldBe List((1, "one"), (2, "two"))
          subMap1.childrenKeys.materialize.toList shouldBe List(3)
          subMap1.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue } shouldBe List((1, "one"), (2, "two"))
          subMap1.childrenKeys.foldLeft(List.empty[Int]) { case (previous, keyValue) => previous :+ keyValue } shouldBe List(3)
          subMap1.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous } shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.map(keyValue => keyValue).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.take(100).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.take(3).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.reverse.take(1).materialize.toList should contain only ((2, "two"))
          subMap1.take(100).materialize.toList should contain only((1, "one"), (2, "two"))
          subMap1.take(2).materialize.toList should contain only((1, "one"), (2, "two"))
          subMap1.take(1).materialize.toList should contain only ((1, "one"))
          subMap1.reverse.drop(1).materialize.toList should contain only ((1, "one"))
          subMap1.childrenKeys.drop(1).materialize.toList shouldBe empty
          subMap1.drop(1).materialize.toList should contain only ((2, "two"))
          subMap1.children.drop(1).materialize.toList shouldBe empty
          subMap1.reverse.drop(0).materialize.toList shouldBe List((2, "two"), (1, "one"))
          subMap1.childrenKeys.drop(0).materialize.toList shouldBe List(3)
          subMap1.drop(0).materialize.toList shouldBe List((1, "one"), (2, "two"))

          //KEYS ONLY ITERATIONS - TODO - Key iterations are currently not supported for MultiMap.
          //      subMap1.keys.size shouldBe 2
          //      subMap1.keys.headOption.value shouldBe 1
          //      subMap1.keys.lastOption.value shouldBe 2
          //      //      subMap1.maps.keys.lastOption.runIO shouldBe 3
          //      //      subMap1.maps.keys.toSeq shouldBe List(3)
          //
          //SECOND MAP ITERATIONS
          subMap2.count shouldBe 2
          subMap2.head.value shouldBe ((3, "three"))
          subMap2.last.value shouldBe ((4, "four"))
          subMap2.map(keyValue => (keyValue._1, keyValue._2)).materialize.toList shouldBe List((3, "three"), (4, "four"))
          subMap2.foldLeft(List.empty[(Int, String)]) { case (previous, keyValue) => previous :+ keyValue } shouldBe List((3, "three"), (4, "four"))
          subMap2.reverse.foldLeft(List.empty[(Int, String)]) { case (keyValue, previous) => keyValue :+ previous } shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.map(keyValue => keyValue).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.take(100).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.take(2).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.reverse.take(1).materialize.toList should contain only ((4, "four"))
          subMap2.take(100).materialize.toList should contain only((3, "three"), (4, "four"))
          subMap2.take(2).materialize.toList should contain only((3, "three"), (4, "four"))
          subMap2.take(1).materialize.toList should contain only ((3, "three"))
          subMap2.reverse.drop(1).materialize.toList should contain only ((3, "three"))
          subMap2.drop(1).materialize.toList should contain only ((4, "four"))
          subMap2.reverse.drop(0).materialize.toList shouldBe List((4, "four"), (3, "three"))
          subMap2.drop(0).materialize.toList shouldBe List((3, "three"), (4, "four"))
      }
    }
  }
}
