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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.multimap

import org.scalatest.OptionValues._
import swaydb.Bag.Less
import swaydb.api.TestBaseEmbedded
import swaydb.core.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.{Bag, IO, MultiMap, Prepare}
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._

import scala.concurrent.duration._
import scala.util.Random

class MultiMapSpec0 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Bag.Less](dir = randomDir)
}

class MultiMapSpec1 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, Int, String, Nothing, Bag.Less] =
    swaydb.persistent.MultiMap[Int, Int, String, Nothing, Bag.Less](dir = randomDir, mapSize = 1.byte)
}

class MultiMapSpec2 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap[Int, Int, String, Nothing, Bag.Less]()
}

class MultiMapSpec3 extends MultiMapSpec {
  val keyValueCount: Int = 1000

  override def newDB(): MultiMap[Int, Int, String, Nothing, Bag.Less] =
    swaydb.memory.MultiMap[Int, Int, String, Nothing, Bag.Less](mapSize = 1.byte)
}

sealed trait MultiMapSpec extends TestBaseEmbedded {

  val keyValueCount: Int

  def newDB(): MultiMap[Int, Int, String, Nothing, Bag.Less]

  implicit val bag = Bag.less

  //  implicit val mapKeySerializer = Key.serializer(IntSerializer)
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  /**
   * The following displays the MultiMap hierarchy existing for the Map.
   */
  //root                          - (0, "zero")
  //  |______ child1              - (1, "one"), (2, "two")
  //  |          |_______ child11 - (3, "three"), (4, "four")
  //  |          |_______ child12 - (5, "five"), (6, "six")
  //  |
  //  |______ child2              - (7, "seven"), (8, "eight")
  //  |          |_______ child21 - (9, "nine"), (10, "ten")
  //  |          |_______ child22 - (11, "eleven"), (12, "twelve")

  def buildRootMap(): MultiMap[Int, Int, String, Nothing, Bag.Less] = {
    val root = newDB()
    root.put(0, "zero")

    /**
     * Child1 hierarchy
     */
    val child1 = root.schema.init(1)
    child1.put(1, "one")
    child1.put(2, "two")

    val child11 = child1.schema.init(11)
    child11.put(3, "three")
    child11.put(4, "four")

    val child12 = child1.schema.init(12)
    child12.put(5, "five")
    child12.put(6, "six")

    /**
     * Child2 hierarchy
     */
    val child2 = root.schema.init(2)
    child2.put(7, "seven")
    child2.put(8, "eight")

    val child21 = child2.schema.init(21)
    child21.put(9, "nine")
    child21.put(10, "ten")

    val child22 = child2.schema.init(22)
    child22.put(11, "eleven")
    child22.put(12, "twelve")

    root.get(0).value shouldBe "zero"
    root.schema.keys.materialize.toList shouldBe List(1, 2)

    child1.get(1).value shouldBe "one"
    child1.get(2).value shouldBe "two"
    child1.schema.keys.materialize.toList shouldBe List(11, 12)
    child11.get(3).value shouldBe "three"
    child11.get(4).value shouldBe "four"
    child12.get(5).value shouldBe "five"
    child12.get(6).value shouldBe "six"

    root
  }

  "put" should {
    "basic" in {
      val root = buildRootMap()
      root.put(0, "zero1")
      root.get(0).value shouldBe "zero1"

      val child1 = root.schema.get(1).value
      val child11 = child1.schema.get(11).value
      val child12 = child1.schema.get(12).value
      //update the last child's key-value only and everything else should remain the same
      child12.put(6, "updated")
      child12.get(5).value shouldBe "five"
      child12.get(6).value shouldBe "updated"
      child1.get(1).value shouldBe "one"
      child1.get(2).value shouldBe "two"
      child11.get(3).value shouldBe "three"
      child11.get(4).value shouldBe "four"

      val child2 = root.schema.get(2).value
      val child21 = child2.schema.get(21).value
      val child22 = child2.schema.get(22).value
      //update the last child's key-value only and everything else should remain the same
      child22.put(12, "updated")
      child22.get(11).value shouldBe "eleven"
      child22.get(12).value shouldBe "updated"
      child2.get(7).value shouldBe "seven"
      child2.get(8).value shouldBe "eight"
      child21.get(9).value shouldBe "nine"
      child21.get(10).value shouldBe "ten"

      root.close()
    }

    "stream" in {
      val root = newDB()

      val stream = swaydb.Stream.range(1, 10).map(int => (int, int.toString))
      root.put(stream)

      root.stream.materialize.toList shouldBe (1 to 10).map(int => (int, int.toString)).toList

      root.close()
    }

    "put a stream to an expired map and also submit key-values with deadline later than map's deadline" in {
      val root = newDB()
      val deadline = 2.seconds.fromNow

      //expired map
      val child = root.schema.init(1, deadline)

      //inserting a stream into child which has deadline set will also set deadline values for these key-values
      val stream = swaydb.Stream.range(1, 10).map(int => (int, int.toString))
      child.put(stream)

      (1 to 10) foreach {
        i =>
          Seq(
            //random overwrite the key-value with another deadline which should be ignored
            //because the map expires earlier.
            () => if (Random.nextBoolean()) child.put(i, i.toString, 1.hour),
            //value is set
            () => child.get(i).value shouldBe i.toString,
            //child has values set
            () => child.expiration(i).value shouldBe deadline
          ).runThisRandomly
      }

      eventual(deadline.timeLeft) {
        child.isEmpty shouldBe true
        child.schema.isEmpty shouldBe true
      }

      root.close()
    }

    "put a stream to an expired map and also submit key-values with deadline earlier than map's deadline" in {
      val root = newDB()
      val mapDeadline = 1.hour.fromNow

      //expired map
      val child = root.schema.init(1, mapDeadline)

      //inserting a stream into child which has deadline set will also set deadline values for these key-values
      val stream = swaydb.Stream.range(1, 10).map(int => (int, int.toString))
      child.put(stream)

      val keyValueDeadline = 2.seconds.fromNow

      (1 to 10) foreach {
        i =>
          //write all key-values with an earlier deadline.
          child.put(i, i.toString, keyValueDeadline)

          Seq(
            //value is set
            () => child.get(i).value shouldBe i.toString,
            //child has values set
            () => child.expiration(i).value shouldBe keyValueDeadline
          ).runThisRandomly
      }

      eventual(keyValueDeadline.timeLeft) {
        child.isEmpty shouldBe true
        child.schema.isEmpty shouldBe true
      }
      //root has only 1 child
      root.schema.keys.materialize.toList should contain only 1

      root.close()
    }

    "put & expire" in {
      val root = newDB()
      root.put(1, "one", 2.second)
      root.get(1).value shouldBe "one"

      //childMap with default expiration set expiration
      val child1 = root.schema.init(2, 2.second)
      child1.put(1, "one") //inserting key-value without expiration uses default expiration.
      child1.get(1).value shouldBe "one"
      child1.expiration(1) shouldBe defined

      child1.put((1, "one"), (2, "two"), (3, "three"))

      eventual(3.seconds) {
        root.get(1) shouldBe empty
      }

      //child map is expired
      eventual(2.seconds) {
        root.schema.keys.materialize.toList shouldBe empty
      }

      child1.isEmpty shouldBe true

      root.close()
    }
  }

  "remove" should {
    "remove key-values and map" in {
      //hierarchy - root --> child1 --> child2 --> child3
      //                                       --> child4
      val root = newDB()
      root.put(1, "one")
      root.put(2, "two")

      val child1 = root.schema.init(1)
      child1.put(1, "one")
      child1.put(2, "two")

      val child2 = child1.schema.init(2)
      child2.put(1, "one")
      child2.put(2, "two")

      val child3 = child2.schema.init(3)
      child2.put(1, "one")
      child2.put(2, "two")

      val child4 = child2.schema.init(4)
      child2.put(1, "one")
      child2.put(2, "two")

      //remove key-values should last child
      eitherOne(child2.remove(Seq(1, 2)), child2.remove(from = 1, to = 2))
      child2.stream.materialize.toList shouldBe empty

      //check 2nd child has it's key-values
      child1.stream.materialize.toList.map(_._1) shouldBe List(1, 2)
      //all maps exists.
      root.schema.keys.materialize.toList shouldBe List(1)
      child1.schema.keys.materialize.toList shouldBe List(2)
      child2.schema.keys.materialize.toList shouldBe List(3, 4)

      //remove child1
      root.schema.remove(1)
      root.schema.keys.materialize.toList shouldBe empty
      child1.stream.materialize.toList shouldBe empty
      child2.stream.materialize.toList shouldBe empty

      root.close()
    }

    "remove sibling map" in {
      //hierarchy - root --> child1 --> child2 --> child3
      //                                       --> child4 (remove this)
      val root = newDB()
      root.put(1, "one")
      root.put(2, "two")

      val child1 = root.schema.init(1)
      child1.put(1, "one")
      child1.put(2, "two")

      val child2 = child1.schema.init(2)
      child2.put(1, "one")
      child2.put(2, "two")

      val child3 = child2.schema.init(3)
      child2.put(1, "one")
      child2.put(2, "two")

      val child4 = child2.schema.init(4)
      child2.put(1, "one")
      child2.put(2, "two")

      child2.schema.remove(4)

      //all maps exists.
      root.schema.keys.materialize.toList shouldBe List(1)
      child1.schema.keys.materialize.toList shouldBe List(2)
      child2.schema.keys.materialize.toList shouldBe List(3)

      root.close()
    }
  }

  "expire child map" in {
    //hierarchy - root --> child1 --> child2
    val root = newDB()
    root.put(1, "one")
    root.put(2, "two")

    val child1 = root.schema.init(1)
    child1.put(1, "one")
    child1.put(2, "two")

    //create last child with expiration
    val child11 = child1.schema.init(11, 3.seconds)
    child11.put(1, "one")
    child11.put(2, "two", 1.hour)
    //expiration should be set to 3.seconds and not 1.hour since the map expires is 3.seconds.
    child11.expiration(2).value.timeLeft.minus(4.seconds).fromNow.hasTimeLeft() shouldBe false

    //last child is not empty
    child11.stream.materialize.toList should not be empty

    //last child is empty
    eventual(4.seconds) {
      child11.stream.materialize.toList shouldBe empty
    }

    //parent child of last child is not empty
    child1.stream.materialize.toList should not be empty
    //parent child of last child has no child maps.
    child1.schema.keys.materialize.toList shouldBe empty

    root.close()
  }

  "update & clearKeyValues" in {
    //hierarchy - root --> child1 --> child2
    val root = newDB()
    root.put(1, "one")
    root.put(2, "two")

    val child1 = root.schema.init(1)
    child1.put(1, "one")
    child1.put(2, "two")

    val child11 = child1.schema.init(11)
    child11.put(1, "one")
    child11.put(2, "two")

    //update just the last child.
    child1.update((1, "updated"), (2, "updated"))
    child1.stream.materialize.toList shouldBe List((1, "updated"), (2, "updated"))

    //everything else remains the same.
    root.stream.materialize.toList shouldBe List((1, "one"), (2, "two"))
    child11.stream.materialize.toList shouldBe List((1, "one"), (2, "two"))

    root.clearKeyValues()
    root.isEmpty shouldBe true

    child11.stream.materialize.toList shouldBe List((1, "one"), (2, "two"))

    root.close()
  }

  "children" should {
    "replace" in {
      //hierarchy - root --> child1 --> child2 --> child3
      //                                       --> child4
      val root = newDB()
      root.put(1, "one")
      root.put(2, "two")

      val child1 = root.schema.init(1)
      child1.put(1, "one")
      child1.put(2, "two")

      var child2 = child1.schema.init(2)
      child2.put(1, "one")
      child2.put(2, "two")

      val child3 = child2.schema.init(3)
      child3.put(1, "one")
      child3.put(2, "two")

      val child4 = child2.schema.init(4)
      child4.put(1, "one")
      child4.put(2, "two")

      child1.get(1).value shouldBe "one"
      child1.get(2).value shouldBe "two"

      //replace last with expiration
      child2 = child1.schema.replace(2, 3.second)
      //child1 now only have one child 2 removing 3 and 4
      child1.schema.keys.materialize.toList should contain only 2

      child2.isEmpty shouldBe true //last has no key-values
      child2.put(1, "one") //add a key-value
      child2.isEmpty shouldBe false //has a key-value now

      child3.isEmpty shouldBe true
      child4.isEmpty shouldBe true

      eventual(4.seconds)(child2.isEmpty shouldBe true) //which eventually expires
      child1.schema.keys.materialize.toList shouldBe empty

      root.close()
    }

    "replace sibling" in {
      //root --> child1 (replace this)
      //     --> child2
      val root = newDB()
      root.put(1, "one")
      root.put(2, "two")

      val child1 = root.schema.init(1)
      child1.put(1, "one")
      child1.put(2, "two")
      generateRandomNestedMaps(child1.toBag[IO.ApiIO])

      val child2 = root.schema.init(2)
      child2.put(1, "one")
      child2.put(2, "two")

      root.schema.replace(1)

      //child1 is replaced and is removed
      child1.isEmpty shouldBe true

      //child2 is untouched.
      child2.isEmpty shouldBe false

      child1.schema.isEmpty shouldBe true
      child1.get(1) shouldBe empty
      child1.get(2) shouldBe empty

      root.close()
    }

    "replace removes all child maps" in {
      runThis(10.times) {
        val root = newDB()

        //randomly generate 100 child of random hierarchy.
        (1 to 100).foldLeft(root) {
          case (parent, i) =>
            val child = parent.schema.init(i)

            //randomly add data to the child
            if (Random.nextBoolean())
              (3 to Random.nextInt(10)) foreach {
                i =>
                  child.put(i, i.toString)
              }

            //randomly return the child or the root to create randomly hierarchies
            if (Random.nextBoolean())
              child
            else
              parent
        }
        //100 children in total are created
        root.schema.flatten should have size 100
        //root should have more than one child
        root.schema.keys.materialize.size should be >= 1

        //random set a deadline
        val deadline = randomDeadlineOption(false)
        //replace all children of root map.
        root.schema.keys.materialize.foreach(child => root.schema.replace(child, deadline))

        //root's children do not contain any children of their own.
        root.schema.stream.materialize.toList.flatten.foreach {
          child =>
            //has no children
            child.schema.isEmpty shouldBe true
            //has no entries
            child.isEmpty shouldBe true
            //deadline is set
            child.defaultExpiration shouldBe deadline
        }

        root.close()
      }
    }

    "init" when {
      "updated expiration" in {
        //hierarchy - root --> child1 --> child2
        val root = newDB()
        root.put(1, "one")
        root.put(2, "two")

        //initial child with expiration.
        val deadline = 1.hour.fromNow
        var child1 = root.schema.init(1, deadline)
        child1.put(1, "one")
        child1.put(2, "two")

        val child2 = child1.schema.init(2)
        child2.put(1, "one")
        child2.put(2, "two")

        //re-init child1 with the same expiration has no effect.
        child1 = root.schema.init(1, deadline)
        child1.get(1).value shouldBe "one"
        child1.get(2).value shouldBe "two"
        child2.get(1).value shouldBe "one"
        child2.get(2).value shouldBe "two"

        //re-init with updated expiration should expire everything.
        child1 = root.schema.init(1, 0.second)
        child1.schema.keys.materialize.toList shouldBe empty
        child1.isEmpty shouldBe true

        //re-init the same expired map with a new map does not set expiration.
        child1 = root.schema.init(1)
        child1.put(1, "one")
        child1.get(1).value shouldBe "one"
        child1.expiration(1) shouldBe empty

        root.close()
      }
    }
  }

  "transaction" in {
    //hierarchy - root --> child1 --> child2 --> child3
    val root = newDB()
    root.put(1, "one")
    root.put(2, "two")

    val child1 = root.schema.init(1)
    child1.put(1, "one")
    child1.put(2, "two")

    val child2 = child1.schema.init(2)
    child2.put(1, "one")
    child2.put(2, "two")

    val child3 = child1.schema.init(3)
    child3.put(1, "one")
    child3.put(2, "two")

    val transaction =
      child3.toTransaction(Prepare.Put(3, "three")) ++
        child2.toTransaction(Prepare.Put(4, "four"))

    child3.commit(transaction)

    child3.get(3).value shouldBe "three"
    child2.get(4).value shouldBe "four"

    root.close()
  }
}
