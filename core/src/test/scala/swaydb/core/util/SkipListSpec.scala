/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.util

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.scalatest.{FlatSpec, Matchers}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class MinMaxSkipListSpec extends SkipListSpec {
  override def isMinMax: Boolean = true
}

sealed trait SkipListSpec extends FlatSpec with Matchers {

  def isMinMax: Boolean

  implicit val ordering = KeyOrder(Ordering.Int)


  it should "dsds" in {
    val skipList = new ConcurrentSkipListMap[Int, Int]()
    skipList.put(1, 1)
    skipList.put(2, 2)
    skipList.put(3, 3)

    val skipList2 = skipList.clone()

    def printlnOne() = {
      println("slipList1")
      skipList.forEach {
        case (key, value) =>
          println(key, value)
      }
    }

    def printlnTwo() = {
      println("slipList2")
      skipList2.forEach {
        case (key, value) =>
          println(key, value)
      }
    }

    printlnOne()
    printlnTwo()
    println("put")
    skipList2.put(3, 33)
    printlnOne()
    printlnTwo()
  }

  it should "put key-values and adjust existing minMax to the nearest keys" in {
    val skipList = SkipList.minMax[Int, Int]()

    skipList.put(1, 1)
    skipList.asScala should contain only ((1, 1))
    skipList.get(1) shouldBe Some(1)

    skipList.putIfAbsent(1, 111) shouldBe false
    skipList.asScala should contain only ((1, 1))
    skipList.get(1) shouldBe Some(1)

    skipList.put(1, 2)
    skipList.asScala should contain only ((1, 2))
    skipList.get(1) shouldBe Some(2)

    skipList.put(1, 1)
    skipList.asScala should contain only ((1, 1))
    skipList.get(1) shouldBe Some(1)

    skipList.put(2, 2)
    skipList.asScala should contain only((1, 1), (2, 2))
    skipList.get(2) shouldBe Some(2)
    skipList.get(1) shouldBe Some(1)

    skipList.put(3, 3)
    skipList.asScala should contain only((2, 2), (3, 3))
    skipList.putIfAbsent(3, 333) shouldBe false
    skipList.asScala should contain only((2, 2), (3, 3))

    skipList.put(1, 1)
    skipList.asScala should contain only((1, 1), (2, 2))
    skipList.get(1) shouldBe Some(1)
    skipList.get(2) shouldBe Some(2)

    skipList.put(5, 5)
    skipList.asScala should contain only((2, 2), (5, 5))
    skipList.get(1) shouldBe empty
    skipList.get(2) shouldBe Some(2)
    skipList.get(3) shouldBe empty
    skipList.get(5) shouldBe Some(5)

    skipList.put(3, 3)
    skipList.asScala should contain only((2, 2), (3, 3))
    skipList.get(1) shouldBe empty
    skipList.get(2) shouldBe Some(2)
    skipList.get(3) shouldBe Some(3)
    skipList.get(5) shouldBe empty

    skipList.put(3, 33)
    skipList.asScala should contain only((2, 2), (3, 33))
    skipList.get(1) shouldBe empty
    skipList.get(2) shouldBe Some(2)
    skipList.get(3) shouldBe Some(33)
    skipList.get(5) shouldBe empty

    skipList.put(2, 22)
    skipList.asScala should contain only((2, 22), (3, 33))
    skipList.get(1) shouldBe empty
    skipList.get(2) shouldBe Some(22)
    skipList.get(3) shouldBe Some(33)
    skipList.get(5) shouldBe empty
  }

  it should "search on single key-value" in {
    val skipList = SkipList.minMax[Int, Int]()

    def assertReads() = {
      skipList.asScala should contain only ((1, 1))
      skipList.get(1) shouldBe Some(1)
      //lower
      skipList.lower(0) shouldBe empty
      skipList.lower(1) shouldBe empty
      skipList.lower(2) shouldBe Some(1)
      //lowerKey
      skipList.lowerKey(0) shouldBe empty
      skipList.lowerKey(1) shouldBe empty
      skipList.lowerKey(2) shouldBe Some(1)
      //higher
      skipList.higher(2) shouldBe empty
      skipList.higher(1) shouldBe empty
      skipList.higher(0) shouldBe Some(1)
      //higher
      skipList.higherKey(2) shouldBe empty
      skipList.higherKey(1) shouldBe empty
      skipList.higherKey(0) shouldBe Some(1)
      //higher
      skipList.higherKeyValue(2) shouldBe empty
      skipList.higherKeyValue(1) shouldBe empty
      skipList.higherKeyValue(0) shouldBe Some(1, 1)
      //floor
      skipList.floor(0) shouldBe empty
      skipList.floor(1) shouldBe Some(1)
      skipList.floor(2) shouldBe Some(1)
      //ceiling
      skipList.ceiling(0) shouldBe Some(1)
      skipList.ceiling(1) shouldBe Some(1)
      skipList.ceiling(2) shouldBe empty
      //ceilingKey
      skipList.ceilingKey(0) shouldBe Some(1)
      skipList.ceilingKey(1) shouldBe Some(1)
      skipList.ceilingKey(2) shouldBe empty
      //foreach
      var keyValues = ListBuffer.empty[(Int, Int)]
      skipList foreach {
        case (key, value) =>
          keyValues += ((key, value))
      }
      keyValues should contain only ((1, 1))
      //subMap
      skipList.subMap(0, 1) shouldBe empty
      skipList.subMap(0, true, 1, true).asScala should contain only ((1, 1))
      skipList.subMap(0, false, 1, true).asScala should contain only ((1, 1))
      skipList.subMap(1, true, 2, false).asScala should contain only ((1, 1))
      skipList.subMap(1, 2).asScala should contain only ((1, 1))
      skipList.subMap(0, 2).asScala should contain only ((1, 1))
      //head & last
      skipList.head() shouldBe Some(1)
      skipList.headKey shouldBe Some(1)
      skipList.last() shouldBe Some(1)
      skipList.lastKey shouldBe Some(1)
      //values
      skipList.values().asScala should contain only 1
      //keys
      skipList.keys().asScala should contain only 1
      //contains
      skipList.contains(1) shouldBe true
      skipList.contains(2) shouldBe false
      //take
      skipList.take(1) should contain only 1
      skipList.take(2) should contain only 1
      skipList.take(100) should contain only 1
      skipList.take(0) shouldBe empty
      //foldLeft
      skipList.foldLeft(2) {
        case (previous, (key, value)) =>
          value shouldBe 1
          previous + key
      } shouldBe 3
      //size
      skipList.size shouldBe 1
      skipList.count() shouldBe 1
    }

    //put
    skipList.put(1, 1)
    assertReads()

    //putIfAbsent
    skipList.putIfAbsent(1, 111) shouldBe false
    assertReads()

    //remove
    skipList.remove(2)
    assertReads()

    //remove
    skipList.remove(1)
    skipList.head() shouldBe empty
    skipList.last() shouldBe empty
    skipList.get(1) shouldBe empty
    skipList.isEmpty shouldBe true
  }

  it should "search on multiple key-value" in {
    val skipList = SkipList.minMax[Int, Int]()

    def assertReads() = {
      skipList.asScala should contain only((1, 1), (5, 5))
      skipList.get(1) shouldBe Some(1)
      //lower
      skipList.lower(0) shouldBe empty
      skipList.lower(1) shouldBe empty
      skipList.lower(2) shouldBe Some(1)
      skipList.lower(3) shouldBe Some(1)
      skipList.lower(4) shouldBe Some(1)
      skipList.lower(5) shouldBe Some(1)
      skipList.lower(6) shouldBe Some(5)
      //lowerKey
      skipList.lowerKey(0) shouldBe empty
      skipList.lowerKey(1) shouldBe empty
      skipList.lowerKey(2) shouldBe Some(1)
      skipList.lowerKey(3) shouldBe Some(1)
      skipList.lowerKey(4) shouldBe Some(1)
      skipList.lowerKey(5) shouldBe Some(1)
      skipList.lowerKey(6) shouldBe Some(5)
      //higher
      skipList.higher(0) shouldBe Some(1)
      skipList.higher(1) shouldBe Some(5)
      skipList.higher(2) shouldBe Some(5)
      skipList.higher(3) shouldBe Some(5)
      skipList.higher(4) shouldBe Some(5)
      skipList.higher(5) shouldBe empty
      //higher
      skipList.higherKey(0) shouldBe Some(1)
      skipList.higherKey(1) shouldBe Some(5)
      skipList.higherKey(2) shouldBe Some(5)
      skipList.higherKey(3) shouldBe Some(5)
      skipList.higherKey(4) shouldBe Some(5)
      skipList.higherKey(5) shouldBe empty
      //higher
      skipList.higherKeyValue(0) shouldBe Some(1, 1)
      skipList.higherKeyValue(1) shouldBe Some(5, 5)
      skipList.higherKeyValue(2) shouldBe Some(5, 5)
      skipList.higherKeyValue(3) shouldBe Some(5, 5)
      skipList.higherKeyValue(4) shouldBe Some(5, 5)
      skipList.higherKeyValue(5) shouldBe empty
      //floor
      skipList.floor(0) shouldBe empty
      skipList.floor(1) shouldBe Some(1)
      skipList.floor(2) shouldBe Some(1)
      skipList.floor(3) shouldBe Some(1)
      skipList.floor(4) shouldBe Some(1)
      skipList.floor(5) shouldBe Some(5)
      skipList.floor(6) shouldBe Some(5)
      //ceiling
      skipList.ceiling(0) shouldBe Some(1)
      skipList.ceiling(1) shouldBe Some(1)
      skipList.ceiling(2) shouldBe Some(5)
      skipList.ceiling(3) shouldBe Some(5)
      skipList.ceiling(4) shouldBe Some(5)
      skipList.ceiling(5) shouldBe Some(5)
      skipList.ceiling(6) shouldBe empty
      //ceilingKey
      skipList.ceilingKey(0) shouldBe Some(1)
      skipList.ceilingKey(1) shouldBe Some(1)
      skipList.ceilingKey(2) shouldBe Some(5)
      skipList.ceilingKey(3) shouldBe Some(5)
      skipList.ceilingKey(4) shouldBe Some(5)
      skipList.ceilingKey(5) shouldBe Some(5)
      skipList.ceilingKey(6) shouldBe empty
      //subMap
      skipList.subMap(0, 1) shouldBe empty
      skipList.subMap(0, 2).asScala should contain only ((1, 1))
      skipList.subMap(0, 3).asScala should contain only ((1, 1))
      skipList.subMap(0, 4).asScala should contain only ((1, 1))
      skipList.subMap(0, 5).asScala should contain only ((1, 1))
      skipList.subMap(0, false, 5, false).asScala should contain only ((1, 1))
      skipList.subMap(0, false, 5, true).asScala should contain only((1, 1), (5, 5))
      skipList.subMap(0, true, 5, true).asScala should contain only((1, 1), (5, 5))
      skipList.subMap(1, false, 5, true).asScala should contain only ((5, 5))
      skipList.subMap(2, false, 5, true).asScala should contain only ((5, 5))
      //foreach
      var keyValues = ListBuffer.empty[(Int, Int)]
      skipList foreach {
        case (key, value) =>
          keyValues += ((key, value))
      }
      keyValues should contain only((1, 1), (5, 5))
      //head & last
      skipList.head() shouldBe Some(1)
      skipList.headKey shouldBe Some(1)
      skipList.last() shouldBe Some(5)
      skipList.lastKey shouldBe Some(5)
      //values
      skipList.values().asScala should contain only(1, 5)
      //keys
      skipList.keys().asScala should contain only(1, 5)
      //contains
      skipList.contains(1) shouldBe true
      skipList.contains(2) shouldBe false
      skipList.contains(5) shouldBe true
      //take
      skipList.take(1) should contain only 1
      skipList.take(2) should contain only(1, 5)
      skipList.take(100) should contain only(1, 5)
      skipList.take(0) shouldBe empty
      //foldLeft
      skipList.foldLeft(2) {
        case (previous, (key, value)) =>
          previous + key
      } shouldBe 8
      //size
      skipList.size shouldBe 2
      skipList.count() shouldBe 2
      //isEmpty
      skipList.isEmpty shouldBe false
    }

    //put
    skipList.put(1, 1)
    skipList.put(5, 5)
    assertReads()

    //putIfAbsent
    skipList.putIfAbsent(1, 111) shouldBe false
    assertReads()
    //putIfAbsent
    skipList.putIfAbsent(5, 555) shouldBe false
    assertReads()

    //remove
    skipList.remove(2)
    assertReads()
    skipList.remove(3)
    assertReads()
    skipList.remove(4)
    assertReads()

    //remove
    skipList.remove(1)
    skipList.head() shouldBe Some(5)
    skipList.last() shouldBe Some(5)
    skipList.get(1) shouldBe empty
    skipList.get(5) shouldBe Some(5)

    //remove
    skipList.remove(5)
    skipList.head() shouldBe empty
    skipList.last() shouldBe empty
    skipList.get(1) shouldBe empty
    skipList.get(5) shouldBe empty
    skipList.isEmpty shouldBe true
  }

//  it should "concurrent vs minMax" in {
//    implicit val ordering = KeyOrder[Int](Ordering.Int)
//
//        val skipList = SkipList.concurrent[Int, Int]()
////    val skipList = SkipList.minMax[Int, Int]()
//
//    Benchmark("") {
//      (1 to 10000000) foreach {
//        i =>
//          skipList.put(i, i)
//      }
//    }
//  }
}
