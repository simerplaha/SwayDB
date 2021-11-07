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
//package swaydb.core.util
//
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.util.Collections._
//import swaydb.data.slice.Slice
//
//import scala.collection.mutable.{ArrayBuffer, ListBuffer}
//
//class CollectionsSpec extends AnyWordSpec with Matchers {
//
//  "foreachBreak" should {
//    "exit on break" in {
//      val slice = Slice(1, 2, 3, 4)
//      var iterations = 0
//
//      slice foreachBreak {
//        item => {
//          iterations += 1
//          if (item == 3)
//            true
//          else
//            false
//        }
//      }
//
//      iterations shouldBe 3
//    }
//
//    "exit at the end of the iteration" in {
//      val slice = Slice(1, 2, 3, 4)
//      var iterations = 0
//
//      slice foreachBreak {
//        _ =>
//          iterations += 1
//          false
//      }
//
//      iterations shouldBe slice.size
//    }
//
//    "exit on empty" in {
//      val slice = Slice.empty[Int]
//      var iterations = 0
//
//      slice foreachBreak {
//        _ =>
//          iterations += 1
//          false
//      }
//
//      iterations shouldBe slice.size
//    }
//  }
//
//  "foldLeftWhile" should {
//    "fold while the condition is true" in {
//
//      (10 to 20).foldLeftWhile(List.empty[Int], _ < 15) {
//        case (fold, item) =>
//          fold :+ item
//      } shouldBe List(10, 11, 12, 13, 14)
//    }
//
//    "fold until the end of iteration" in {
//
//      (10 to 20).foldLeftWhile(List.empty[Int], _ => true) {
//        case (fold, item) =>
//          fold :+ item
//      } shouldBe (10 to 20).toList
//    }
//
//    "fold on empty" in {
//      List.empty[Int].foldLeftWhile(List.empty[Int], _ < 15) {
//        case (fold, item) =>
//          fold :+ item
//      } shouldBe List.empty
//    }
//
//    "return empty fold on fail condition" in {
//      (10 to 20).foldLeftWhile(List.empty[Int], _ => false) {
//        case (fold, item) =>
//          fold :+ item
//      } shouldBe List.empty
//    }
//  }
//
//  "Performance" when {
//    val count = 100000
//    //inserting into slice is at least 3 times faster then using ListBuffer.
//    //Using Slice during merge instead of ListBuffer would result.
//    "ListBuffer" in {
//      Benchmark("ListBuffer benchmark") {
//        val buff = ListBuffer.empty[Int]
//        (1 to count) foreach {
//          i =>
//            buff += i
//        }
//      }
//    }
//
//    "ArrayBuffer" in {
//      Benchmark("ArrayBuffer benchmark") {
//        val buff = new ArrayBuffer[Int](count)
//        (1 to count) foreach {
//          i =>
//            buff += i
//        }
//      }
//    }
//
//    "Slice" in {
//      Benchmark("Slice benchmark") {
//        val buff = Slice.of[Int](count)
//        (1 to count) foreach {
//          i =>
//            buff add i
//        }
//      }
//    }
//  }
//
//  "groupedMergeSingles" in {
//    Collections.groupedMergeSingles(0, List(1, 2, 3)) shouldBe List(List(1, 2, 3))
//    Collections.groupedMergeSingles(1, List(1, 2, 3)) shouldBe List(List(1), List(2), List(3))
//    Collections.groupedMergeSingles(2, List(1, 2, 3)) shouldBe List(List(1, 2, 3))
//    Collections.groupedMergeSingles(3, List(1, 2, 3)) shouldBe List(List(1, 2, 3))
//    Collections.groupedMergeSingles(4, List(1, 2, 3)) shouldBe List(List(1, 2, 3))
//
//    Collections.groupedMergeSingles(1, List(1, 2, 3, 4)) shouldBe List(List(1), List(2), List(3), List(4))
//    Collections.groupedMergeSingles(2, List(1, 2, 3, 4)) shouldBe List(List(1, 2), List(3, 4))
//  }
//
//  "groupedBySize" when {
//    sealed trait Item {
//      val size: Int = 1
//    }
//    object Item extends Item
//
//
//
//    "1 item" in {
//      val items = Slice(Item)
//
//      (0 to 10) foreach {
//        i =>
//          groupedBySize[Item](i, _.size, items) shouldBe Slice(items)
//      }
//    }
//
//    "2 items" in {
//      val items = Slice(Item, Item)
//
//      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item))
//      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item))
//      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item))
//      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item))
//    }
//
//    "3 items" in {
//      val items = Slice(Item, Item, Item)
//
//      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item), Slice(Item))
//      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item, Item))
//      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item, Item))
//      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item, Item))
//    }
//
//    "4 items" in {
//      val items = Slice(Item, Item, Item, Item)
//
//      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item), Slice(Item), Slice(Item))
//      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item), Slice(Item, Item))
//      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item))
//      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item))
//    }
//
//    "5 items" in {
//      val items = Slice(Item, Item, Item, Item, Item)
//
//      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item), Slice(Item), Slice(Item), Slice(Item))
//      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item), Slice(Item, Item, Item))
//      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item, Item))
//      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item, Item))
//    }
//  }
//}
