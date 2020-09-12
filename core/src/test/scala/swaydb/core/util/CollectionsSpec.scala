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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.util.Collections._
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class CollectionsSpec extends AnyWordSpec with Matchers {

  "foreachBreak" should {
    "exit on break" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      slice foreachBreak {
        item => {
          iterations += 1
          if (item == 3)
            true
          else
            false
        }
      }

      iterations shouldBe 3
    }

    "exit at the end of the iteration" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      slice foreachBreak {
        _ =>
          iterations += 1
          false
      }

      iterations shouldBe slice.size
    }

    "exit on empty" in {
      val slice = Slice.empty[Int]
      var iterations = 0

      slice foreachBreak {
        _ =>
          iterations += 1
          false
      }

      iterations shouldBe slice.size
    }
  }

  "foldLeftWhile" should {
    "fold while the condition is true" in {

      (10 to 20).foldLeftWhile(List.empty[Int], _ < 15) {
        case (fold, item) =>
          fold :+ item
      } shouldBe List(10, 11, 12, 13, 14)
    }

    "fold until the end of iteration" in {

      (10 to 20).foldLeftWhile(List.empty[Int], _ => true) {
        case (fold, item) =>
          fold :+ item
      } shouldBe (10 to 20).toList
    }

    "fold on empty" in {
      List.empty[Int].foldLeftWhile(List.empty[Int], _ < 15) {
        case (fold, item) =>
          fold :+ item
      } shouldBe List.empty
    }

    "return empty fold on fail condition" in {
      (10 to 20).foldLeftWhile(List.empty[Int], _ => false) {
        case (fold, item) =>
          fold :+ item
      } shouldBe List.empty
    }
  }

  "Performance" when {
    val count = 100000
    //inserting into slice is at least 3 times faster then using ListBuffer.
    //Using Slice during merge instead of ListBuffer would result.
    "ListBuffer" in {
      Benchmark("ListBuffer benchmark") {
        val buff = ListBuffer.empty[Int]
        (1 to count) foreach {
          i =>
            buff += i
        }
      }
    }

    "ArrayBuffer" in {
      Benchmark("ArrayBuffer benchmark") {
        val buff = new ArrayBuffer[Int](count)
        (1 to count) foreach {
          i =>
            buff += i
        }
      }
    }

    "Slice" in {
      Benchmark("Slice benchmark") {
        val buff = Slice.create[Int](count)
        (1 to count) foreach {
          i =>
            buff add i
        }
      }
    }
  }

  "groupedMergeSingles" in {
    Collections.groupedMergeSingles(0, List(1, 2, 3)) shouldBe List(List(1, 2, 3))
    Collections.groupedMergeSingles(1, List(1, 2, 3)) shouldBe List(List(1), List(2), List(3))
    Collections.groupedMergeSingles(2, List(1, 2, 3)) shouldBe List(List(1, 2, 3))
    Collections.groupedMergeSingles(3, List(1, 2, 3)) shouldBe List(List(1, 2, 3))
    Collections.groupedMergeSingles(4, List(1, 2, 3)) shouldBe List(List(1, 2, 3))

    Collections.groupedMergeSingles(1, List(1, 2, 3, 4)) shouldBe List(List(1), List(2), List(3), List(4))
    Collections.groupedMergeSingles(2, List(1, 2, 3, 4)) shouldBe List(List(1, 2), List(3, 4))
  }

  "groupedBySize" when {
    sealed trait Item {
      val size: Int = 1
    }
    object Item extends Item



    "1 item" in {
      val items = Slice(Item)

      (0 to 10) foreach {
        i =>
          groupedBySize[Item](i, _.size, items) shouldBe Slice(items)
      }
    }

    "2 items" in {
      val items = Slice(Item, Item)

      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item))
      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item))
      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item))
      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item))
    }

    "3 items" in {
      val items = Slice(Item, Item, Item)

      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item), Slice(Item))
      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item, Item))
      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item, Item))
      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item, Item))
    }

    "4 items" in {
      val items = Slice(Item, Item, Item, Item)

      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item), Slice(Item), Slice(Item))
      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item), Slice(Item, Item))
      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item))
      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item))
    }

    "5 items" in {
      val items = Slice(Item, Item, Item, Item, Item)

      groupedBySize[Item](1, _.size, items) shouldBe Slice(Slice(Item), Slice(Item), Slice(Item), Slice(Item), Slice(Item))
      groupedBySize[Item](2, _.size, items) shouldBe Slice(Slice(Item, Item), Slice(Item, Item, Item))
      groupedBySize[Item](3, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item, Item))
      groupedBySize[Item](4, _.size, items) shouldBe Slice(Slice(Item, Item, Item, Item, Item))
    }
  }
}
