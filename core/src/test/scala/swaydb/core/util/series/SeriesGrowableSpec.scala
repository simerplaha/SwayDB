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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.util.series

import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.util.series.growable.SeriesGrowable

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


class Volatile_SeriesGrowableSpec extends SeriesGrowableSpec {
  def create[T >: Null : ClassTag](lengthPerSeries: Int): SeriesGrowable[T] =
    SeriesGrowable.volatile[T](lengthPerSeries)
}

sealed trait SeriesGrowableSpec extends AnyWordSpec with Matchers with PrivateMethodTester {

  def create[T >: Null : ClassTag](lengthPerSlice: Int): SeriesGrowable[T]

  def getState[T >: Null](series: SeriesGrowable[T]): SeriesGrowable.State[T] = {
    val function = PrivateMethod[SeriesGrowable.State[T]](Symbol("state"))
    series.invokePrivate(function())
  }

  "initial state" in {
    val series = create[Integer](1)
    val state = getState(series)
    state.startIndex shouldBe 0
    state.endIndex shouldBe -1

    state.length shouldBe 0

    series.isEmpty shouldBe true
  }

  "add" should {
    "insert item" when {
      "space available" in {
        val series = create[Integer](1)
        series.isEmpty shouldBe true

        series add 10

        val state = getState(series)
        state.startIndex shouldBe 0
        state.endIndex shouldBe 0
        state.length shouldBe 1

        series.depth shouldBe 1

        series.get(0) shouldBe 10
        assertThrows[ArrayIndexOutOfBoundsException](series.get(1))

        val state2 = getState(series)
        state2.startIndex shouldBe 0
        state2.endIndex shouldBe 0
        state2.length shouldBe 1
      }
    }

    "extend" when {
      "space is not available" in {
        val series = create[Integer](1)
        series.depth shouldBe 1
        series.isEmpty shouldBe true

        //add
        series.add(1)
        series.depth shouldBe 1
        series.isEmpty shouldBe false

        //check state
        var state = getState(series)
        state.startIndex shouldBe 0
        state.endIndex shouldBe 0
        state.series.length shouldBe 1

        //add
        series.add(2)
        series.depth shouldBe 2
        series.isEmpty shouldBe false

        //check state
        state = getState(series)
        state.startIndex shouldBe 0
        state.endIndex shouldBe 1
        state.series.length shouldBe 2

        //add
        series.add(3)
        series.depth shouldBe 3
        series.isEmpty shouldBe false

        //check state
        state = getState(series)
        state.startIndex shouldBe 0
        state.endIndex shouldBe 2
        state.series.length shouldBe 3

        series.get(state.startIndex) shouldBe 1
        series.get(state.endIndex) shouldBe 3

        series.get(0) shouldBe 1
        series.get(1) shouldBe 2
        series.get(2) shouldBe 3
        assertThrows[ArrayIndexOutOfBoundsException](series.get(4))

        series.length shouldBe 3
      }
    }
  }

  "lastOrNull and headOrNull" when {
    "empty" in {
      val series = create[Integer](2)
      series.headOrNull shouldBe null
      series.lastOrNull shouldBe null

      series.isEmpty shouldBe true
      series.length shouldBe 0
    }

    "nonEmpty" in {
      val series = create[Integer](2)
      series.isEmpty shouldBe true

      series.add(1)
      series.headOrNull shouldBe 1
      series.lastOrNull shouldBe 1
      series.length shouldBe 1

      series.add(2)
      series.headOrNull shouldBe 1
      series.lastOrNull shouldBe 2
      series.length shouldBe 2

      (3 to 10).foreach(series.add(_))
      series.headOrNull shouldBe 1
      series.lastOrNull shouldBe 10
    }
  }

  "findReverse" should {
    "throw ArrayIndexOutOfBoundsException" when {
      "empty" in {
        val series = create[Integer](0)
        assertThrows[ArrayIndexOutOfBoundsException](series.findReverse(0, 1)(_ => fail("should not have executed")))

        val series2 = create[Integer](2)
        assertThrows[ArrayIndexOutOfBoundsException](series2.findReverse(0, 1)(_ => fail("should not have executed")))
      }

      "nonEmpty" in {
        val series = create[Integer](1)
        series.add(1)
        assertThrows[ArrayIndexOutOfBoundsException](series.findReverse(2, 1)(_ => fail("should not have executed")))

        val series2 = create[Integer](2)
        series.add(1)
        series.add(2)
        assertThrows[ArrayIndexOutOfBoundsException](series2.findReverse(2, 1)(_ => fail("should not have executed")))
      }
    }

    "findReverse" when {
      "single item" in {
        val series = create[Integer](1)
        series.add(1)

        var iterations = 0
        val found = series.findReverse(0, Int.MinValue) {
          integer =>
            iterations += 1
            integer == 1
        }

        found shouldBe 1
        iterations shouldBe 1
      }

      "multiple items" should {
        val series = create[Integer](5)
        series.add(1)
        series.add(2)
        series.add(3)
        series.add(4)
        series.add(5)

        "start from index provided 1" in {
          val visits = ListBuffer.empty[Int]
          val found =
            series.findReverse(from = 4, nonResult = Int.MinValue) {
              integer =>
                visits += integer
                integer == 1
            }

          found shouldBe 1
          visits shouldBe List(5, 4, 3, 2, 1)
        }

        "start from index provided 2" in {
          val visits = ListBuffer.empty[Int]
          val found =
            series.findReverse(from = 3, nonResult = Int.MinValue) {
              integer =>
                visits += integer
                integer == 1
            }

          found shouldBe 1
          visits shouldBe List(4, 3, 2, 1)
        }

        "start from index provided 3" in {
          val visits = ListBuffer.empty[Int]
          val found =
            series.findReverse(from = 0, nonResult = Int.MinValue) {
              integer =>
                visits += integer
                integer == 1
            }

          found shouldBe 1
          visits shouldBe List(1)
        }

        "return none" when {
          "item not found" in {

            val visits = ListBuffer.empty[Int]
            val found =
              series.findReverse(4, Int.MinValue) {
                integer =>
                  visits += integer
                  integer == 100
              }

            found shouldBe Int.MinValue
            visits shouldBe List(5, 4, 3, 2, 1)
          }
        }
      }
    }

    "find" when {
      "single item" in {
        val series = create[Integer](1)
        series.add(1)

        var iterations = 0
        val found = series.find(0, Int.MinValue) {
          integer =>
            iterations += 1
            integer == 1
        }

        found shouldBe 1
        iterations shouldBe 1
      }

      "multiple items" should {
        val series = create[Integer](5)
        series.add(1)
        series.add(2)
        series.add(3)
        series.add(4)
        series.add(5)

        "start from index provided 1" in {
          val visits = ListBuffer.empty[Int]
          val found =
            series.find(from = 0, nonResult = Int.MinValue) {
              integer =>
                visits += integer
                integer == 5
            }

          found shouldBe 5
          visits shouldBe List(1, 2, 3, 4, 5)
        }

        "start from index provided 2" in {
          val visits = ListBuffer.empty[Int]
          val found =
            series.find(from = 1, nonResult = Int.MinValue) {
              integer =>
                visits += integer
                integer == 5
            }

          found shouldBe 5
          visits shouldBe List(2, 3, 4, 5)
        }

        "start from index provided 3" in {
          val visits = ListBuffer.empty[Int]
          val found =
            series.find(from = 4, nonResult = Int.MinValue) {
              integer =>
                visits += integer
                integer == 5
            }

          found shouldBe 5
          visits shouldBe List(5)
        }

        "return none" when {
          "item not found" in {

            val visits = ListBuffer.empty[Int]
            val found =
              series.find(0, Int.MinValue) {
                integer =>
                  visits += integer
                  integer == 100
              }

            found shouldBe Int.MinValue
            visits shouldBe List(1, 2, 3, 4, 5)
          }
        }
      }
    }
  }


  "removeHead" when {
    "empty" should {
      "not change state" in {
        val series = create[Integer](2)
        val initialState = getState(series)

        series.removeHead() shouldBe false
        series.removeHead() shouldBe false

        getState(series) shouldBe initialState
      }
    }

    "non-empty but emptied later" should {
      "not change state" in {
        val series = create[Integer](2)
        series add 1
        series.headOrNull shouldBe 1

        var state = getState(series)
        state.startIndex shouldBe 0
        state.endIndex shouldBe 0
        series.length shouldBe 1

        series.removeHead() shouldBe true
        series.headOrNull shouldBe null

        state = getState(series)
        state.startIndex shouldBe 1
        state.endIndex shouldBe 0

        series.length shouldBe 0

        series.removeHead() shouldBe false
        series.removeHead() shouldBe false
      }
    }

    "full" should {
      "adjust end add" in {
        //create series with already added 3 items - full
        val series = create[Integer](3)
        series add 1
        series add 2
        series add 3

        def state = getState(series)

        //assert initial state
        state.startIndex shouldBe 0
        state.endIndex shouldBe 2
        state.length shouldBe 3
        series.headOrNull shouldBe 1

        series.removeHead()
        state.startIndex shouldBe 1
        state.endIndex shouldBe 2
        state.length shouldBe 2
        series.headOrNull shouldBe 2

        series.removeHead()
        state.startIndex shouldBe 2
        state.endIndex shouldBe 2
        state.length shouldBe 1
        series.headOrNull shouldBe 3

        series.removeHead()
        state.startIndex shouldBe 3
        state.endIndex shouldBe 2
        state.length shouldBe 0
        series.headOrNull shouldBe null

        //the array is full
        state.series.isFull shouldBe true
        state.series.length shouldBe 1
        state.series.headOrNull.length shouldBe 3
        state.series.headOrNull.isFull shouldBe true

        //adding another will extend and also remove old array
        series.add(1)
        state.startIndex shouldBe 0
        state.endIndex shouldBe 0
        series.length shouldBe 1
        state.series.length shouldBe 1
      }
    }

    "extended twice without remove" should {
      "adjust end add" in {
        //create series with already added 3 items - full
        val series = create[Integer](3)
        //[1, 2, 3, 4]
        series add 1
        series add 2
        series add 3

        series add 4

        series.iterator.toList shouldBe List(1, 2, 3, 4)

        def state = getState(series)

        //assert initial state
        state.startIndex shouldBe 0
        state.endIndex shouldBe 3
        state.length shouldBe 4
        series.headOrNull shouldBe 1
        series.lastOrNull shouldBe 4

        //there are two arrays because the 4th added extended the array
        state.series.length shouldBe 2

        //remove the first three
        series.removeHead() shouldBe true
        state.length shouldBe 3
        series.removeHead() shouldBe true
        state.length shouldBe 2
        series.removeHead() shouldBe true
        state.length shouldBe 1

        series.iterator.toList shouldBe List(4)

        //state remains unchanged
        state.series.length shouldBe 2

        series add 5
        series add 6
        state.series.length shouldBe 2 //still unchanged

        series.iterator.toList shouldBe List(4, 5, 6)

        series add 7 //this will remove the head removed array

        series.iterator.toList shouldBe List(4, 5, 6, 7)

        state.series.length shouldBe 2 //length decreases to 1
        //first removed array gets dropped
        state.series.get(0).get(0) shouldBe 4

        state.startIndex shouldBe 0
        state.endIndex shouldBe 3

        series add 8
        series.iterator.toList shouldBe List(4, 5, 6, 7, 8)

        state.startIndex shouldBe 0
        state.endIndex shouldBe 4

        //remove all
        series.headOrNull shouldBe 4
        series.removeHead() shouldBe true
        series.headOrNull shouldBe 5
        series.removeHead() shouldBe true
        series.headOrNull shouldBe 6
        series.removeHead() shouldBe true
        series.headOrNull shouldBe 7
        series.removeHead() shouldBe true
        series.headOrNull shouldBe 8
        series.removeHead() shouldBe true
        series.headOrNull shouldBe null

        series add 9
        series add 10

        state.series.length shouldBe 2
      }
    }
  }
}
