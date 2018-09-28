/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.util

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TryAssert
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._
import CollectionUtil._

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

class CollectionUtilSpec extends WordSpec with Matchers with TryAssert {

  "foreachBreak" should {
    "exit on break" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      slice.iterator foreachBreak {
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

      slice.iterator foreachBreak {
        _ =>
          iterations += 1
          false
      }

      iterations shouldBe slice.size
    }

    "exit on empty" in {
      val slice = Slice.empty[Int]
      var iterations = 0

      slice.iterator foreachBreak {
        _ =>
          iterations += 1
          false
      }

      iterations shouldBe slice.size
    }
  }

  "foldLeftWhile" should {
    "fold while the condition is true" in {

      (10 to 20).iterator.foldLeftWhile(List.empty[Int], _ < 15) {
        case (fold, item) =>
          fold :+ item
      } shouldBe List(10, 11, 12, 13, 14)

    }

    "fold until the end of iteration" in {

      (10 to 20).iterator.foldLeftWhile(List.empty[Int], _ => true) {
        case (fold, item) =>
          fold :+ item
      } shouldBe (10 to 20).toList

    }

    "fold on empty" in {
      List.empty[Int].iterator.foldLeftWhile(List.empty[Int], _ < 15) {
        case (fold, item) =>
          fold :+ item
      } shouldBe List.empty
    }

    "return empty fold on fail condition" in {
      (10 to 20).iterator.foldLeftWhile(List.empty[Int], _ => false) {
        case (fold, item) =>
          fold :+ item
      } shouldBe List.empty
    }
  }

}
