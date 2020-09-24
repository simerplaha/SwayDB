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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.util.series.appendable.{SeriesAppendable, SeriesAppendableBasic, SeriesAppendableVolatile}

import scala.reflect.ClassTag

class Basic_SeriesAppendableSpec extends SeriesAppendableSpec {
  override def create[T >: Null : ClassTag](limit: Int): SeriesAppendable[T] =
    SeriesAppendableBasic[T](limit)
}

class Volatile_SeriesAppendableSpec extends SeriesAppendableSpec {
  override def create[T >: Null : ClassTag](limit: Int): SeriesAppendable[T] =
    SeriesAppendableVolatile[T](limit)
}

sealed trait SeriesAppendableSpec extends AnyWordSpec with Matchers {

  def create[T >: Null : ClassTag](limit: Int): SeriesAppendable[T]

  "throw ArrayIndexOutOfBoundsException" when {
    "empty" in {
      val series = create[Integer](0)
      assertThrows[ArrayIndexOutOfBoundsException](series.add(1))
      assertThrows[ArrayIndexOutOfBoundsException](series.get(0))
      series.length shouldBe 0
      series.innerArrayLength shouldBe 0
    }

    "nonEmpty" in {
      val series = create[Integer](1)
      series.add(1)
      series.length shouldBe 1
      series.get(0) shouldBe 1

      assertThrows[ArrayIndexOutOfBoundsException](series.add(2))
      assertThrows[ArrayIndexOutOfBoundsException](series.get(1))
      series.length shouldBe 1
      series.innerArrayLength shouldBe 1
    }
  }

  "lastOrNull & headOrNull" when {
    "empty" in {
      val series = create[Integer](0)
      series.lastOrNull shouldBe null
      series.headOrNull shouldBe null
    }

    "nonEmpty" in {
      val series = create[Integer](10)
      series.innerArrayLength shouldBe 10

      series.add(1)
      series.length shouldBe 1
      series.lastOrNull shouldBe 1
      series.headOrNull shouldBe 1

      series.add(2)
      series.length shouldBe 2
      series.lastOrNull shouldBe 2
      series.headOrNull shouldBe 1

      (3 to 10).foreach(series.add(_))
      series.lastOrNull shouldBe 10
      series.headOrNull shouldBe 1

      assertThrows[ArrayIndexOutOfBoundsException](series.add(11))
      series.isFull shouldBe true
    }
  }

  "iterator" in {
    val series = create[Integer](5)
    series.iterator shouldBe empty

    series.add(1)
    series.iterator.toList should contain only 1

    series.add(2)
    series.iterator.toList should contain only(1, 2)

    series.add(3)
    series.iterator.toList should contain only(1, 2, 3)

    series.add(4)
    series.iterator.toList should contain only(1, 2, 3, 4)

    series.add(5)
    series.iterator.toList should contain only(1, 2, 3, 4, 5)

    assertThrows[ArrayIndexOutOfBoundsException](series.add(6))

    series.headOrNull shouldBe 1
    series.lastOrNull shouldBe 5
    series.length shouldBe 5
  }


}
