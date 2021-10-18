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

package swaydb.core.util.series

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.util.series.appendable.{SeriesAppendable, SeriesAppendableVolatile}

import scala.reflect.ClassTag

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
