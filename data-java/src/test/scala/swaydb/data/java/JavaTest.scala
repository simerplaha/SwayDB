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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.java

import java.util.Optional
import java.util.function.{Consumer, Supplier}

import org.scalatest.OptionValues._
import org.scalatest.matchers.should.Matchers._
import swaydb.data.java.JavaEventually._
import swaydb.data.util.Java._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Random

object JavaTest {

  def randomString(size: Int = 10) = Random.alphanumeric.take(size max 1).mkString

  def shouldBeDefined[T](option: Optional[T]): Unit =
    option.isPresent shouldBe true

  def shouldBeEmpty[T](option: Optional[T]): Unit =
    option shouldBe Optional.empty[T]()

  def shouldBeEmpty[T](stream: swaydb.java.Stream[T]): Unit =
    stream.count shouldBe 0

  def shouldBeEmpty[T](items: java.lang.Iterable[T]): Unit =
    items.iterator().hasNext shouldBe false

  def shouldBeEmpty[T](items: java.util.Iterator[T]): Unit =
    items.hasNext shouldBe false

  def shouldBeEmptyEventually[T](timeout: Int, option: Supplier[Optional[T]]): Unit =
    eventually(
      timeout.seconds,
      new Test {
        override def assert(): Unit =
          option.get().isPresent shouldBe false
      }
    )

  def shouldContain[T](actual: Optional[T], expected: T): Unit =
    actual.asScala.value shouldBe expected

  def shouldBe[T](actual: T, expected: T): Unit =
    actual shouldBe expected

  def shouldBeSameIterators[T](actual: java.util.Iterator[T], expected: java.util.Iterator[T]): Unit = {
    val left = ListBuffer.empty[T]
    val right = ListBuffer.empty[T]

    actual.forEachRemaining(int => left += int)
    expected.forEachRemaining(int => right += int)

    left shouldBe right
  }

  def shouldBeGreaterThan(actual: Int, expected: Int): Unit =
    actual should be > expected

  def shouldBeGreaterThanEqualTo(actual: Int, expected: Int): Unit =
    actual should be >= expected

  def shouldBeLessThan(actual: Int, expected: Int): Unit =
    actual should be < expected

  def shouldBeLessThanEqualTo(actual: Int, expected: Int): Unit =
    actual should be <= expected

  def shouldHaveSize[T](actual: java.lang.Iterable[T], expected: Int): Unit =
    actual.asScala should have size expected

  def shouldHaveSize[T](actual: swaydb.java.Stream[T], expected: Int): Unit =
    actual.count shouldBe expected

  def shouldHaveSize[K, V](actual: java.util.Map[K, V], expected: Int): Unit =
    actual should have size expected

  def shouldContainSameInOrder[T](actual: java.lang.Iterable[T], expected: java.lang.Iterable[T]): Unit =
    actual.asScala.toList should contain theSameElementsInOrderAs expected.asScala

  def shouldContainTheSameElementsAs[T](actual: java.lang.Iterable[T], expected: java.lang.Iterable[T]): Unit =
    actual.asScala should contain theSameElementsAs expected.asScala

  def shouldInclude(string: String, contain: String): Unit =
    string should include(contain)

  def shouldStartWith(string: Optional[String], start: String): Unit =
    string.asScala.value should startWith(start)

  def shouldStartWith(string: String, start: String): Unit =
    string should startWith(start)

  def shouldBeEmptyString(string: Optional[String]): Unit =
    string.asScala.value shouldBe empty

  def shouldIncludeIgnoreCase(string: String, contain: String): Unit =
    string.toLowerCase should include(contain.toLowerCase)

  def shouldBe[T](actual: java.lang.Iterable[T], expected: java.lang.Iterable[T]): Unit =
    actual.asScala shouldBe expected.asScala

  def shouldBe[T](actual: swaydb.java.Stream[T], expected: java.lang.Iterable[T]): Unit =
    actual.materialize.asScala.toList shouldBe expected.asScala.toList

  def shouldBe[T](actual: swaydb.java.Stream[T], expected: swaydb.java.Stream[T]): Unit =
    actual.materialize shouldBe expected.materialize

  def shouldContainOnly[T](actual: swaydb.java.Stream[T], expected: T): Unit =
    actual.materialize should contain only expected

  def shouldBeFalse(actual: Boolean): Unit =
    actual shouldBe false

  def shouldBeTrue(actual: Boolean): Unit =
    actual shouldBe true

  def foreachRange(from: Int, to: Int, test: Consumer[Integer]): Unit =
    (from to to).foreach(int => test.accept(int))

  def eitherOne[T](left: Supplier[T], right: Supplier[T]): T =
    if (Random.nextBoolean())
      left.get()
    else
      right.get()

  def eitherOne[T](left: Supplier[T], mid: Supplier[T], right: Supplier[T]): T =
    if (Random.nextBoolean())
      left.get()
    else if (Random.nextBoolean())
      mid.get()
    else
      right.get()

  def eitherOne(left: Test, right: Test): Unit =
    if (Random.nextBoolean())
      left.assert()
    else
      right.assert()

  def eitherOne(left: Test, mid: Test, right: Test): Unit =
    if (Random.nextBoolean())
      left.assert()
    else if (Random.nextBoolean())
      mid.assert()
    else
      right.assert()
}
