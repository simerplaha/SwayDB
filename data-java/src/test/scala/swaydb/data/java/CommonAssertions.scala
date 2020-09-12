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

package swaydb.data.java

import java.util.Optional
import java.util.function.{Consumer, Supplier}

import org.scalatest.OptionValues.convertOptionToValuable
import org.scalatest.matchers.should.Matchers._
import swaydb.data.java.JavaEventually._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

object CommonAssertions {

  def shouldBeEmpty[T](option: Optional[T]): Unit =
    option shouldBe Optional.empty[T]()

  def shouldBeEmpty[T](stream: swaydb.java.Stream[T]): Unit =
    stream.size shouldBe 0

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
    actual.toScala.value shouldBe expected

  def shouldBe[T](actual: T, expected: T): Unit =
    actual shouldBe expected

  def shouldBe[T](actual: java.util.Iterator[T], expected: java.util.Iterator[T]): Unit = {
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
    actual.size shouldBe expected

  def shouldHaveSize[K, V](actual: java.util.Map[K, V], expected: Int): Unit =
    actual should have size expected

  def shouldContainSameInOrder[T](actual: java.lang.Iterable[T], expected: java.lang.Iterable[T]): Unit =
    actual.asScala.toList should contain theSameElementsInOrderAs expected.asScala

  def shouldContainSame[T](actual: java.lang.Iterable[T], expected: java.lang.Iterable[T]): Unit =
    actual.asScala should contain theSameElementsAs expected.asScala

  def shouldBe[T](actual: java.lang.Iterable[T], expected: java.lang.Iterable[T]): Unit =
    actual.asScala shouldBe expected.asScala

  def shouldBe[T](actual: swaydb.java.Stream[T], expected: java.lang.Iterable[T]): Unit =
    actual.materialize.asScala.toList shouldBe expected.asScala.toList

  def shouldBe[T](actual: swaydb.java.Stream[T], expected: swaydb.java.Stream[T]): Unit =
    actual.materialize shouldBe expected.materialize

  def shouldBeFalse(actual: Boolean): Unit =
    actual shouldBe false

  def shouldBeTrue(actual: Boolean): Unit =
    actual shouldBe true

  def foreachRange(from: Int, to: Int, test: Consumer[Integer]): Unit =
    (from to to).foreach(int => test.accept(int))
}
