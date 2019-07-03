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

package swaydb.core.map.serializer

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.Value
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.data.slice.Slice

class RangeValueSerializerSpec extends WordSpec with Matchers {

  def doAssert[R <: RangeValue](rangeValue: R)(implicit serializer: RangeValueSerializer[Unit, R]) = {
    val bytesRequired = RangeValueSerializer.bytesRequired((), rangeValue)
    //    rangeId shouldBe expectedId.id
    val bytes = Slice.create[Byte](bytesRequired)

    RangeValueSerializer.write((), rangeValue)(bytes)
    bytes.isFull shouldBe true

    RangeValueSerializer.read(bytes).assertGet shouldBe ((Option.empty[FromValue], rangeValue))

    //also assert option Serializer
    def doAssertOption(rangeValue: RangeValue) = {
      val bytesRequired = RangeValueSerializer.bytesRequired(Option.empty[FromValue], rangeValue)(RangeValueSerializer.OptionRangeValueSerializer)
      //    rangeId shouldBe expectedId.id
      val bytes = Slice.create[Byte](bytesRequired)

      RangeValueSerializer.write(Option.empty[FromValue], rangeValue)(bytes)(RangeValueSerializer.OptionRangeValueSerializer)
      bytes.isFull shouldBe true

      RangeValueSerializer.read(bytes).assertGet shouldBe ((None, rangeValue))
    }

    doAssertOption(rangeValue)
  }

  "Serialize range values" in {

    runThis(1000.times) {
      implicit val testTimer = TestTimer.random

      randomRangeValue() match {
        case rangeValue: Value.Remove =>
          doAssert(rangeValue)

        case rangeValue: Value.Update =>
          doAssert(rangeValue)

        case rangeValue: Value.Function =>
          doAssert(rangeValue)

        case rangeValue: Value.PendingApply =>
          doAssert(rangeValue)
      }
    }
  }

  def doAssert[F <: FromValue, R <: RangeValue](fromValue: F, rangeValue: R)(implicit serializer: RangeValueSerializer[F, R]) = {
    val bytesRequired = RangeValueSerializer.bytesRequired(fromValue, rangeValue)
    //    rangeId shouldBe expectedId.id
    val bytes = Slice.create[Byte](bytesRequired)
    RangeValueSerializer.write(fromValue, rangeValue)(bytes)
    bytes.isFull shouldBe true

    RangeValueSerializer.read(bytes).assertGet shouldBe ((Some(fromValue), rangeValue))

    //also assert option Serializer
    def doAssertOption(fromValue: FromValue, rangeValue: RangeValue) = {
      val bytesRequired = RangeValueSerializer.bytesRequired(Option(fromValue), rangeValue)(RangeValueSerializer.OptionRangeValueSerializer)
      //    rangeId shouldBe expectedId.id
      val bytes = Slice.create[Byte](bytesRequired)
      RangeValueSerializer.write(Option(fromValue), rangeValue)(bytes)(RangeValueSerializer.OptionRangeValueSerializer)
      bytes.isFull shouldBe true

      RangeValueSerializer.read(bytes).assertGet shouldBe ((Some(fromValue), rangeValue))
    }

    doAssertOption(fromValue, rangeValue)
  }

  "Serialize from values and range values" in {

    runThis(1000.times) {
      implicit val testTimer = TestTimer.random

      (randomFromValue(), randomRangeValue()) match {
        case (fromValue: Value.Remove, rangeValue: Value.Remove) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.Update) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.Function) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Remove, rangeValue: Value.PendingApply) => doAssert(fromValue, rangeValue)

        case (fromValue: Value.Put, rangeValue: Value.Remove) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.Update) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.Function) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Put, rangeValue: Value.PendingApply) => doAssert(fromValue, rangeValue)

        case (fromValue: Value.Update, rangeValue: Value.Remove) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.Update) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.Function) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Update, rangeValue: Value.PendingApply) => doAssert(fromValue, rangeValue)

        case (fromValue: Value.Function, rangeValue: Value.Remove) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.Update) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.Function) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.Function, rangeValue: Value.PendingApply) => doAssert(fromValue, rangeValue)

        case (fromValue: Value.PendingApply, rangeValue: Value.Remove) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.Update) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.Function) => doAssert(fromValue, rangeValue)
        case (fromValue: Value.PendingApply, rangeValue: Value.PendingApply) => doAssert(fromValue, rangeValue)
      }
    }
  }
}
