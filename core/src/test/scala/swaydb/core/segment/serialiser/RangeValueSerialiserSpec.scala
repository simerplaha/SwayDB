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

package swaydb.core.segment.serialiser

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CoreTestData._
import swaydb.core.TestTimer
import swaydb.core.segment.data.Value
import swaydb.core.segment.data.Value.{FromValue, FromValueOption, RangeValue}
import swaydb.slice.Slice
import swaydb.testkit.RunThis._

class RangeValueSerialiserSpec extends AnyWordSpec with Matchers {

  def doAssert[R <: RangeValue](rangeValue: R)(implicit serialiser: RangeValueSerialiser[Unit, R]) = {
    val bytesRequired = RangeValueSerialiser.bytesRequired((), rangeValue)
    //    rangeId shouldBe expectedId.id
    val bytes = Slice.of[Byte](bytesRequired)

    RangeValueSerialiser.write((), rangeValue)(bytes)
    bytes.isFull shouldBe true

    RangeValueSerialiser.read(bytes) shouldBe ((FromValue.Null, rangeValue))

    //also assert option Serialiser
    def doAssertOption(rangeValue: RangeValue) = {
      val bytesRequired = RangeValueSerialiser.bytesRequired(Value.FromValue.Null: FromValueOption, rangeValue)(RangeValueSerialiser.OptionRangeValueSerialiser)
      //    rangeId shouldBe expectedId.id
      val bytes = Slice.of[Byte](bytesRequired)

      RangeValueSerialiser.write(Value.FromValue.Null: FromValueOption, rangeValue)(bytes)(RangeValueSerialiser.OptionRangeValueSerialiser)
      bytes.isFull shouldBe true

      RangeValueSerialiser.read(bytes) shouldBe ((FromValue.Null, rangeValue))
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

  def doAssert[F <: FromValue, R <: RangeValue](fromValue: F, rangeValue: R)(implicit serialiser: RangeValueSerialiser[F, R]) = {
    val bytesRequired = RangeValueSerialiser.bytesRequired(fromValue, rangeValue)
    //    rangeId shouldBe expectedId.id
    val bytes = Slice.of[Byte](bytesRequired)
    RangeValueSerialiser.write(fromValue, rangeValue)(bytes)
    bytes.isFull shouldBe true

    RangeValueSerialiser.read(bytes) shouldBe ((fromValue, rangeValue))

    //also assert option Serialiser
    def doAssertOption(fromValue: FromValue, rangeValue: RangeValue) = {
      val bytesRequired = RangeValueSerialiser.bytesRequired(fromValue: FromValueOption, rangeValue)(RangeValueSerialiser.OptionRangeValueSerialiser)
      //    rangeId shouldBe expectedId.id
      val bytes = Slice.of[Byte](bytesRequired)
      RangeValueSerialiser.write(fromValue: FromValueOption, rangeValue)(bytes)(RangeValueSerialiser.OptionRangeValueSerialiser)
      bytes.isFull shouldBe true

      RangeValueSerialiser.read(bytes) shouldBe ((fromValue, rangeValue))
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
