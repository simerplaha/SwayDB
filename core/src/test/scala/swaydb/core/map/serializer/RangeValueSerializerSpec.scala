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

package swaydb.core.map.serializer

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TryAssert
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data.{DataId, Value}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class RangeValueSerializerSpec extends WordSpec with Matchers with TryAssert {

  import RangeValueSerializers._

  //also assert option Serializer
  def doAssertOption(rangeValue: RangeValue, expectedId: DataId) = {
    val (bytesRequired, rangeId) = RangeValueSerializer.bytesRequiredAndRangeId(Option.empty[FromValue], rangeValue)(RangeValueSerializers.OptionRangeValueSerializer)
    rangeId shouldBe expectedId.id
    val bytes = Slice.create[Byte](bytesRequired)
    RangeValueSerializer.write(Option.empty[FromValue], rangeValue)(bytes)(RangeValueSerializers.OptionRangeValueSerializer)
    RangeValueSerializer.read(rangeId, bytes).assertGet shouldBe ((None, rangeValue))
  }

  def doAssert[R <: RangeValue](rangeValue: R, expectedId: DataId)(implicit serializer: RangeValueSerializer[Unit, R]) = {
    val (bytesRequired, rangeId) = RangeValueSerializer.bytesRequiredAndRangeId((), rangeValue)
    rangeId shouldBe expectedId.id
    val bytes = Slice.create[Byte](bytesRequired)
    RangeValueSerializer.write((), rangeValue)(bytes)
    RangeValueSerializer.read(rangeId, bytes).assertGet shouldBe ((Option.empty[FromValue], rangeValue))

    doAssertOption(rangeValue, expectedId)
  }


  "Serialize Remove range" should {

    "Remove None" in {
      doAssert(Value.Remove(None), DataId.RemoveRange)
    }

    "Remove Some" in {
      doAssert(Value.Remove(1.second.fromNow), DataId.RemoveRange)
    }
  }

  "Serialize Update range" should {

    "Update None None" in {
      doAssert(Value.Update(None, None), DataId.UpdateRange)
    }

    "Update None Some" in {
      doAssert(Value.Update(None, 1.second), DataId.UpdateRange)
    }

    "Update Some None" in {
      doAssert(Value.Update(1, None), DataId.UpdateRange)
    }

    "Update Some Some" in {
      doAssert(Value.Update(1, 1.hour.fromNow), DataId.UpdateRange)
    }
  }

  //also assert option Serializer
  def doAssertOption(fromValue: FromValue, rangeValue: RangeValue, expectedId: DataId) = {
    val (bytesRequired, rangeId) = RangeValueSerializer.bytesRequiredAndRangeId(Option(fromValue), rangeValue)(RangeValueSerializers.OptionRangeValueSerializer)
    rangeId shouldBe expectedId.id
    val bytes = Slice.create[Byte](bytesRequired)
    RangeValueSerializer.write(Option(fromValue), rangeValue)(bytes)(RangeValueSerializers.OptionRangeValueSerializer)
    RangeValueSerializer.read(rangeId, bytes).assertGet shouldBe ((Some(fromValue), rangeValue))
  }

  def doAssert[F <: FromValue, R <: RangeValue](fromValue: F, rangeValue: R, expectedId: DataId)(implicit serializer: RangeValueSerializer[F, R]) = {
    val (bytesRequired, rangeId) = RangeValueSerializer.bytesRequiredAndRangeId(fromValue, rangeValue)
    rangeId shouldBe expectedId.id
    val bytes = Slice.create[Byte](bytesRequired)
    RangeValueSerializer.write(fromValue, rangeValue)(bytes)
    RangeValueSerializer.read(rangeId, bytes).assertGet shouldBe ((Some(fromValue), rangeValue))

    doAssertOption(fromValue, rangeValue, expectedId)
  }

  "Serialize Remove Remove range" should {

    "Remove None Remove None" in {
      doAssert(Value.Remove(None), Value.Remove(None), DataId.RemoveRemoveRange)
    }

    "Remove None Remove Some" in {
      doAssert(Value.Remove(None), Value.Remove(10.seconds.fromNow), DataId.RemoveRemoveRange)
    }

    "Remove Some Remove None" in {
      doAssert(Value.Remove(10.seconds.fromNow), Value.Remove(None), DataId.RemoveRemoveRange)
    }

    "Remove Some Remove Some" in {
      doAssert(Value.Remove(10.seconds.fromNow), Value.Remove(5.seconds.fromNow), DataId.RemoveRemoveRange)
    }
  }

  "Serialize Remove Update range" should {

    //Remove None
    "Remove None Update None None" in {
      doAssert(Value.Remove(None), Value.Update(None, None), DataId.RemoveUpdateRange)
    }

    "Remove None Update None Some" in {
      doAssert(Value.Remove(None), Value.Update(None, 10.seconds), DataId.RemoveUpdateRange)
    }

    "Remove None Update Some None" in {
      doAssert(Value.Remove(None), Value.Update(1, None), DataId.RemoveUpdateRange)
    }

    "Remove None Update Some Some" in {
      doAssert(Value.Remove(None), Value.Update(1, 10.seconds), DataId.RemoveUpdateRange)
    }
    //Remove Some

    "Remove Some Update None None" in {
      doAssert(Value.Remove(1.second.fromNow), Value.Update(None, None), DataId.RemoveUpdateRange)
    }

    "Remove Some Update None Some" in {
      doAssert(Value.Remove(1.second.fromNow), Value.Update(None, 10.seconds), DataId.RemoveUpdateRange)
    }

    "Remove Some Update Some None" in {
      doAssert(Value.Remove(1.second.fromNow), Value.Update(1, None), DataId.RemoveUpdateRange)
    }

    "Remove Some Update Some Some" in {
      doAssert(Value.Remove(1.second.fromNow), Value.Update(1, 10.seconds), DataId.RemoveUpdateRange)
    }

  }

  "Serialize Put Remove range" should {

    //Put None None
    "Put None None Remove None" in {
      doAssert(Value.Put(None, None), Value.Remove(None), DataId.PutRemoveRange)
    }

    //Put None None
    "Put None None Remove Some" in {
      doAssert(Value.Put(None, None), Value.Remove(2.seconds.fromNow), DataId.PutRemoveRange)
    }

    //Put None Some
    "Put None Some Remove None" in {
      doAssert(Value.Put(None, 1.second), Value.Remove(None), DataId.PutRemoveRange)
    }

    //Put None Some
    "Put None Some Remove Some" in {
      doAssert(Value.Put(None, 1.second), Value.Remove(2.seconds.fromNow), DataId.PutRemoveRange)
    }

    //Put None None
    "Put Some None Remove None" in {
      doAssert(Value.Put(1), Value.Remove(None), DataId.PutRemoveRange)
    }

    //Put None None
    "Put Some None Remove Some" in {
      doAssert(Value.Put(1), Value.Remove(2.seconds.fromNow), DataId.PutRemoveRange)
    }

    //Put Some Some
    "Put Some Some Remove None" in {
      doAssert(Value.Put(1, 1.second), Value.Remove(None), DataId.PutRemoveRange)
    }

    //Put Some Some
    "Put Some Some Remove Some" in {
      doAssert(Value.Put(1, 1.second), Value.Remove(2.seconds.fromNow), DataId.PutRemoveRange)
    }

  }

  "Serialize Put Update range" should {

    //Put None None
    "Put None None Update None None" in {
      doAssert(Value.Put(None, None), Value.Update(None, None), DataId.PutUpdateRange)
    }

    "Put None None Update None Some" in {
      doAssert(Value.Put(None, None), Value.Update(None, 1.second), DataId.PutUpdateRange)
    }

    "Put None None Update Some None" in {
      doAssert(Value.Put(None, None), Value.Update(1), DataId.PutUpdateRange)
    }

    "Put None None Update Some Some" in {
      doAssert(Value.Put(None, None), Value.Update(1, 5.second.fromNow), DataId.PutUpdateRange)
    }

    //Put None Some
    "Put None Some Update None None" in {
      doAssert(Value.Put(None, 1.second), Value.Update(None, None), DataId.PutUpdateRange)
    }

    "Put None Some Update None Some" in {
      doAssert(Value.Put(None, 2.second), Value.Update(None, 1.second), DataId.PutUpdateRange)
    }

    "Put None Some Update Some None" in {
      doAssert(Value.Put(None, 3.second), Value.Update(1), DataId.PutUpdateRange)
    }

    "Put None Some Update Some Some" in {
      doAssert(Value.Put(None, 4.second), Value.Update(1, 5.second.fromNow), DataId.PutUpdateRange)
    }

    //Put Some None
    "Put Some None Update None None" in {
      doAssert(Value.Put(1), Value.Update(None, None), DataId.PutUpdateRange)
    }

    "Put Some None Update None Some" in {
      doAssert(Value.Put(2), Value.Update(None, 1.second), DataId.PutUpdateRange)
    }

    "Put Some None Update Some None" in {
      doAssert(Value.Put(3), Value.Update(1), DataId.PutUpdateRange)
    }

    "Put Some None Update Some Some" in {
      doAssert(Value.Put(4), Value.Update(1, 5.second.fromNow), DataId.PutUpdateRange)
    }

    //Put Some Some
    "Put Some Some Update None None" in {
      doAssert(Value.Put(1, 1.second), Value.Update(None, None), DataId.PutUpdateRange)
    }

    "Put Some Some Update None Some" in {
      doAssert(Value.Put(2, 2.seconds), Value.Update(None, 1.second), DataId.PutUpdateRange)
    }

    "Put Some Some Update Some None" in {
      doAssert(Value.Put(3, 3.seconds), Value.Update(1), DataId.PutUpdateRange)
    }

    "Put Some Some Update Some Some" in {
      doAssert(Value.Put(4, 4.seconds), Value.Update(1, 5.second.fromNow), DataId.PutUpdateRange)
    }

  }

  "Serialize Update Remove range" should {

    //Update None None
    "Update None None Remove None" in {
      doAssert(Value.Update(None, None), Value.Remove(None), DataId.UpdateRemoveRange)
    }

    //Update None None
    "Update None None Remove Some" in {
      doAssert(Value.Update(None, None), Value.Remove(2.seconds.fromNow), DataId.UpdateRemoveRange)
    }

    //Update None Some
    "Update None Some Remove None" in {
      doAssert(Value.Update(None, 1.second), Value.Remove(None), DataId.UpdateRemoveRange)
    }

    //Update None Some
    "Update None Some Remove Some" in {
      doAssert(Value.Update(None, 1.second), Value.Remove(2.seconds.fromNow), DataId.UpdateRemoveRange)
    }

    //Update None None
    "Update Some None Remove None" in {
      doAssert(Value.Update(1), Value.Remove(None), DataId.UpdateRemoveRange)
    }

    //Update None None
    "Update Some None Remove Some" in {
      doAssert(Value.Update(1), Value.Remove(2.seconds.fromNow), DataId.UpdateRemoveRange)
    }

    //Update Some Some
    "Update Some Some Remove None" in {
      doAssert(Value.Update(1, 1.second), Value.Remove(None), DataId.UpdateRemoveRange)
    }

    //Update Some Some
    "Update Some Some Remove Some" in {
      doAssert(Value.Update(1, 1.second), Value.Remove(2.seconds.fromNow), DataId.UpdateRemoveRange)
    }

  }

  "Serialize Update Update range" should {

    //Update None None
    "Update None None Update None None" in {
      doAssert(Value.Update(None, None), Value.Update(None, None), DataId.UpdateUpdateRange)
    }

    "Update None None Update None Some" in {
      doAssert(Value.Update(None, None), Value.Update(None, 1.second), DataId.UpdateUpdateRange)
    }

    "Update None None Update Some None" in {
      doAssert(Value.Update(None, None), Value.Update(1), DataId.UpdateUpdateRange)
    }

    "Update None None Update Some Some" in {
      doAssert(Value.Update(None, None), Value.Update(1, 5.second.fromNow), DataId.UpdateUpdateRange)
    }

    //Update None Some
    "Update None Some Update None None" in {
      doAssert(Value.Update(None, 1.second), Value.Update(None, None), DataId.UpdateUpdateRange)
    }

    "Update None Some Update None Some" in {
      doAssert(Value.Update(None, 2.second), Value.Update(None, 1.second), DataId.UpdateUpdateRange)
    }

    "Update None Some Update Some None" in {
      doAssert(Value.Update(None, 3.second), Value.Update(1), DataId.UpdateUpdateRange)
    }

    "Update None Some Update Some Some" in {
      doAssert(Value.Update(None, 4.second), Value.Update(1, 5.second.fromNow), DataId.UpdateUpdateRange)
    }

    //Update Some None
    "Update Some None Update None None" in {
      doAssert(Value.Update(1), Value.Update(None, None), DataId.UpdateUpdateRange)
    }

    "Update Some None Update None Some" in {
      doAssert(Value.Update(2), Value.Update(None, 1.second), DataId.UpdateUpdateRange)
    }

    "Update Some None Update Some None" in {
      doAssert(Value.Update(3), Value.Update(1), DataId.UpdateUpdateRange)
    }

    "Update Some None Update Some Some" in {
      doAssert(Value.Update(4), Value.Update(1, 5.second.fromNow), DataId.UpdateUpdateRange)
    }

    //Update Some Some
    "Update Some Some Update None None" in {
      doAssert(Value.Update(1, 1.second), Value.Update(None, None), DataId.UpdateUpdateRange)
    }

    "Update Some Some Update None Some" in {
      doAssert(Value.Update(2, 2.seconds), Value.Update(None, 1.second), DataId.UpdateUpdateRange)
    }

    "Update Some Some Update Some None" in {
      doAssert(Value.Update(3, 3.seconds), Value.Update(1), DataId.UpdateUpdateRange)
    }

    "Update Some Some Update Some Some" in {
      doAssert(Value.Update(4, 4.seconds), Value.Update(1, 5.second.fromNow), DataId.UpdateUpdateRange)
    }

  }

}
