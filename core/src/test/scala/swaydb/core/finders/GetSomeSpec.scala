package swaydb.core.finders

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpec}
import scala.util.Try
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TryAssert._
import swaydb.core.data.{KeyValue, SwayFunctionOutput, Value}
import swaydb.core.merge.{FixedMerger, FunctionMerger, PendingApplyMerger}
import swaydb.core.{TestData, TestTimeGenerator}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class GetSomeSpec extends WordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return Some" when {
    "put is not expired" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val keyValue = randomPutKeyValue(1, deadline = randomDeadlineOption(false))
        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(keyValue))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGet shouldBe keyValue
      }
    }

    "remove has time left" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val remove = randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))
        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val expect = put.copy(deadline = remove.deadline.orElse(put.deadline), time = remove.time)

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(remove))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(put))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGet shouldBe expect
      }
    }

    "update has time left" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val update = randomUpdateKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val expect = put.copy(deadline = update.deadline.orElse(put.deadline), value = update.value, time = update.time)

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(update))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(put))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGet shouldBe expect
      }
    }

    "functions either update or update expiry but do not remove" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val function =
          randomKeyValueFunctionKeyValue(
            key = 1,
            output =
              eitherOne(
                SwayFunctionOutput.Expire(randomDeadline(false)),
                SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(false))
              )
          )

        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val expect = FunctionMerger(function, put).assertGet

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(function))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(put))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGet shouldBe expect
      }
    }

    "pending applies that do not expire or remove" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val pendingApply =
          randomPendingApplyKeyValue(
            key = 1,
            deadline = Some(randomDeadline(false)),
            functionOutput =
              eitherOne(
                SwayFunctionOutput.Expire(randomDeadline(false)),
                SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(false)),
              )
          )

        val put =
          randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        val expected = PendingApplyMerger(pendingApply, put).assertGet

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(pendingApply))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(put))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGet shouldBe expected
      }
    }

    "range's fromValue is put" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val fromValue = Value.put(None, randomDeadlineOption(false))

        val range = randomRangeKeyValue(1, 10, Some(fromValue))

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(range))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGet shouldBe fromValue.toMemory(1)
      }
    }

    "range's fromValue or/and rangeValue is a function that does not removes or expires" in {
      runThis(30.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val functionValue =
          randomKeyValueFunctionKeyValue(
            key = 1,
            output =
              eitherOne(
                SwayFunctionOutput.Expire(randomDeadline(false)),
                SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(false)),
              )
          )

        val range = randomRangeKeyValue(1, 10, eitherOne(None, Some(functionValue.toRangeValue().assertGet)), functionValue.toRangeValue().assertGet)
        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        val expected = FixedMerger(functionValue, put).assertGet

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(range))
        //next level can return anything it will be removed.
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(put))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGet shouldBe expected
      }
    }
  }

}
