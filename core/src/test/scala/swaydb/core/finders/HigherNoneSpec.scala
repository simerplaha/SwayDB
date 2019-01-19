package swaydb.core.finders

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpec}
import scala.util.{Success, Try}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TryAssert._
import swaydb.core.data._
import swaydb.core.util.TryUtil
import swaydb.core.{TestData, TestTimeGenerator}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class HigherNoneSpec extends WordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return None" when {
    "current Level is returning SegmentResponse key-values and next Level returns None" in {
      runThis(100.times) {
        implicit val timeGenerator = TestTimeGenerator.random

        val higherFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val get = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]
        val higherInNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val maxNextHigherFetches = 100
        (0 to maxNextHigherFetches) foreach {
          i =>
            //if the max iteration is reached returning None so that higher fetches stop.
            val nextKeyValue: Try[Option[Memory.SegmentResponse]] =
              if (i == maxNextHigherFetches)
                TryUtil.successNone
              else
                Success(
                  Some(
                    eitherOne(
                      randomFixedKeyValue(i + 1, includePuts = false),
                      randomPutKeyValue(i + 1, deadline = Some(expiredDeadline())),
                    )
                  )
                ) //returning the next highest fixed key-value or a put key-value that is expire should always fetch the next highest.
            higherFromCurrentLevel expects (i: Slice[Byte]) returning nextKeyValue
        }

        //next Level fetch should only be called 1 time. Since it's returning none it should not be read again.
        higherInNextLevel expects (0: Slice[Byte]) returning TryUtil.successNone

        Higher(0, higherFromCurrentLevel, get, higherInNextLevel).assertGetOpt shouldBe empty
      }
    }

    "input key is smaller than this Level's higher Range's fromKey and next Level returns None. It should only call next Level once." in {
      runThis(100.times) {
        implicit val timeGenerator = TestTimeGenerator.random

        val higherFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val get = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]
        val higherInNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        //if the input key is smaller than this Level's higher Range's fromKey.
        //example:
        //0           (input key)
        //    10 - 20 (higher range)

        //    1 - 2   (higher range)
        higherFromCurrentLevel expects (0: Slice[Byte]) returning
          Success(Some(randomRangeKeyValue(from = 1, to = 2, Some(randomFromValueWithDeadline(deadline = expiredDeadline())))))

        //next Level fetch should only be called 1 time. Since it's returning none it should not be read again.
        higherInNextLevel expects (0: Slice[Byte]) returning TryUtil.successNone

        get expects (2: Slice[Byte]) returning
          Try(Some(randomPutKeyValue(2, deadline = Some(expiredDeadline()))))

        //        2 - 3   (higher range overlaps with previous range's toKey)
        higherFromCurrentLevel expects (2: Slice[Byte]) returning
          Success(Some(randomRangeKeyValue(from = 2, to = 3, Some(randomFromValueWithDeadline(deadline = expiredDeadline())))))

        get expects (3: Slice[Byte]) returning
          Try(Some(randomPutKeyValue(3, deadline = Some(expiredDeadline()))))

        //               4 - 5   (higher range does not overlap with previous range's toKey)
        higherFromCurrentLevel expects (3: Slice[Byte]) returning
          Success(Some(randomRangeKeyValue(from = 4, to = 5, Some(randomFromValueWithDeadline(deadline = expiredDeadline())))))

        //get returned None but higher is still read.
        get expects (5: Slice[Byte]) returning
          TryUtil.successNone

        //higher is still read.
        higherFromCurrentLevel expects (5: Slice[Byte]) returning
          TryUtil.successNone

        Higher(0, higherFromCurrentLevel, get, higherInNextLevel).assertGetOpt shouldBe empty
      }
    }
  }
}
