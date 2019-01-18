package swaydb.core.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.{CommonAssertions, TestTimeGenerator}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TryAssert._
import swaydb.data.slice.Slice

class FunctionMergerSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def compression = randomGroupingStrategyOption(randomNextInt(1000))

  "Merging Function into any key-value" when {
    "times are not in order" should {
      "always return old key-value" in {

        implicit val timeGenerator = TestTimeGenerator.Incremental()

        runThis(1000.times) {
          val key = randomBytesSlice()

          //new but has older time than oldKeyValue
          val newKeyValue = randomFunctionKeyValue(key = key)

          //oldKeyValue but it has a newer time.
          val oldKeyValue = randomFixedKeyValue(key = key)

          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"newKeyValue: $newKeyValue")

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = oldKeyValue,
            lastLevel = oldKeyValue.toLastLevelExpected
          )
        }
      }
    }
  }
}
