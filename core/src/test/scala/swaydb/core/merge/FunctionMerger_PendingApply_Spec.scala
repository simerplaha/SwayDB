package swaydb.core.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TryAssert._
import swaydb.core.TestData._
import swaydb.core.{TestTimeGenerator, TryAssert}
import swaydb.core.data.Memory
import swaydb.data.slice.Slice
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TryAssert._
import swaydb.data.order.{KeyOrder, TimeOrder}

class FunctionMerger_PendingApply_Spec extends WordSpec with Matchers {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit def groupingStrategy = randomGroupingStrategyOption(randomNextInt(1000))

  "Merging Function into PendingApply with a single apply" when {

    "times are in order" should {

      "always return the same result as the Function being merged into Fixed" in {

        implicit val timeGenerator = TestTimeGenerator.Incremental()

        runThis(1000.times) {
          val key = randomBytesSlice()

          val apply = randomApplyWithDeadline()
          //oldKeyValue but it has a newer time.
          val oldKeyValue = Memory.PendingApply(key = key, Slice(apply))

          //new but has older time than oldKeyValue
          val newKeyValue = randomFunctionKeyValue(key = key)

          val expected = FixedMerger(newKeyValue, apply.toMemory(key)).assertGet

          //          println(s"newKeyValue: $newKeyValue")
          //          println(s"old apply: $apply")
          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"expected: $expected")
          //          println(s"function: ${functionStore.get(newKeyValue.function)}")

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected,
            lastLevelExpect = expected.asInstanceOf[Memory.Fixed].toLastLevelExpected
          )
        }
      }
    }
  }

  "Merging Function into PendingApply with multiple apply" when {

    "times are in order" should {

      "always return the same result as the Function being merged into Fixed" in {

        implicit val timeGenerator = TestTimeGenerator.Incremental()

        runThis(1000.times) {
          val key = eitherOne(randomBytesSlice(), Slice.emptyBytes)

          val oldApplies = (1 to randomIntMax(20)) map { _ => randomApplyWithDeadline() } toSlice

          val newKeyValue = randomFunctionKeyValue(key = key)

          val oldKeyValue = Memory.PendingApply(key = key, oldApplies)

          val expected = collapseMerge(newKeyValue, oldApplies)

          //          println(s"newKeyValue: $newKeyValue")
          //          println(s"old applies: $oldApplies")
          //          println(s"oldKeyValue: $oldKeyValue")
          ////          println(s"function result: ${functionStore.get(newKeyValue.function).get.asInstanceOf[SwayFunction.Key].f(randomBytesSlice())}")
          //          println(s"expected: $expected")
          //          println

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected,
            lastLevelExpect = expected.asInstanceOf[Memory.Fixed].toLastLevelExpected
          )
        }
      }
    }
  }
}
