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

package swaydb.core.segment.data.merge

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.log.timer.TestTimer
import swaydb.core.segment.{CoreFunctionStore, TestCoreFunctionStore}
import swaydb.core.segment.data._
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.SliceTestKit._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class KeyValueGrouperSpec extends AnyWordSpec {

  implicit def testTimer: TestTimer = TestTimer.Empty

  private implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  private implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  private implicit val testFunctionStore: TestCoreFunctionStore = TestCoreFunctionStore()
  private implicit val functionStore: CoreFunctionStore = testFunctionStore.store

  "add fixed key-value" when {
    "expired" in {
      runThis(100.times) {
        var builder = MergeStats.random()

        val keyValue = randomFixedKeyValue(1, genStringOption(), Some(expiredDeadline()))
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = false)
        builder.keyValues should contain only keyValue

        builder = MergeStats.random()
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = true)
        builder.keyValues should have size 0
      }
    }

    "not expired" in {
      runThis(100.times) {
        var builder = MergeStats.random()

        val keyValue = randomFixedKeyValue(1, genStringOption(), deadline = None)
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = false)
        builder.keyValues should contain only keyValue

        builder = MergeStats.random()
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = true)
        if (keyValue.isPut)
          builder.keyValues should contain only keyValue
        else
          builder.keyValues should have size 0
      }
    }
  }

  "add range key-value" when {
    "expired" in {
      runThis(100.times) {
        var builder = MergeStats.random()

        val fromKeyValue = eitherOne(randomRangeValue(), Value.Put(genStringOption(), deadline = Some(expiredDeadline()), testTimer.next))
        val keyValue = randomRangeKeyValue(1, 100, fromValue = eitherOne(fromKeyValue, Value.FromValue.Null), randomRangeValue())
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = false)
        builder.keyValues should contain only keyValue

        builder = MergeStats.random()
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = true)
        builder.keyValues should have size 0
      }
    }

    "not expired" in {
      runThis(100.times) {
        var builder = MergeStats.random()

        val fromKeyValue = eitherOne(randomRangeValue(), Value.Put(genStringOption(), deadline = Some(expiredDeadline()), testTimer.next))
        val keyValue = randomRangeKeyValue(1, 100, fromValue = eitherOne(fromKeyValue, Value.FromValue.Null), randomRangeValue())
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = false)
        builder.keyValues should contain only keyValue

        builder = MergeStats.random()
        KeyValueGrouper.add(keyValue = keyValue, builder = builder, isLastLevel = true)
        if (keyValue.isPut)
          builder.keyValues should contain only keyValue
        else
          builder.keyValues should have size 0
      }
    }
  }
}
