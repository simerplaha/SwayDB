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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.merge

import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.merge.{KeyValueGrouper, MergeStats}
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.RunThis._
import swaydb.serializers.Default._
import swaydb.serializers._

class KeyValueGrouperSpec extends TestBase {

  implicit def testTimer: TestTimer = TestTimer.Empty

  "add fixed key-value" when {
    "expired" in {
      runThis(100.times) {
        var builder = MergeStats.random()

        val keyValue = randomFixedKeyValue(1, randomStringOption, Some(expiredDeadline()))
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

        val keyValue = randomFixedKeyValue(1, randomStringOption, deadline = None)
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

        val fromKeyValue = eitherOne(randomRangeValue(), Value.Put(randomStringOption, deadline = Some(expiredDeadline()), testTimer.next))
        val keyValue = randomRangeKeyValue(1, 100, fromValue = eitherOne(fromKeyValue, Value.FromValue.Null))
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

        val fromKeyValue = eitherOne(randomRangeValue(), Value.Put(randomStringOption, deadline = Some(expiredDeadline()), testTimer.next))
        val keyValue = randomRangeKeyValue(1, 100, fromValue = eitherOne(fromKeyValue, Value.FromValue.Null))
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
