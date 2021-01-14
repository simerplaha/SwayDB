/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.function

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import swaydb.core.data.{SwayFunction, SwayFunctionOutput}
import swaydb.serializers.Default._
import swaydb.serializers._

class FunctionStoreSpec extends AnyFlatSpec with Matchers {

  val store = FunctionStore.memory()

  it should "write int keys" in {

    (1 to 100) foreach {
      i =>
        val function = SwayFunction.Key(_ => SwayFunctionOutput.Update(i, None))

        store.put(i, function)
        store.contains(i) shouldBe true
        store.get(i).get.asInstanceOf[SwayFunction.Key].f(i) shouldBe SwayFunctionOutput.Update(i, None)
    }
  }

  it should "not allow duplicate functions" in {

    val key = 0
    val function = SwayFunction.Key(_ => SwayFunctionOutput.Update(key, None))
    store.put(key, function)
    assertThrows[Exception] {
      store.put(key, function)
    }
  }
}
