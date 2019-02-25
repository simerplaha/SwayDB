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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.function

import org.scalatest.{FlatSpec, Matchers}
import swaydb.core.data.{SwayFunction, SwayFunctionOutput}
import swaydb.serializers.Default._
import swaydb.serializers._

class FunctionStoreSpec extends FlatSpec with Matchers {

  val store = FunctionStore.memory()

  it should "write int keys" in {

    (1 to 100) foreach {
      i =>
        val function = SwayFunction.Key(_ => SwayFunctionOutput.Update(Some(i), None))

        store.put(i, function)
        store.exists(i) should be
        store.get(i).get.asInstanceOf[SwayFunction.Key].f(i) shouldBe SwayFunctionOutput.Update(Some(i), None)
    }
  }
}
