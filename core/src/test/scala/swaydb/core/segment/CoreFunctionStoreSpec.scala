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

package swaydb.core.segment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import swaydb.core.segment.data.{CoreFunction, CoreFunctionOutput}
import swaydb.serializers.Default._
import swaydb.serializers._

class CoreFunctionStoreSpec extends AnyFlatSpec with Matchers {

  val store = CoreFunctionStore.memory()

  it should "write int keys" in {

    (1 to 100) foreach {
      i =>
        val function = CoreFunction.Key(_ => CoreFunctionOutput.Update(i, None))

        store.put(i, function)
        store.contains(i) shouldBe true
        store.get(i).get.asInstanceOf[CoreFunction.Key].f(i) shouldBe CoreFunctionOutput.Update(i, None)
    }
  }

  it should "not allow duplicate functions" in {

    val key = 0
    val function = CoreFunction.Key(_ => CoreFunctionOutput.Update(key, None))
    store.put(key, function)
    assertThrows[Exception] {
      store.put(key, function)
    }
  }
}
