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

package swaydb.core.segment.entry.id

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BaseEntryIdFormatASpec extends AnyFlatSpec with Matchers {

  it should "contain unique ids" in {
    val baseIds = BaseEntryIdFormatA.baseIds
    val distinct = baseIds.map(_.baseId).distinct
    baseIds should have size distinct.size

    baseIds.map(_.baseId).foldLeft(-1) {
      case (previousId, nextId) =>
        //        println(s"previousId: $previousId -> nextId: $nextId")
        (nextId - previousId) shouldBe 1
        nextId
    }
  }
}
