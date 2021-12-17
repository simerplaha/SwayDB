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

package swaydb.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import swaydb.utils.IDGenerator

import scala.collection.parallel.CollectionConverters._

class IDGeneratorSpec extends AnyFlatSpec {

  it should "always return new incremental ids when access concurrently" in {

    val gen = IDGenerator()

    (1 to 100).par.foldLeft(-1L) {
      case (previous, _) =>
        //check nextSegmentId should return valid text
        gen.nextSegmentId() should fullyMatch regex s"\\d+\\.seg"
        val next = gen.nextId()
        next should be > previous
        next
    }
  }

  it should "return segment string" in {
    IDGenerator.segment(1) should fullyMatch regex "1.seg"
  }
}
