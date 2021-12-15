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

package swaydb.core.segment.block.reader

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.slice.Slice

class BlockReaderCacheSpec extends AnyWordSpec with Matchers {

  "set & read" when {
    "set position is 0" in {
      val state = BlockReaderCache.init(0, Slice[Byte](1.toByte, 2.toByte, 3.toByte))

      BlockReaderCache.read(0, 10, state) shouldBe Slice(1.toByte, 2.toByte, 3.toByte)
      BlockReaderCache.read(1, 10, state) shouldBe Slice(2.toByte, 3.toByte)
      BlockReaderCache.read(2, 10, state) shouldBe Slice(3.toByte)
      BlockReaderCache.read(3, 10, state) shouldBe empty
      BlockReaderCache.read(10, 10, state) shouldBe empty
    }

    "set position is 10" in {
      val state = BlockReaderCache.init(10, Slice[Byte](10.toByte, 11.toByte, 12.toByte))

      (0 to 9) foreach {
        i =>
          BlockReaderCache.read(i, 10, state) shouldBe empty
      }

      BlockReaderCache.read(10, 1, state) shouldBe Slice[Byte](10.toByte)
      BlockReaderCache.read(10, 2, state) shouldBe Slice[Byte](10.toByte, 11.toByte)
      BlockReaderCache.read(10, 3, state) shouldBe Slice[Byte](10.toByte, 11.toByte, 12.toByte)
      BlockReaderCache.read(10, 10, state) shouldBe Slice[Byte](10.toByte, 11.toByte, 12.toByte)

      BlockReaderCache.read(11, 1, state) shouldBe Slice[Byte](11.toByte)
      BlockReaderCache.read(11, 2, state) shouldBe Slice[Byte](11.toByte, 12.toByte)
      BlockReaderCache.read(11, 3, state) shouldBe Slice[Byte](11.toByte, 12.toByte)
      BlockReaderCache.read(11, 10, state) shouldBe Slice[Byte](11.toByte, 12.toByte)

      BlockReaderCache.read(12, 1, state) shouldBe Slice[Byte](12.toByte)
      BlockReaderCache.read(12, 2, state) shouldBe Slice[Byte](12.toByte)
      BlockReaderCache.read(12, 3, state) shouldBe Slice[Byte](12.toByte)
      BlockReaderCache.read(12, 10, state) shouldBe Slice[Byte](12.toByte)

      BlockReaderCache.read(13, 1, state) shouldBe empty
      BlockReaderCache.read(13, 2, state) shouldBe empty
      BlockReaderCache.read(13, 3, state) shouldBe empty
      BlockReaderCache.read(13, 10, state) shouldBe empty
    }
  }
}
