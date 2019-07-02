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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util.cache

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import swaydb.data.IO

class CacheSpec extends WordSpec with Matchers with MockFactory {

  "CacheValue" should {
    "invoke the init function only once on success" in {
      val mock = mockFunction[IO[Int]]

      val cache = Cache.io[Int](false, false)(mock.apply())
      cache.isCached shouldBe false
      mock.expects() returning IO(123)

      cache.value shouldBe IO.Success(123)
      cache.isCached shouldBe true
      cache.value shouldBe IO.Success(123) //value again mock function is not invoked again
    }

    "not cache on failure" in {
      val mock = mockFunction[IO[Int]]

      val cache = Cache.io[Int](false, false)(mock.apply())
      cache.isCached shouldBe false
      mock.expects() returning IO.Failure("Kaboom!")

      //failure
      cache.value.failed.get.exception.getMessage shouldBe "Kaboom!"
      cache.isCached shouldBe false

      //success
      mock.expects() returning IO(123)
      cache.value shouldBe IO.Success(123) //value again mock function is not invoked again
      cache.isCached shouldBe true
    }

  }
}
