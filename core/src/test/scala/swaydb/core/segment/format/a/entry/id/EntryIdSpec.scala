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

package swaydb.core.segment.format.a.entry.id

import org.scalatest.{FlatSpec, Matchers}

class EntryIdSpec extends FlatSpec with Matchers {

  it should "not allow conflicting ids and adjust baseIds to entryIds and vice versa" in {
    KeyValueId.keyValueId foreach { //for all ids
      id =>
        BaseEntryId.baseIds foreach { //for all base ids for each
          baseEntryId =>

            val otherIds = KeyValueId.keyValueId.filter(_ != id)
            otherIds should not be empty

            val entryId = id.adjustBaseIdToKeyValueId(baseEntryId.baseId)
            if (id == KeyValueId.Put) entryId shouldBe baseEntryId.baseId //not change. Put's are default.

            //entryId should have this entryId
            id.hasKeyValueId(keyValueId = entryId) shouldBe true

            //others should not
            otherIds.foreach(_.hasKeyValueId(entryId) shouldBe false)

            //bump to entryID to be uncompressed.
            val uncompressedEntryId = id.adjustKeyValueIdToKeyUncompressed(baseEntryId.baseId)
            uncompressedEntryId should be >= entryId
            //id should have this entryId
            id.hasKeyValueId(keyValueId = uncompressedEntryId) shouldBe true
            //others should not
            otherIds.foreach(_.hasKeyValueId(uncompressedEntryId) shouldBe false)

            //adjust uncompressed entryId to base should return back to original.
            id.adjustKeyValueIdToBaseId(uncompressedEntryId) shouldBe baseEntryId.baseId
        }
    }
  }
}
