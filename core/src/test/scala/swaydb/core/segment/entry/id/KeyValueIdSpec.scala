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

package swaydb.core.segment.entry.id

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KeyValueIdSpec extends AnyFlatSpec with Matchers {

  it should "not have overlapping ids" in {

    KeyValueId.all.foldLeft(-1) {
      case (previousId, keyValueId) =>
        keyValueId.minKey_Compressed_KeyValueId == previousId + 1
        keyValueId.maxKey_Compressed_KeyValueId > keyValueId.minKey_Compressed_KeyValueId + 1

        keyValueId.minKey_Uncompressed_KeyValueId == keyValueId.maxKey_Compressed_KeyValueId + 1
        keyValueId.maxKey_Uncompressed_KeyValueId > keyValueId.minKey_Uncompressed_KeyValueId + 1

        keyValueId.maxKey_Uncompressed_KeyValueId
    }
  }

  it should "not allow conflicting ids and adjust baseIds to entryIds and vice versa" in {
    KeyValueId.all foreach { //for all ids
      keyValueId =>
        BaseEntryIdFormatA.baseIds foreach { //for all base ids for each
          baseEntryId =>

            val otherIds = KeyValueId.all.filter(_ != keyValueId)
            otherIds should not be empty

            val entryId = keyValueId.adjustBaseIdToKeyValueIdKey_Compressed(baseEntryId.baseId)
            if (keyValueId == KeyValueId.Put) entryId shouldBe baseEntryId.baseId //not change. Put's are default.

            //entryId should have this entryId
            keyValueId.hasKeyValueId(keyValueId = entryId) shouldBe true

            //others should not
            otherIds.foreach(_.hasKeyValueId(entryId) shouldBe false)

            //bump to entryID to be uncompressed.
            val uncompressedEntryId = keyValueId.adjustBaseIdToKeyValueIdKey_UnCompressed(baseEntryId.baseId)
            uncompressedEntryId should be >= entryId
            //id should have this entryId
            keyValueId.hasKeyValueId(keyValueId = uncompressedEntryId) shouldBe true
            //others should not
            otherIds.foreach(_.hasKeyValueId(uncompressedEntryId) shouldBe false)

            //adjust uncompressed entryId to base should return back to original.
            keyValueId.adjustKeyValueIdToBaseId(uncompressedEntryId) shouldBe baseEntryId.baseId
        }
    }
  }
}
