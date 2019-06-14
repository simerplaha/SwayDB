/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
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

import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.macros.SealedList

sealed trait KeyValueId {

  def minKeyPartiallyCompressedEntryId: Int
  def maxKeyPartiallyCompressedEntryId: Int

  def minKeyUncompressedEntryId: Int
  def maxKeyUncompressedEntryId: Int

  def hasKeyValueId(keyValueId: Int): Boolean =
    keyValueId >= minKeyPartiallyCompressedEntryId && keyValueId <= maxKeyUncompressedEntryId

  def isKeyValueIdPartiallyCompressedKey(keyValueId: Int): Boolean =
    keyValueId >= minKeyPartiallyCompressedEntryId && keyValueId <= maxKeyPartiallyCompressedEntryId

  def isKeyValueIdUncompressedKey(keyValueId: Int): Boolean =
    keyValueId >= minKeyUncompressedEntryId && keyValueId <= maxKeyUncompressedEntryId

  /**
    * Given persisted entryID convert it to
    */
  def adjustKeyValueIdToBaseId(keyValueId: Int): Int =
    if (isKeyValueIdPartiallyCompressedKey(keyValueId))
      if (minKeyPartiallyCompressedEntryId == KeyValueId.Put.minKeyPartiallyCompressedEntryId)
        keyValueId
      else
        keyValueId - minKeyPartiallyCompressedEntryId
    else if (isKeyValueIdUncompressedKey(keyValueId))
      keyValueId - minKeyUncompressedEntryId
    else
      throw new Exception(s"Int id: $keyValueId does not belong to ${this.getClass.getSimpleName} ")

  //Note: this exceptions above and below is not expected to occur. This may only occur due to file corruption.
  //instead of wrapping in IO for performance throw exception as this is not expected to occur.
  //if it does then it will be caught higher up in SegmentReader before responding the user.

  def adjustBaseIdToKeyValueId(baseId: Int): Int =
    if (minKeyPartiallyCompressedEntryId == KeyValueId.Put.minKeyPartiallyCompressedEntryId) //if it's put the ids are the same as base entry.
      if (isKeyValueIdPartiallyCompressedKey(baseId))
        baseId
      else if (isKeyValueIdUncompressedKey(baseId + minKeyUncompressedEntryId))
        baseId + minKeyUncompressedEntryId
      else
        throw new Exception(s"Int id: $baseId does not belong to ${this.getClass.getSimpleName} ")
    else if (isKeyValueIdPartiallyCompressedKey(baseId + minKeyPartiallyCompressedEntryId))
      baseId + minKeyPartiallyCompressedEntryId
    else if (isKeyValueIdUncompressedKey(baseId + minKeyUncompressedEntryId))
      baseId + minKeyUncompressedEntryId
    else
      throw new Exception(s"Int id: $baseId does not belong to ${this.getClass.getSimpleName} ")

  def adjustKeyValueIdToKeyUncompressed(keyValueId: Int): Int =
    keyValueId + minKeyUncompressedEntryId

  def adjustBaseIdToKeyValueIdAndKeyUncompressed(baseId: Int): Int =
    adjustKeyValueIdToKeyUncompressed(adjustBaseIdToKeyValueId(baseId))
}

object KeyValueId {
  //Last max id used in BaseEntryId.
  val reservedKeysPerGroup = EntryReader.readers.last.maxID

  object Put extends KeyValueId {
    override val minKeyPartiallyCompressedEntryId: Int = EntryReader.readers.head.minID
    override val maxKeyPartiallyCompressedEntryId: Int = reservedKeysPerGroup
    override val minKeyUncompressedEntryId: Int = reservedKeysPerGroup + 1
    override val maxKeyUncompressedEntryId: Int = minKeyUncompressedEntryId + reservedKeysPerGroup
  }

  object Group extends KeyValueId {
    override val minKeyPartiallyCompressedEntryId: Int = Put.maxKeyUncompressedEntryId + 1
    override val maxKeyPartiallyCompressedEntryId: Int = minKeyPartiallyCompressedEntryId + reservedKeysPerGroup
    override val minKeyUncompressedEntryId: Int = maxKeyPartiallyCompressedEntryId + 1
    override val maxKeyUncompressedEntryId: Int = minKeyUncompressedEntryId + reservedKeysPerGroup
  }

  /**
    * Reserve 1 & 2 bytes ids for Put and Group. All the following key-values
    * disappear in last Level but [[Put]] and [[Group]] are kept unless deleted.
    */
  object Range extends KeyValueId {
    override val minKeyPartiallyCompressedEntryId: Int = 16384
    override val maxKeyPartiallyCompressedEntryId: Int = minKeyPartiallyCompressedEntryId + reservedKeysPerGroup
    override val minKeyUncompressedEntryId: Int = maxKeyPartiallyCompressedEntryId + 1
    override val maxKeyUncompressedEntryId: Int = minKeyUncompressedEntryId + reservedKeysPerGroup
  }

  object Remove extends KeyValueId {

    override val minKeyPartiallyCompressedEntryId: Int = Range.maxKeyUncompressedEntryId + 1
    override val maxKeyPartiallyCompressedEntryId: Int = minKeyPartiallyCompressedEntryId + reservedKeysPerGroup
    override val minKeyUncompressedEntryId: Int = maxKeyPartiallyCompressedEntryId + 1
    override val maxKeyUncompressedEntryId: Int = minKeyUncompressedEntryId + reservedKeysPerGroup
  }

  object Update extends KeyValueId {
    override val minKeyPartiallyCompressedEntryId: Int = Remove.maxKeyUncompressedEntryId + 1
    override val maxKeyPartiallyCompressedEntryId: Int = minKeyPartiallyCompressedEntryId + reservedKeysPerGroup
    override val minKeyUncompressedEntryId: Int = maxKeyPartiallyCompressedEntryId + 1
    override val maxKeyUncompressedEntryId: Int = minKeyUncompressedEntryId + reservedKeysPerGroup
  }

  object Function extends KeyValueId {
    override val minKeyPartiallyCompressedEntryId: Int = Update.maxKeyUncompressedEntryId + 1
    override val maxKeyPartiallyCompressedEntryId: Int = minKeyPartiallyCompressedEntryId + reservedKeysPerGroup
    override val minKeyUncompressedEntryId: Int = maxKeyPartiallyCompressedEntryId + 1
    override val maxKeyUncompressedEntryId: Int = minKeyUncompressedEntryId + reservedKeysPerGroup
  }

  object PendingApply extends KeyValueId {
    override val minKeyPartiallyCompressedEntryId: Int = Function.maxKeyUncompressedEntryId + 1
    override val maxKeyPartiallyCompressedEntryId: Int = minKeyPartiallyCompressedEntryId + reservedKeysPerGroup
    override val minKeyUncompressedEntryId: Int = maxKeyPartiallyCompressedEntryId + 1
    override val maxKeyUncompressedEntryId: Int = minKeyUncompressedEntryId + reservedKeysPerGroup
  }

  def keyValueId: List[KeyValueId] =
    SealedList.list[KeyValueId]
}