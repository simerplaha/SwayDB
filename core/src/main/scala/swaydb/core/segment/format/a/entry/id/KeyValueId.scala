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

  def minKey_Compressed_KeyValueId: Int
  def maxKey_Compressed_KeyValueId: Int

  def minKey_Uncompressed_KeyValueId: Int
  def maxKey_Uncompressed_KeyValueId: Int

  def hasKeyValueId(keyValueId: Int): Boolean =
    keyValueId >= minKey_Compressed_KeyValueId && keyValueId <= maxKey_Uncompressed_KeyValueId

  def isKeyValueId_CompressedKey(keyValueId: Int): Boolean =
    keyValueId >= minKey_Compressed_KeyValueId && keyValueId <= maxKey_Compressed_KeyValueId

  def isKeyValueId_UncompressedKey(keyValueId: Int): Boolean =
    keyValueId >= minKey_Uncompressed_KeyValueId && keyValueId <= maxKey_Uncompressed_KeyValueId

  /**
    * Given persisted entryID convert it to [[BaseEntryId]].
    */
  def adjustKeyValueIdToBaseId(keyValueId: Int): Int =
    if (isKeyValueId_CompressedKey(keyValueId))
      if (minKey_Compressed_KeyValueId == KeyValueId.Put.minKey_Compressed_KeyValueId)
        keyValueId
      else
        keyValueId - minKey_Compressed_KeyValueId
    else if (isKeyValueId_UncompressedKey(keyValueId))
      keyValueId - minKey_Uncompressed_KeyValueId
    else
      throw new Exception(s"Int id: $keyValueId does not belong to ${this.getClass.getSimpleName} ")

  //Note: this exceptions above and below is not expected to occur. This may only occur due to file corruption.
  //instead of wrapping in IO for performance throw exception as this is not expected to occur.
  //if it does then it will be caught higher up in SegmentReader before responding the user.

  def adjustBaseIdToKeyValueIdKey(baseId: Int, keyCompressed: Boolean) =
    if (keyCompressed)
      adjustBaseIdToKeyValueIdKey_Compressed(baseId)
    else
      adjustBaseIdToKeyValueIdKey_UnCompressed(baseId)

  //the _ is to highlight that this will return an uncompressed id. There used to be a type-safe way to handle this!
  def adjustBaseIdToKeyValueIdKey_Compressed(baseId: Int): Int =
    if (minKey_Compressed_KeyValueId == KeyValueId.Put.minKey_Compressed_KeyValueId) //if it's put the ids are the same as base entry.
      if (isKeyValueId_CompressedKey(baseId))
        baseId
      else
        throw new Exception(s"Int id: $baseId does not belong to ${this.getClass.getSimpleName} ")
    else if (isKeyValueId_CompressedKey(baseId + minKey_Compressed_KeyValueId))
      baseId + minKey_Compressed_KeyValueId
    else
      throw new Exception(s"Int id: $baseId does not belong to ${this.getClass.getSimpleName}. Adjusted id was :${baseId + minKey_Compressed_KeyValueId}")

  //the _ is to highlight that this will return an uncompressed id. There used to be a type-safe way to handle this!
  def adjustBaseIdToKeyValueIdKey_UnCompressed(baseId: Int): Int =
    if (isKeyValueId_UncompressedKey(baseId + minKey_Uncompressed_KeyValueId))
      baseId + minKey_Uncompressed_KeyValueId
    else
      throw new Exception(s"Int id: $baseId does not belong to ${this.getClass.getSimpleName}. Adjusted id was :${baseId + minKey_Uncompressed_KeyValueId}")
}

object KeyValueId {
  //Last max id used in BaseEntryId.
  val reservedKeysPerGroup = EntryReader.readers.last.maxID

  object Put extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = EntryReader.readers.head.minID
    override val maxKey_Compressed_KeyValueId: Int = reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = reservedKeysPerGroup + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object Group extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = Put.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  /**
    * Reserve 1 & 2 bytes ids for Put and Group. All the following key-values
    * disappear in last Level but [[Put]] and [[Group]] are kept unless deleted.
    */
  object Range extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = 16384
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object Remove extends KeyValueId {

    override val minKey_Compressed_KeyValueId: Int = Range.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object Update extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = Remove.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object Function extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = Update.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object PendingApply extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = Function.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  def all: List[KeyValueId] =
    SealedList.list[KeyValueId]
}