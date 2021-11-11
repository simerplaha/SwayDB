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

import swaydb.IO
import swaydb.core.segment.entry.reader.base.BaseEntryReader
import swaydb.macros.Sealed

sealed trait KeyValueId {

  val minKey_Compressed_KeyValueId: Int
  val maxKey_Compressed_KeyValueId: Int

  val minKey_Uncompressed_KeyValueId: Int
  val maxKey_Uncompressed_KeyValueId: Int

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
      throw new Exception(s"Int id: $keyValueId does not belong to ${this.getClass.getSimpleName}")

  //Note: this exceptions above and below is not expected to occur. This may only occur due to file corruption.
  //instead of wrapping in IO for performance throw exception as this is not expected to occur.
  //if it does then it will be caught higher up in SegmentReader before responding the user.

  def adjustBaseIdToKeyValueIdKey(baseId: Int, isKeyCompressed: Boolean) =
    if (isKeyCompressed)
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
  val reservedKeysPerGroup = BaseEntryReader.readers.last.maxID

  //byte size of the maximum keyValueId that can be persisted.
  //the macro cannot be used because it's make KeyValueIds vals to return 0
  //  val maxKeyValueIdByteSize = Bytes.sizeOfUnsignedInt(all.map(_.maxKey_Uncompressed_KeyValueId).max)
  val maxKeyValueIdByteSize = 3

  def isFixedId(id: Int): Boolean =
    KeyValueId.Put.hasKeyValueId(id) || KeyValueId.Remove.hasKeyValueId(id) || KeyValueId.Update.hasKeyValueId(id) || KeyValueId.Function.hasKeyValueId(id) || KeyValueId.PendingApply.hasKeyValueId(id)

  def isRange(id: Int): Boolean =
    KeyValueId.Range.hasKeyValueId(id)

  object Put extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = BaseEntryReader.readers.head.minID
    override val maxKey_Compressed_KeyValueId: Int = reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = reservedKeysPerGroup + 1
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

    override val minKey_Compressed_KeyValueId: Int = KeyValueId.Range.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object Update extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = KeyValueId.Remove.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object Function extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = KeyValueId.Update.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  object PendingApply extends KeyValueId {
    override val minKey_Compressed_KeyValueId: Int = KeyValueId.Function.maxKey_Uncompressed_KeyValueId + 1
    override val maxKey_Compressed_KeyValueId: Int = minKey_Compressed_KeyValueId + reservedKeysPerGroup
    override val minKey_Uncompressed_KeyValueId: Int = maxKey_Compressed_KeyValueId + 1
    override val maxKey_Uncompressed_KeyValueId: Int = minKey_Uncompressed_KeyValueId + reservedKeysPerGroup
  }

  def all: List[KeyValueId] =
    Sealed.list[KeyValueId]
}
