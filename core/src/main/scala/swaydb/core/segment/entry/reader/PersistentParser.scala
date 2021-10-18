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

package swaydb.core.segment.entry.reader

import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Persistent, PersistentOption}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.entry.id.{KeyValueId, PersistentToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice

object PersistentParser {

  def parse(headerInteger: Int,
            indexOffset: Int,
            tailBytes: Slice[Byte],
            previous: PersistentOption,
            mightBeCompressed: Boolean,
            keyCompressionOnly: Boolean,
            sortedIndexEndOffset: Int,
            normalisedByteSize: Int,
            hasAccessPositionIndex: Boolean,
            optimisedForReverseIteration: Boolean,
            valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent = {
    val reader = Reader(tailBytes)

    val headerKeyBytes = reader.read(headerInteger)

    val keyValueId = reader.readUnsignedInt()

    var persistentKeyValue: Persistent = null

    def parsePersistent[T <: Persistent](readerType: Persistent.Reader[T])(implicit binder: PersistentToKeyValueIdBinder[T]) = {
      if (persistentKeyValue == null)
        persistentKeyValue =
          PersistentReader.read(
            indexOffset = indexOffset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = reader,
            previous = previous,
            //sorted index stats
            mightBeCompressed = mightBeCompressed,
            keyCompressionOnly = keyCompressionOnly,
            sortedIndexEndOffset = sortedIndexEndOffset,
            normalisedByteSize = normalisedByteSize,
            hasAccessPositionIndex = hasAccessPositionIndex,
            optimisedForReverseIteration = optimisedForReverseIteration,
            valuesReaderOrNull = valuesReaderOrNull,
            reader = readerType
          )

      persistentKeyValue
    }

    if (KeyValueId.Put hasKeyValueId keyValueId)
      parsePersistent(Persistent.Put)
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      parsePersistent(Persistent.Range)
    else if (KeyValueId.Remove hasKeyValueId keyValueId)
      parsePersistent(Persistent.Remove)
    else if (KeyValueId.Update hasKeyValueId keyValueId)
      parsePersistent(Persistent.Update)
    else if (KeyValueId.Function hasKeyValueId keyValueId)
      parsePersistent(Persistent.Function)
    else if (KeyValueId.PendingApply hasKeyValueId keyValueId)
      parsePersistent(Persistent.PendingApply)
    else
      throw swaydb.Exception.InvalidBaseId(keyValueId)
  }

  def parsePartial(offset: Int,
                   headerInteger: Int,
                   tailBytes: Slice[Byte],
                   sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial = {
    val tailReader = Reader(tailBytes)

    val headerKeyBytes = tailReader.read(headerInteger)

    val keyValueId = tailReader.readUnsignedInt()

    var persistentKeyValue: Persistent = null

    @inline def parsePersistent[T <: Persistent](reader: Persistent.Reader[T])(implicit binder: PersistentToKeyValueIdBinder[T]) = {
      if (persistentKeyValue == null)
        persistentKeyValue =
          PersistentReader.read[T](
            indexOffset = offset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            optimisedForReverseIteration = sortedIndex.block.optimiseForReverseIteration,
            valuesReaderOrNull = valuesReaderOrNull,
            reader = reader
          )

      persistentKeyValue
    }

    if (KeyValueId.Put hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Put)
      }
    else if (KeyValueId.Remove hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Remove)
      }
    else if (KeyValueId.Function hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Function)
      }
    else if (KeyValueId.Update hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Update)
      }
    else if (KeyValueId.PendingApply hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.PendingApply)
      }
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      new Partial.Range {
        val (fromKey, toKey) = Bytes.decompressJoin(headerKeyBytes)

        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          fromKey

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Range)
      }
    else
      throw new Exception(s"Invalid keyType: $keyValueId, offset: $offset, headerInteger: $headerInteger")
  }

  def matchPartial(offset: Int,
                   headerInteger: Int,
                   tailBytes: Slice[Byte],
                   sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial = {
    val tailReader = Reader(tailBytes)

    val headerKeyBytes = tailReader.read(headerInteger)

    val keyValueId = tailReader.readUnsignedInt()

    var persistentKeyValue: Persistent = null

    @inline def parsePersistent[T <: Persistent](reader: Persistent.Reader[T])(implicit binder: PersistentToKeyValueIdBinder[T]) = {
      if (persistentKeyValue == null)
        persistentKeyValue =
          PersistentReader.read[T](
            indexOffset = offset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            optimisedForReverseIteration = sortedIndex.block.optimiseForReverseIteration,
            valuesReaderOrNull = valuesReaderOrNull,
            reader = reader
          )

      persistentKeyValue
    }

    if (KeyValueId.Put hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Put)
      }
    else if (KeyValueId.Remove hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Remove)
      }
    else if (KeyValueId.Function hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Function)
      }
    else if (KeyValueId.Update hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Update)
      }
    else if (KeyValueId.PendingApply hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent =
          parsePersistent(Persistent.PendingApply)
      }
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      new Partial.Range {
        val (fromKey, toKey) = Bytes.decompressJoin(headerKeyBytes)

        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          fromKey

        override def toPersistent: Persistent =
          parsePersistent(Persistent.Range)
      }
    else
      throw new Exception(s"Invalid keyType: $keyValueId, offset: $offset, headerInteger: $headerInteger")
  }
}
