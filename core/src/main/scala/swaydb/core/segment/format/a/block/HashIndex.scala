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

package swaydb.core.segment.format.a.block

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.CompressionInternal
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.format.a.{KeyMatcher, OffsetBase}
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.config.RandomKeyIndex
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

/**
  * HashIndex.
  */
private[core] object HashIndex extends LazyLogging {

  object Config {
    val disabled =
      Config(
        maxProbe = -1,
        minimumNumberOfKeys = Int.MaxValue,
        allocateSpace = _ => Int.MinValue,
        cacheOnAccess = false,
        compressions = Seq.empty
      )

    def apply(config: swaydb.data.config.RandomKeyIndex): Config =
      config match {
        case swaydb.data.config.RandomKeyIndex.Disable =>
          Config(
            maxProbe = -1,
            minimumNumberOfKeys = Int.MaxValue,
            allocateSpace = _ => Int.MinValue,
            cacheOnAccess = false,
            compressions = Seq.empty
          )
        case enable: swaydb.data.config.RandomKeyIndex.Enable =>
          Config(
            maxProbe = enable.retries,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            allocateSpace = enable.allocateSpace,
            cacheOnAccess = enable.cacheOnAccess,
            compressions = enable.compression map CompressionInternal.apply
          )
      }
  }

  case class Config(maxProbe: Int,
                    minimumNumberOfKeys: Int,
                    allocateSpace: RandomKeyIndex.RequiredSpace => Int,
                    cacheOnAccess: Boolean,
                    compressions: Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends OffsetBase

  final case class State(var hit: Int,
                         var miss: Int,
                         minimumNumberOfKeys: Int,
                         writeAbleLargestValueSize: Int,
                         headerSize: Int,
                         maxProbe: Int,
                         var _bytes: Slice[Byte],
                         compressions: Seq[CompressionInternal]) {

    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes

    def hasMinimumKeys =
      hit >= minimumNumberOfKeys
  }

  def init(maxProbe: Int,
           keyValues: Iterable[KeyValue.WriteOnly],
           compressions: Seq[CompressionInternal]): Option[HashIndex.State] =
    if (keyValues.size < keyValues.last.hashIndexConfig.minimumNumberOfKeys)
      None
    else if (keyValues.last.stats.segmentHashIndexSize <= 6) //formatId, maxProbe, hit, miss, largestValue, allocatedBytes
      None
    else
      Some {
        val writeAbleLargestValueSize = Bytes.sizeOf(keyValues.last.stats.thisKeyValuesAccessIndexOffset + 1)
        val headSize =
          headerSize(
            keyCounts = keyValues.last.stats.segmentUniqueKeysCount,
            writeAbleLargestValueSize = writeAbleLargestValueSize,
            hasCompression = compressions.nonEmpty
          )
        HashIndex.State(
          hit = 0,
          miss = 0,
          minimumNumberOfKeys = keyValues.last.hashIndexConfig.minimumNumberOfKeys,
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          headerSize = headSize,
          maxProbe = maxProbe,
          _bytes = Slice.create[Byte](keyValues.last.stats.segmentHashIndexSize),
          compressions = compressions
        )
      }

  def headerSize(keyCounts: Int,
                 writeAbleLargestValueSize: Int,
                 hasCompression: Boolean): Int = {
    val headerSize =
      Block.headerSize(hasCompression) +
        ByteSizeOf.int + 1 + //allocated bytes
        ByteSizeOf.int + //max probe
        (Bytes.sizeOf(keyCounts) * 2) + //hit & miss rate
        Bytes.sizeOf(writeAbleLargestValueSize) //largest value size

    Bytes.sizeOf(headerSize) +
      headerSize
  }

  /**
    * Number of bytes required to build a high probability index.
    */
  def optimalBytesRequired(keyCounts: Int,
                           minimumNumberOfKeys: Int,
                           largestValue: Int,
                           hasCompression: Boolean,
                           allocateSpace: RandomKeyIndex.RequiredSpace => Int): Int = {
    if (keyCounts < minimumNumberOfKeys) {
      0
    } else {
      val writeAbleLargestValueSize = Bytes.sizeOf(largestValue + 1) //largest value is +1 because 0s are reserved.

      val minimumRequired =
        headerSize(
          keyCounts = keyCounts,
          hasCompression = hasCompression,
          writeAbleLargestValueSize = writeAbleLargestValueSize
        ) + (keyCounts * (writeAbleLargestValueSize + 1)) //+1 to skip left & right 0 start-end markers.

      Try {
        allocateSpace {
          new RandomKeyIndex.RequiredSpace {
            override def requiredSpace: Int = minimumRequired
            override def numberOfKeys: Int = keyCounts
          }
        }
      } getOrElse minimumRequired
    }
  }

  def close(state: State): IO[State] =
    Block.create(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions
    ) flatMap {
      compressedOrUncompressedBytes =>
        IO {
          val allocatedBytes = state.bytes.size
          state.bytes = compressedOrUncompressedBytes
          state.bytes addInt allocatedBytes //allocated bytes
          state.bytes addInt state.maxProbe
          state.bytes addIntUnsigned state.hit
          state.bytes addIntUnsigned state.miss
          state.bytes addIntUnsigned state.writeAbleLargestValueSize
          if (state.bytes.currentWritePosition > state.headerSize)
            throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
          state
        }
    }

  def read(offset: Offset, reader: Reader): IO[HashIndex] =
    for {
      result <- Block.readHeader(offset = offset, segmentReader = reader)
      allocatedBytes <- result.headerReader.readInt()
      maxProbe <- result.headerReader.readInt()
      hit <- result.headerReader.readIntUnsigned()
      miss <- result.headerReader.readIntUnsigned()
      largestValueSize <- result.headerReader.readIntUnsigned()
    } yield
      HashIndex(
        offset = offset,
        compressionInfo = result.compressionInfo,
        maxProbe = maxProbe,
        hit = hit,
        miss = miss,
        writeAbleLargestValueSize = largestValueSize,
        headerSize = result.headerSize,
        allocatedBytes = allocatedBytes
      )

  def adjustHash(hash: Int,
                 totalBlockSpace: Int,
                 headerSize: Int,
                 writeAbleLargestValueSize: Int) =
    ((hash & Int.MaxValue) % (totalBlockSpace - writeAbleLargestValueSize - headerSize)) + headerSize

  /**
    * Mutates the slice and adds writes the indexOffset to it's hash index.
    */
  def write(key: Slice[Byte],
            value: Int,
            state: State): IO[Boolean] = {

    //add 1 to each offset to avoid 0 offsets.
    //0 bytes are reserved as empty bucket markers.
    val valuePlusOne = value + 1
    val valueBytes = Slice.writeIntUnsigned(valuePlusOne)

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    @tailrec
    def doWrite(key: Slice[Byte], probe: Int): Boolean =
      if (probe >= state.maxProbe) {
        //println(s"Key: ${key.readInt()}: write index: miss probe: $probe")
        state.miss += 1
        false
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            totalBlockSpace = state.bytes.size,
            headerSize = state.headerSize,
            writeAbleLargestValueSize = state.writeAbleLargestValueSize
          )

        val existing = state.bytes.take(hashIndex, valueBytes.size + 2) ////+1 to reserve left 0 byte another +1 not overwrite next 0.
        if (existing.forall(_ == 0)) {
          state.bytes moveWritePosition (hashIndex + 1)
          state.bytes addAll valueBytes
          state.hit += 1
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, valueBytes: ${Slice.writeIntUnsigned(valuePlusOne)} = success")
          true
        } else if (existing == valueBytes) {
          true
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, valueBytes: ${Slice.writeIntUnsigned(valuePlusOne)} = failure")
          doWrite(key = key, probe = probe + 1)
        }
      }

    if (state.bytes.size == 0)
      IO.`false`
    else
      IO(doWrite(key, 0))
  }

  /**
    * Finds a key in the hash index.
    *
    * @param assertValue performs find or forward fetch from the currently being read sorted index's hash block.
    */
  private[block] def find[V](key: Slice[Byte],
                             blockReader: BlockReader[HashIndex],
                             assertValue: Int => IO[Option[V]]): IO[Option[V]] = {

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val hashIndex = blockReader.block

    @tailrec
    def doFind(probe: Int, checkedHashIndexes: mutable.HashSet[Int]): IO[Option[V]] =
      if (probe > hashIndex.maxProbe) {
        IO.none
      } else {
        val index =
          adjustHash(
            hash = hash1 + probe * hash2,
            totalBlockSpace = hashIndex.allocatedBytes,
            headerSize = hashIndex.headerSize,
            writeAbleLargestValueSize = hashIndex.writeAbleLargestValueSize
          ) - hashIndex.headerSize

        if (checkedHashIndexes contains index) //do not check the same index again.
          doFind(probe + 1, checkedHashIndexes)
        else
          blockReader
            .moveTo(index)
            .read(hashIndex.bytesToReadPerIndex) match {
            case IO.Success(possibleValueBytes) =>
              //println(s"Key: ${key.readInt()}: read hashIndex: $hashedIndex probe: $probe. sortedIndex bytes: $possibleValueBytes")
              if (possibleValueBytes.head != 0) { //head should never be empty because the hash adjusts it.
                //println(s"Key: ${key.readInt()}: read hashIndex: $hashedIndex probe: $probe = failure - invalid start offset.")
                doFind(probe + 1, checkedHashIndexes)
              } else {
                val possibleValueWithoutHeader = possibleValueBytes.dropHead()
                possibleValueWithoutHeader.readIntUnsigned() match {
                  case IO.Success(possibleValue) =>
                    //println(s"Key: ${key.readInt()}: read hashIndex: $hashedIndex probe: $probe, sortedIndex: ${possibleValue - 1} = reading now!")
                    if (possibleValue == 0 || possibleValueWithoutHeader.take(Bytes.sizeOf(possibleValue)).exists(_ == 0))
                      doFind(probe + 1, checkedHashIndexes)
                    else
                      assertValue(possibleValue - 1) match { //assert value removing the 1 added on write.
                        case success @ IO.Success(Some(_)) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: $hashedIndex probe: $probe, sortedIndex: ${possibleValue - 1} = success")
                          success

                        case IO.Success(None) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: $hashedIndex probe: $probe: sortedIndex: ${possibleValue - 1} = not found")
                          doFind(probe + 1, checkedHashIndexes += index)

                        case IO.Failure(error) =>
                          IO.Failure(error)
                      }

                  case IO.Failure(error) =>
                    IO.Failure(error)
                }
              }

            case IO.Failure(error) =>
              IO.Failure(error)
          }
      }

    doFind(
      probe = 0,
      checkedHashIndexes = mutable.HashSet.empty
    )
  }

  def get(matcher: KeyMatcher.Get.WhilePrefixCompressed,
          hashIndex: BlockReader[HashIndex],
          sortedIndex: BlockReader[SortedIndex],
          values: Option[BlockReader[Values]]): IO[Option[Persistent]] =
    find(
      key = matcher.key,
      blockReader = hashIndex,
      assertValue =
        sortedIndexOffsetValue =>
          SortedIndex.findAndMatchOrNext(
            matcher = matcher,
            fromOffset = sortedIndexOffsetValue,
            indexReader = sortedIndex,
            valueReader = values
          )
    )
}

case class HashIndex(offset: HashIndex.Offset,
                     compressionInfo: Option[Block.CompressionInfo],
                     maxProbe: Int,
                     hit: Int,
                     miss: Int,
                     writeAbleLargestValueSize: Int,
                     headerSize: Int,
                     allocatedBytes: Int) extends Block {
  val bytesToReadPerIndex = writeAbleLargestValueSize + 1 //+1 to read header 0 byte.

  val isCompressed = compressionInfo.isDefined

  override def createBlockReader(bytes: Slice[Byte]): BlockReader[HashIndex] =
    createBlockReader(Reader(bytes))

  def createBlockReader(segmentReader: Reader): BlockReader[HashIndex] =
    BlockReader(
      reader = segmentReader,
      block = this
    )

  override def updateOffset(start: Int, size: Int): Block =
    copy(offset = HashIndex.Offset(start = start, size = size))
}