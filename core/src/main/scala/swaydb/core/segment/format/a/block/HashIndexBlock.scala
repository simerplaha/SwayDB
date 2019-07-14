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
import swaydb.core.io.reader.BlockReader
import swaydb.core.util.{Bytes, FunctionUtil}
import swaydb.data.IO
import swaydb.data.config.{BlockIO, BlockStatus, RandomKeyIndex, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * HashIndex.
  */
private[core] object HashIndexBlock extends LazyLogging {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    val disabled =
      Config(
        maxProbe = -1,
        minimumNumberOfKeys = Int.MaxValue,
        allocateSpace = _ => Int.MinValue,
        minimumNumberOfHits = Int.MaxValue,
        blockIO = blockStatus => BlockIO.SynchronisedIO(cacheOnAccess = blockStatus.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.RandomKeyIndex): Config =
      config match {
        case swaydb.data.config.RandomKeyIndex.Disable =>
          Config(
            maxProbe = -1,
            minimumNumberOfKeys = Int.MaxValue,
            allocateSpace = _ => Int.MinValue,
            minimumNumberOfHits = Int.MaxValue,
            blockIO = blockStatus => BlockIO.SynchronisedIO(cacheOnAccess = blockStatus.isCompressed),
            compressions = _ => Seq.empty
          )
        case enable: swaydb.data.config.RandomKeyIndex.Enable =>
          Config(
            maxProbe = enable.tries,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            minimumNumberOfHits = enable.minimumNumberOfHits,
            allocateSpace = enable.allocateSpace,
            blockIO = FunctionUtil.safe(BlockIO.default, enable.blockIO),
            compressions =
              FunctionUtil.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(maxProbe: Int,
                    minimumNumberOfKeys: Int,
                    minimumNumberOfHits: Int,
                    allocateSpace: RandomKeyIndex.RequiredSpace => Int,
                    blockIO: BlockStatus => BlockIO,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  final case class State(var hit: Int,
                         var miss: Int,
                         minimumNumberOfKeys: Int,
                         minimumNumberOfHits: Int,
                         writeAbleLargestValueSize: Int,
                         headerSize: Int,
                         maxProbe: Int,
                         var _bytes: Slice[Byte],
                         compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {

    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes

    def hasMinimumHits =
      hit >= minimumNumberOfHits
  }

  def init(keyValues: Iterable[KeyValue.WriteOnly]): Option[HashIndexBlock.State] =
    if (keyValues.size < keyValues.last.hashIndexConfig.minimumNumberOfKeys)
      None
    else if (keyValues.last.stats.segmentHashIndexSize <= 0) //formatId, maxProbe, hit, miss, largestValue, allocatedBytes
      None
    else {
      val writeAbleLargestValueSize = Bytes.sizeOf(keyValues.last.stats.thisKeyValuesAccessIndexOffset + 1)
      val hasCompression = keyValues.last.hashIndexConfig.compressions(UncompressedBlockInfo(keyValues.last.stats.segmentHashIndexSize)).nonEmpty

      val headSize =
        headerSize(
          keyCounts = keyValues.last.stats.segmentUniqueKeysCount,
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          hasCompression = hasCompression
        )

      val optimalBytes =
        optimalBytesRequired(
          keyCounts = keyValues.last.stats.segmentUniqueKeysCount,
          minimumNumberOfKeys = keyValues.last.hashIndexConfig.minimumNumberOfKeys,
          largestValue = writeAbleLargestValueSize,
          allocateSpace = keyValues.last.hashIndexConfig.allocateSpace,
          hasCompression = hasCompression
        )

      //if the user allocated
      if (optimalBytes < headSize + ByteSizeOf.varInt)
        None
      else
        Some(
          HashIndexBlock.State(
            hit = 0,
            miss = 0,
            minimumNumberOfKeys = keyValues.last.hashIndexConfig.minimumNumberOfKeys,
            minimumNumberOfHits = keyValues.last.hashIndexConfig.minimumNumberOfHits,
            writeAbleLargestValueSize = writeAbleLargestValueSize,
            headerSize = headSize,
            maxProbe = keyValues.last.hashIndexConfig.maxProbe,
            _bytes = Slice.create[Byte](optimalBytes),
            compressions = keyValues.last.hashIndexConfig.compressions
          )
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

      try
        allocateSpace(
          RandomKeyIndex.RequiredSpace(
            _requiredSpace = minimumRequired,
            _numberOfKeys = keyCounts
          )
        )
      catch {
        case exception: Exception =>
          logger.error(
            """Custom allocate space calculation for HashIndex returned failure.
              |Using the default requiredSpace instead. Please check your implementation to ensure it's not throwing exception.
            """.stripMargin, exception)
          minimumRequired
      }
    }
  }

  def close(state: State): IO[Option[State]] =
    if (state.bytes.isEmpty || !state.hasMinimumHits)
      IO.none
    else
      Block.create(
        headerSize = state.headerSize,
        bytes = state.bytes,
        compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
        blockName = blockName
      ) flatMap {
        compressedOrUncompressedBytes =>
          IO {
            val allocatedBytes = state.bytes.allocatedSize
            state.bytes = compressedOrUncompressedBytes
            state.bytes addInt allocatedBytes //allocated bytes
            state.bytes addInt state.maxProbe
            state.bytes addIntUnsigned state.hit
            state.bytes addIntUnsigned state.miss
            state.bytes addIntUnsigned state.writeAbleLargestValueSize
            if (state.bytes.currentWritePosition > state.headerSize)
              throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
            Some(state)
          }
      }

  def read(offset: Offset, reader: BlockReader[SegmentBlock]): IO[HashIndexBlock] =
    for {
      result <- Block.readHeader(offset = offset, reader = reader)
      allocatedBytes <- result.headerReader.readInt()
      maxProbe <- result.headerReader.readInt()
      hit <- result.headerReader.readIntUnsigned()
      miss <- result.headerReader.readIntUnsigned()
      largestValueSize <- result.headerReader.readIntUnsigned()
    } yield
      HashIndexBlock(
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
    val valuePlusOneBytes = Slice.writeIntUnsigned(valuePlusOne)

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
            totalBlockSpace = state.bytes.allocatedSize,
            headerSize = state.headerSize,
            writeAbleLargestValueSize = state.writeAbleLargestValueSize
          )

        val existing = state.bytes.take(hashIndex, valuePlusOneBytes.size + 2) ////+1 to reserve left 0 byte another +1 not overwrite next 0.
        if (existing.forall(_ == 0)) {
          state.bytes moveWritePosition (hashIndex + 1)
          state.bytes addAll valuePlusOneBytes
          state.hit += 1
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, valueBytes: ${Slice.writeIntUnsigned(valuePlusOne)} = success")
          true
        } else if (existing.head == 0 && existing.dropHead() == valuePlusOneBytes) { //check if value already exists.
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, valueBytes: ${Slice.writeIntUnsigned(valuePlusOne)} = existing")
          state.hit += 1
          true
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, valueBytes: ${Slice.writeIntUnsigned(valuePlusOne)} = failure")
          doWrite(key = key, probe = probe + 1)
        }
      }

    if (state.bytes.allocatedSize == 0)
      IO.`false`
    else
      IO(doWrite(key, 0))
  }

  /**
    * Finds a key in the hash index.
    *
    * @param assertValue performs find or forward fetch from the currently being read sorted index's hash block.
    */
  private[block] def search[R](key: Slice[Byte],
                               blockReader: BlockReader[HashIndexBlock],
                               assertValue: Int => IO[Option[R]]): IO[Option[R]] = {

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val hashIndex = blockReader.block

    @tailrec
    def doFind(probe: Int, checkedHashIndexes: mutable.HashSet[Int]): IO[Option[R]] =
      if (probe >= hashIndex.maxProbe) {
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
              //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe. sortedIndex bytes: $possibleValueBytes")
              if (possibleValueBytes.isEmpty || possibleValueBytes.head != 0) {
                //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe = failure - invalid start offset.")
                doFind(probe + 1, checkedHashIndexes)
              } else {
                val possibleValueWithoutHeader = possibleValueBytes.dropHead()
                possibleValueWithoutHeader.readIntUnsigned() match {
                  case IO.Success(possibleValue) =>
                    //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe, sortedIndex: ${possibleValue - 1} = reading now!")
                    if (possibleValue == 0 || possibleValueWithoutHeader.take(Bytes.sizeOf(possibleValue)).exists(_ == 0))
                      doFind(probe + 1, checkedHashIndexes)
                    else
                      assertValue(possibleValue - 1) match { //assert value removing the 1 added on write.
                        case success @ IO.Success(Some(_)) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe, sortedIndex: ${possibleValue - 1} = success")
                          success

                        case IO.Success(None) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe: sortedIndex: ${possibleValue - 1} = not found")
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

  def search(key: Slice[Byte],
             hashIndexReader: BlockReader[HashIndexBlock],
             sortedIndexReader: BlockReader[SortedIndexBlock],
             valuesReaderReader: Option[BlockReader[ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] = {
    val matcher =
      if (sortedIndexReader.block.hasPrefixCompression)
        KeyMatcher.Get.WhilePrefixCompressed(key)
      else
        KeyMatcher.Get.MatchOnly(key)

    search(
      key = key,
      blockReader = hashIndexReader,
      assertValue =
        sortedIndexOffsetValue =>
          SortedIndexBlock.findAndMatchOrNextPersistent(
            matcher = matcher,
            fromOffset = sortedIndexOffsetValue,
            indexReader = sortedIndexReader,
            valueReader = valuesReaderReader
          )
    )
  }

  implicit object HashIndexBlockUpdater extends BlockUpdater[HashIndexBlock] {
    override def updateOffset(block: HashIndexBlock, start: Int, size: Int): HashIndexBlock =
      block.copy(offset = HashIndexBlock.Offset(start = start, size = size))
  }
}

private[core] case class HashIndexBlock(offset: HashIndexBlock.Offset,
                                        compressionInfo: Option[Block.CompressionInfo],
                                        maxProbe: Int,
                                        hit: Int,
                                        miss: Int,
                                        writeAbleLargestValueSize: Int,
                                        headerSize: Int,
                                        allocatedBytes: Int) extends Block {
  val bytesToReadPerIndex = writeAbleLargestValueSize + 1 //+1 to read header/marker 0 byte.

  val isCompressed = compressionInfo.isDefined

  def isPerfect =
    miss == 0
}