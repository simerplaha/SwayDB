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
import swaydb.Error.Segment.ErrorHandler
import swaydb.compression.CompressionInternal
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.Numbers._
import swaydb.core.util.{Bytes, CRC32, Functions}
import swaydb.data.config.{IOAction, IOStrategy, RandomKeyIndex, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.{Error, IO}

import scala.annotation.tailrec
import scala.beans.BeanProperty
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
        copyIndex = false,
        blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
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
            copyIndex = false,
            blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.RandomKeyIndex.Enable =>
          Config(
            maxProbe = enable.tries,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            minimumNumberOfHits = enable.minimumNumberOfHits,
            copyIndex = enable.copyKeys,
            allocateSpace = Functions.safe(_.requiredSpace, enable.allocateSpace),
            blockIO = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
            compressions =
              Functions.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(maxProbe: Int,
                    minimumNumberOfKeys: Int,
                    minimumNumberOfHits: Int,
                    copyIndex: Boolean,
                    allocateSpace: RandomKeyIndex.RequiredSpace => Int,
                    blockIO: IOAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  final case class State(var hit: Int,
                         var miss: Int,
                         copyIndex: Boolean,
                         copyIndexWithReferences: Boolean,
                         minimumNumberOfKeys: Int,
                         minimumNumberOfHits: Int,
                         writeAbleLargestValueSize: Int,
                         @BeanProperty var minimumCRC: Long,
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

  def init(keyValues: Iterable[Transient]): Option[HashIndexBlock.State] =
    if (keyValues.size < keyValues.last.hashIndexConfig.minimumNumberOfKeys || keyValues.last.stats.segmentHashIndexSize <= 0) {
      None
    } else {
      val last = keyValues.last

      val copyWithReferences =
        last.hashIndexConfig.copyIndex && (last.stats.segmentHasGroup || last.stats.hasPrefixCompression)

      val writeAbleLargestValueSize =
        if (last.hashIndexConfig.copyIndex)
          last.stats.segmentMaxSortedIndexEntrySize +
            ByteSizeOf.long + //varLong == CRC bytes
            ByteSizeOf.int + //accessIndexOffset
            whenOrZero(copyWithReferences)(ByteSizeOf.boolean) //boolean for isReference.
        else
          Bytes.sizeOf(last.stats.thisKeyValuesAccessIndexOffset + 1)

      val hasCompression = last.hashIndexConfig.compressions(UncompressedBlockInfo(last.stats.segmentHashIndexSize)).nonEmpty

      val headSize =
        headerSize(
          keyCounts = last.stats.segmentUniqueKeysCount,
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          hasCompression = hasCompression
        )

      val optimalBytes =
        optimalBytesRequired(
          keyCounts = last.stats.segmentUniqueKeysCount,
          minimumNumberOfKeys = last.hashIndexConfig.minimumNumberOfKeys,
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          allocateSpace = last.hashIndexConfig.allocateSpace,
          copyIndex = last.hashIndexConfig.copyIndex,
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
            copyIndex = last.hashIndexConfig.copyIndex,
            copyIndexWithReferences = copyWithReferences,
            minimumNumberOfKeys = last.hashIndexConfig.minimumNumberOfKeys,
            minimumNumberOfHits = last.hashIndexConfig.minimumNumberOfHits,
            writeAbleLargestValueSize = writeAbleLargestValueSize,
            minimumCRC = CRC32.disabledCRC,
            headerSize = headSize,
            maxProbe = last.hashIndexConfig.maxProbe,
            _bytes = Slice.create[Byte](optimalBytes),
            compressions =
              //cannot have no compression to begin with a then have compression because that upsets the total bytes required.
              if (hasCompression)
                last.hashIndexConfig.compressions
              else
                _ => Seq.empty
          )
        )
    }

  def headerSize(keyCounts: Int,
                 writeAbleLargestValueSize: Int,
                 hasCompression: Boolean): Int = {
    val headerSize =
      Block.headerSize(hasCompression) +
        ByteSizeOf.int + //allocated bytes
        ByteSizeOf.varInt + //max probe
        ByteSizeOf.boolean + //copyIndex
        ByteSizeOf.boolean + //copyIndexWithReferences
        (Bytes.sizeOf(keyCounts) * 2) + //hit & miss rate
        ByteSizeOf.varLong + //minimumCRC
        Bytes.sizeOf(writeAbleLargestValueSize) //largest value size

    Bytes.sizeOf(headerSize) +
      headerSize
  }

  def optimalBytesRequired(keyCounts: Int,
                           minimumNumberOfKeys: Int,
                           writeAbleLargestValueSize: Int,
                           hasCompression: Boolean,
                           copyIndex: Boolean,
                           allocateSpace: RandomKeyIndex.RequiredSpace => Int): Int =
    if (keyCounts < minimumNumberOfKeys) {
      0
    } else {
      val sizePerKey =
      //+1 to skip left & right 0 start-end markers if it's not copiedIndex
      //+1 to for the last 1.byte entry so that next entry does overwrite previous writes tail 0's
      //the +1 does not need to be accounted in writeAbleLargestValueSize because these markers are just an indication of start and end index entry.
        keyCounts * (writeAbleLargestValueSize + 1)

      val minimumRequired =
        headerSize(
          keyCounts = keyCounts,
          hasCompression = hasCompression,
          writeAbleLargestValueSize = writeAbleLargestValueSize
        ) + sizePerKey

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

  def close(state: State): IO[swaydb.Error.Segment, Option[State]] =
    if (state.bytes.isEmpty || !state.hasMinimumHits)
      IO.none
    else
      Block.block(
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
            state.bytes addIntUnsigned state.maxProbe
            state.bytes addBoolean state.copyIndex
            state.bytes addBoolean state.copyIndexWithReferences
            state.bytes addIntUnsigned state.hit
            state.bytes addIntUnsigned state.miss
            state.bytes addLongUnsigned {
              //CRC can be -1 when HashIndex is not fully copied.
              if (state.minimumCRC == CRC32.disabledCRC)
                0
              else
                state.minimumCRC
            }
            state.bytes addIntUnsigned state.writeAbleLargestValueSize
            if (state.bytes.currentWritePosition > state.headerSize)
              throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition}")
            Some(state)
          }
      }

  def read(header: Block.Header[HashIndexBlock.Offset]): IO[swaydb.Error.Segment, HashIndexBlock] =
    for {
      allocatedBytes <- header.headerReader.readInt()
      maxProbe <- header.headerReader.readIntUnsigned()
      copyIndex <- header.headerReader.readBoolean()
      copyIndexWithReferences <- header.headerReader.readBoolean()
      hit <- header.headerReader.readIntUnsigned()
      miss <- header.headerReader.readIntUnsigned()
      minimumCRC <- header.headerReader.readLongUnsigned()
      largestValueSize <- header.headerReader.readIntUnsigned()
    } yield
      HashIndexBlock(
        offset = header.offset,
        compressionInfo = header.compressionInfo,
        maxProbe = maxProbe,
        copyIndex = copyIndex,
        copyIndexWithReferences = copyIndexWithReferences,
        minimumCRC = minimumCRC,
        hit = hit,
        miss = miss,
        writeAbleLargestValueSize = largestValueSize,
        headerSize = header.headerSize,
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
            state: State): IO[swaydb.Error.Segment, Boolean] = {

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

        val existing = state.bytes.take(hashIndex, valuePlusOneBytes.size + 2) //+1 to reserve left 0 byte another +1 not overwrite next 0.
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
                               reader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                               assertValue: Int => IO[swaydb.Error.Segment, Option[R]]): IO[swaydb.Error.Segment, Option[R]] = {

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val hashIndex = reader.block

    @tailrec
    def doFind(probe: Int, checkedHashIndexes: mutable.HashSet[Int]): IO[swaydb.Error.Segment, Option[R]] =
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
          reader
            .moveTo(index)
            .read(hashIndex.bytesToReadPerIndex) match {
            case IO.Right(possibleValueBytes) =>
              //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe. sortedIndex bytes: $possibleValueBytes")
              if (possibleValueBytes.isEmpty || possibleValueBytes.head != 0) {
                //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe = failure - invalid start offset.")
                doFind(probe + 1, checkedHashIndexes)
              } else {
                val possibleValueWithoutHeader = possibleValueBytes.dropHead()
                possibleValueWithoutHeader.readIntUnsignedWithByteSize() match {
                  case IO.Right((possibleValue, bytesRead)) =>
                    //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe, sortedIndex: ${possibleValue - 1} = reading now!")
                    if (possibleValue == 0 || possibleValueWithoutHeader.take(bytesRead).exists(_ == 0))
                      doFind(probe + 1, checkedHashIndexes)
                    else
                      assertValue(possibleValue - 1) match { //assert value removing the 1 added on write.
                        case success @ IO.Right(Some(_)) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe, sortedIndex: ${possibleValue - 1} = success")
                          success

                        case IO.Right(None) =>
                          //println(s"Key: ${key.readInt()}: read hashIndex: ${index + hashIndex.headerSize} probe: $probe: sortedIndex: ${possibleValue - 1} = not found")
                          doFind(probe + 1, checkedHashIndexes += index)

                        case IO.Left(error) =>
                          IO.Left(error)
                      }

                  case IO.Left(error) =>
                    IO.Left[swaydb.Error.Segment, Option[R]](value = error)
                }
              }

            case IO.Left(error) =>
              IO.Left(error)
          }
      }

    doFind(
      probe = 0,
      checkedHashIndexes = mutable.HashSet.empty
    )
  }

  def writeCopied(key: Slice[Byte],
                  indexEntry: Slice[Byte],
                  thisKeyValuesAccessOffset: Int,
                  state: State): IO[swaydb.Error.Segment, Boolean] =
    writeCopied(
      key = key,
      value = indexEntry,
      thisKeyValuesAccessOffset = thisKeyValuesAccessOffset,
      isReference = false,
      state = state
    )

  def writeCopied(key: Slice[Byte],
                  indexOffset: Int,
                  state: State): IO[swaydb.Error.Segment, Boolean] =
    writeCopied(
      key = key,
      value = Slice.writeIntUnsigned(indexOffset),
      thisKeyValuesAccessOffset = -1,
      isReference = true,
      state = state
    )

  /**
   * Writes full copy of the index entry within HashIndex.
   */
  private def writeCopied(key: Slice[Byte],
                          value: Slice[Byte],
                          thisKeyValuesAccessOffset: Int,
                          isReference: Boolean,
                          state: State): IO[swaydb.Error.Segment, Boolean] = {

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    //[CRC|Option[isRef]|Option[accessOffset]|valuesBytes]
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

        val existing = state.bytes.take(hashIndex, state.writeAbleLargestValueSize)

        if (existing.forall(_ == 0)) {
          state.bytes moveWritePosition hashIndex
          val crc = CRC32.forBytes(value)
          //write as unsignedLong to avoid writing any zeroes.
          state.bytes addLongUnsigned crc

          if (state.copyIndexWithReferences)
            state.bytes addBoolean isReference

          if (!isReference)
            state.bytes addIntUnsigned thisKeyValuesAccessOffset

          state.bytes addAll value

          if (value.last == 0) //if the last byte is 0 add one to avoid next write overwriting this entry's last byte.
            state.bytes addByte Bytes.one

          if (state.minimumCRC == CRC32.disabledCRC)
            state setMinimumCRC crc
          else
            state setMinimumCRC (crc min state.minimumCRC)

          state.hit += 1
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, crcBytes: ${Slice.writeLongUnsigned(crc)}, value: $value, crc: $crc, isReference: $isReference = success")
          true
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, value: $value, isReference: $isReference = failure")
          doWrite(key = key, probe = probe + 1)
        }
      }

    assert(!isReference || state.copyIndexWithReferences) //if isReference is true, state should have copyIndexWithReferences set to true.

    if (state.bytes.allocatedSize == 0)
      IO.`false`
    else
      IO(doWrite(key, 0))
  }

  /**
   * Parses bytes written by [[writeCopied]] without CRC bytes.
   *
   * @return valueBytes, isReference and accessIndexOffset.
   */
  private def parseCopiedValuesBytes(valueBytesWithoutCRC: Slice[Byte],
                                     copyIndexWithReferences: Boolean): IO[Error.Segment, (Slice[Byte], Boolean, Int)] = {
    //if references were also being written then check if the current is reference or fullCopy.
    val (isReference, remainingWithoutCRCAndIsReference) =
      if (copyIndexWithReferences) //if references is enabled then it should also contain a boolean value which indicates if this entry is reference or copy.
        (valueBytesWithoutCRC.readBoolean(), valueBytesWithoutCRC.drop(ByteSizeOf.boolean))
      else
        (false, valueBytesWithoutCRC)

    //[CRC|Option[isRef]|Option[accessOffset]|valuesBytes]
    remainingWithoutCRCAndIsReference.readIntUnsignedWithByteSize() flatMap {
      case (entrySizeOrAccessIndexOffsetEntry, entrySizeOrAccessIndexOffsetEntryByteSize) => //this will be entrySize/sortedIndexOffset if it's a reference else it will be thisKeyValuesAccessIndexOffset.
        //it's a reference so there is no accessIndexOffset therefore this value is reference offset.
        if (isReference) {
          val valueBytes = remainingWithoutCRCAndIsReference.take(entrySizeOrAccessIndexOffsetEntryByteSize)
          IO.Right((valueBytes, isReference, -1)) //it's a reference there will be no accessIndexOffset.
        } else { //else this is accessIndexOffset and remaining is indexEntry bytes.
          remainingWithoutCRCAndIsReference.drop(entrySizeOrAccessIndexOffsetEntryByteSize).readIntUnsignedWithByteSize() map {
            case (entrySize, entryByteSize) =>
              val valueBytes =
                remainingWithoutCRCAndIsReference
                  .drop(entrySizeOrAccessIndexOffsetEntryByteSize)
                  .take(entrySize + entryByteSize)

              (valueBytes, isReference, entrySizeOrAccessIndexOffsetEntry)
          }
        }
    }
  }

  /**
   * Finds a key in the hash index.
   *
   * @param assertValue performs find or forward fetch from the currently being read sorted index's hash block.
   */
  private[block] def searchCopied[R](key: Slice[Byte],
                                     reader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                                     assertValue: (Slice[Byte], Int, Boolean) => IO[swaydb.Error.Segment, Option[R]]): IO[swaydb.Error.Segment, Option[R]] = {

    val hash = key.##
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val block = reader.block

    @tailrec
    def doFind(probe: Int): IO[swaydb.Error.Segment, Option[R]] =
      if (probe >= block.maxProbe) {
        IO.none
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            totalBlockSpace = block.allocatedBytes,
            headerSize = block.headerSize,
            writeAbleLargestValueSize = block.writeAbleLargestValueSize
          ) - block.headerSize //remove headerSize since the blockReader points to the hashIndex's start offset.

        //[CRC|Option[isRef]|Option[accessOffset]|valuesBytes]

        reader
          .moveTo(hashIndex)
          .read(block.writeAbleLargestValueSize) match {
          case IO.Right(possibleValueBytes: Slice[Byte]) =>
            //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe. sortedIndex bytes: $possibleValueBytes")
            if (possibleValueBytes.isEmpty || possibleValueBytes.size == 1) {
              //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe = failure - invalid start offset.")
              doFind(probe + 1)
            } else {
              //writeAbleLargestValueSize could also read extra tail bytes so fetch only the bytes that are specific to the indexEntry.
              //Read the crc and then read the indexEntry's entrySize and fetch on the indexEntry bytes and then check CRC.
              possibleValueBytes.readUnsignedLongWithByteSize() match {
                case IO.Right((crc, crcByteSize)) =>
                  if (crc < reader.block.minimumCRC)
                    doFind(probe + 1)
                  else
                    parseCopiedValuesBytes(
                      valueBytesWithoutCRC = possibleValueBytes drop crcByteSize,
                      copyIndexWithReferences = reader.block.copyIndexWithReferences
                    ) match {
                      case IO.Right((valueBytes, isReference, accessIndexOffset)) => //valueBytes can either be offset or the indexEntry itself.
                        if (crc == CRC32.forBytes(valueBytes))
                          assertValue(valueBytes, accessIndexOffset, isReference) match { //assert value.
                            case success @ IO.Right(Some(_)) =>
                              //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe, entryBytes: $valueBytes = success")
                              success

                            case IO.Right(None) =>
                              //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe: entryBytes: $valueBytes = not found")
                              doFind(probe + 1)

                            case IO.Left(error) =>
                              IO.Left(error)
                          }
                        else
                          doFind(probe + 1)

                      case IO.Left(error) =>
                        error.exception match {
                          case _: ArrayIndexOutOfBoundsException =>
                            doFind(probe + 1)

                          case exception: IllegalArgumentException if exception.getMessage.contains("requirement failed") =>
                            doFind(probe + 1)

                          case _ =>
                            IO.Left(error)
                        }
                    }

                case IO.Left(error) =>
                  error.exception match {
                    case _: ArrayIndexOutOfBoundsException =>
                      doFind(probe + 1)

                    case _ =>
                      IO.Left(error)
                  }
              }
            }

          case IO.Left(error) =>
            IO.Left(error)
        }
      }

    doFind(probe = 0)
  }

  def search(key: Slice[Byte],
             hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent]] = {
    val matcher =
      if (sortedIndexReader.block.hasPrefixCompression)
        KeyMatcher.Get.WhilePrefixCompressed(key)
      else
        KeyMatcher.Get.SeekOne(key)

    if (hashIndexReader.block.copyIndex)
      searchCopied(
        key = key,
        reader = hashIndexReader,
        assertValue =
          (referenceOrIndexEntry: Slice[Byte], accessIndexOffset: Int, isReference: Boolean) =>
            if (isReference)
              referenceOrIndexEntry.readIntUnsigned() flatMap {
                index =>
                  SortedIndexBlock.findAndMatchOrNextPersistent(
                    matcher = matcher,
                    fromOffset = index,
                    indexReader = sortedIndexReader,
                    valuesReader = valuesReader
                  )
              }
            else
              SortedIndexBlock.parseAndMatch( //do no perform read for next key-value since this indexReader only contains bytes for the current read indexEntry.
                matcher = matcher,
                fromOffset = 0,
                overwriteNextIndexOffset = Some(accessIndexOffset + referenceOrIndexEntry.size),
                indexReader =
                  UnblockedReader(
                    block =
                      SortedIndexBlock(
                        offset = SortedIndexBlock.Offset(0, referenceOrIndexEntry.size),
                        enableAccessPositionIndex = sortedIndexReader.block.enableAccessPositionIndex,
                        hasPrefixCompression = false,
                        normaliseForBinarySearch = sortedIndexReader.block.normaliseForBinarySearch,
                        isPreNormalised = sortedIndexReader.block.isPreNormalised,
                        headerSize = 0,
                        segmentMaxIndexEntrySize = sortedIndexReader.block.segmentMaxIndexEntrySize,
                        compressionInfo = None
                      ),
                    bytes = referenceOrIndexEntry
                  ),
                valuesReader = valuesReader
              )
      )
    else
      search(
        key = key,
        reader = hashIndexReader,
        assertValue =
          (sortedIndexOffsetValue: Int) =>
            SortedIndexBlock.findAndMatchOrNextPersistent(
              matcher = matcher,
              fromOffset = sortedIndexOffsetValue,
              indexReader = sortedIndexReader,
              valuesReader = valuesReader
            )
      )
  }

  implicit object HashIndexBlockOps extends BlockOps[HashIndexBlock.Offset, HashIndexBlock] {
    override def updateBlockOffset(block: HashIndexBlock, start: Int, size: Int): HashIndexBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      HashIndexBlock.Offset(start = start, size = size)

    override def readBlock(header: Block.Header[Offset]): IO[swaydb.Error.Segment, HashIndexBlock] =
      HashIndexBlock.read(header)
  }
}

private[core] case class HashIndexBlock(offset: HashIndexBlock.Offset,
                                        compressionInfo: Option[Block.CompressionInfo],
                                        maxProbe: Int,
                                        copyIndex: Boolean,
                                        copyIndexWithReferences: Boolean,
                                        minimumCRC: Long,
                                        hit: Int,
                                        miss: Int,
                                        writeAbleLargestValueSize: Int,
                                        headerSize: Int,
                                        allocatedBytes: Int) extends Block[HashIndexBlock.Offset] {
  val bytesToReadPerIndex = writeAbleLargestValueSize + 1 //+1 to read header/marker 0 byte.

  val isCompressed = compressionInfo.isDefined

  def isPerfect =
    miss == 0
}