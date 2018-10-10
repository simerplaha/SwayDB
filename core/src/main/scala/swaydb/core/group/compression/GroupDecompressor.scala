/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.group.compression

import java.util.concurrent.atomic.AtomicBoolean

import swaydb.compression.DecompressorInternal
import swaydb.core.group.compression.data.{GroupHeader, ValueInfo}
import swaydb.core.io.reader.{GroupReader, Reader}
import swaydb.core.segment.SegmentException
import swaydb.core.segment.format.one.SegmentFooter
import swaydb.core.util.TryUtil
import swaydb.data.slice.Reader

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

private[core] case class GroupDecompressor(private val compressedGroupReader: Reader,
                                           groupStartOffset: Int) {

  private val maxTimesToTryDecompress = 10000
  @volatile private var groupHeader: GroupHeader = _ //header for compressed Segment info
  @volatile private var decompressedIndexReader: GroupReader = _ //reader for keys bytes that contains function to read value bytes
  @volatile private var decompressedValuesReader: Reader = _ //value bytes reader.

  private val busyReadingHeader = new AtomicBoolean(false) //atomic boolean for reading header
  private val busyIndexDecompressing = new AtomicBoolean(false) //atomic boolean for decompressing key bytes
  private val busyValueDecompressing = new AtomicBoolean(false) //atomic boolean for decompressing value bytes

  /**
    * Compressed values do not need to be read & decompressed by multiple thread concurrently. The responsibility
    * of decompression is delegated to the first thread and the others will read from the same thread-safe [[decompressedValuesReader]]
    */

  @tailrec
  private def valuesDecompressor(headerSize: Int,
                                 valuesCompressedLength: Int,
                                 valuesDecompressedLength: Int,
                                 valuesCompression: DecompressorInternal,
                                 maxTimesToTry: Int): Try[Reader] =
    if (decompressedValuesReader != null) //if values are already decompressed, return values reader!
      Try(decompressedValuesReader.copy())
    else if (maxTimesToTry <= 0)
      Failure(SegmentException.BusyDecompressionValues)
    else if (busyValueDecompressing.compareAndSet(false, true)) //start values decompression.
      compressedGroupReader.copy().moveTo(groupStartOffset + headerSize + 1).read(valuesCompressedLength) flatMap { //move to the head of the compressed and read compressed value bytes.
        compressedValueBytes =>
          valuesCompression.decompress(compressedValueBytes, valuesDecompressedLength) map { //do decompressing
            valueDecompressedBytes =>
              decompressedValuesReader = Reader(valueDecompressedBytes) //set decompressed bytes so other threads can read concurrently.
              decompressedValuesReader.copy() //return new decompressed value reader. Do a copy to be thread-safe.
          }
      } recoverWith {
        case ex =>
          busyValueDecompressing.set(false) //free decompressor
          Failure(ex)
      }
    else //currently being decompressed by another thread. Try again!
      valuesDecompressor(headerSize, valuesCompressedLength, valuesDecompressedLength, valuesCompression, maxTimesToTry - 1)

  private def readHeader() =
    for {
      compressedReader <- Try(compressedGroupReader.copy().moveTo(groupStartOffset))
      headerSize <- compressedReader.readIntUnsigned()
      header <- compressedReader.read(headerSize) map (Reader(_))
      //format id ignored. Currently there is only one format.
      _ <- header.readIntUnsigned()
      hasRange <- header.readBoolean()
      //this Compression instance is used for decompressing only so minCompressionPercentage is irrelevant
      keysCompression <- header.readIntUnsigned() flatMap (DecompressorInternal(_))
      keyValueCount <- header.readIntUnsigned()
      bloomFilterItemsCount <- header.readIntUnsigned()
      indexDecompressedLength <- header.readIntUnsigned()
      indexCompressedLength <- header.readIntUnsigned()
      hasValues <- header.hasMore //read values related header bytes only if header contains more data.
      //this Compression instance is used for decompressing only so minCompressionPercentage is irrelevant
      valuesCompression <- if (hasValues) header.readIntUnsigned().flatMap(id => DecompressorInternal(id).map(Some(_))) else TryUtil.successNone
      valuesDecompressedLength <- if (hasValues) header.readIntUnsigned() else TryUtil.successZero
      valuesCompressedLength <- if (hasValues) header.readIntUnsigned() else TryUtil.successZero
    } yield {
      GroupHeader(
        headerSize = headerSize,
        hasRange = hasRange,
        indexDecompressor = keysCompression,
        keyValueCount = keyValueCount,
        bloomFilterItemsCount = bloomFilterItemsCount,
        indexDecompressedLength = indexDecompressedLength,
        indexCompressedLength = indexCompressedLength,
        compressedStartIndexOffset = groupStartOffset + headerSize + valuesCompressedLength + 1,
        compressedEndIndexOffset = groupStartOffset + headerSize + valuesCompressedLength + indexCompressedLength,
        decompressedStartIndexOffset = groupStartOffset + headerSize + valuesDecompressedLength + 1,
        decompressedEndIndexOffset = groupStartOffset + headerSize + valuesDecompressedLength + indexDecompressedLength,
        valueInfo =
          valuesCompression map {
            valuesCompression =>
              ValueInfo(
                valuesDecompressor = valuesCompression,
                valuesDecompressedLength = valuesDecompressedLength,
                valuesCompressedLength = valuesCompressedLength
              )
          }
      )
    }

  @tailrec
  private def header(maxTimesToTry: Int): Try[GroupHeader] =
    if (groupHeader != null)
      Success(groupHeader)
    else if (maxTimesToTry <= 0)
      Failure(SegmentException.BusyReadingHeader)
    else if (busyReadingHeader.compareAndSet(false, true))
      readHeader() map {
        header =>
          groupHeader = header
          header
      } recoverWith {
        case ex: Exception =>
          busyReadingHeader.set(false)
          Failure(ex)
      }
    else
      header(maxTimesToTry - 1)

  def header(): Try[GroupHeader] =
    header(maxTimesToTryDecompress)

  private def decompressor(): Try[GroupReader] =
    for {
      header <- header()
      keyCompressedBytes <- compressedGroupReader.copy().moveTo(header.compressedStartIndexOffset).read(header.indexCompressedLength)
      keysDecompressedBytes <- header.indexDecompressor.decompress(keyCompressedBytes, header.indexDecompressedLength)
    } yield {
      //create and set the footer for SegmentReader.
      //create a GroupReader with keys decompressed and a lazy valueDecompressor function.
      new GroupReader(
        decompressedValuesSize = header.valueInfo.map(_.valuesDecompressedLength).getOrElse(0),
        startIndexOffset = header.decompressedStartIndexOffset,
        endIndexOffset = header.decompressedEndIndexOffset,
        valuesDecompressor =
          () =>
            header.valueInfo map { //values can be empty.
              valueInfo =>
                valuesDecompressor(
                  headerSize = header.headerSize,
                  valuesCompressedLength = valueInfo.valuesCompressedLength,
                  valuesDecompressedLength = valueInfo.valuesDecompressedLength,
                  valuesCompression = valueInfo.valuesDecompressor,
                  maxTimesToTry = maxTimesToTryDecompress
                )
            } getOrElse TryUtil.emptyReader, //Return empty reader if values are empty
        indexReader = Reader(keysDecompressedBytes)
      )
    }

  /**
    * Compressed keys do not need to be read & decompressed by multiple threads concurrently. The responsibility
    * of decompression is delegated to the first thread and the others will read from the same thread-safe [[decompressedIndexReader]].
    *
    * This function returns a [[Reader]] of type [[GroupReader]] which contains decompressed keys bytes (index entries).
    * Values are decompressed on demand.
    *
    * It also initialises [[valuesDecompressor]] partial function which when invoked decompresses values in a thread-safe manner.
    */
  @tailrec
  private def decompress(timesToTry: Int): Try[Reader] =
    if (decompressedIndexReader != null) //if keys are already decompressed, return!
      Try(decompressedIndexReader.copy())
    else if (timesToTry <= 0)
      Failure(SegmentException.BusyDecompressingIndex)
    else if (busyIndexDecompressing.compareAndSet(false, true)) //start decompressing keys.
      decompressor() map {
        reader =>
          decompressedIndexReader = reader
          reader.copy()
      } recoverWith {
        case exception =>
          busyIndexDecompressing.set(false) //free decompressor
          Failure(exception)
      }
    else
      decompress(timesToTry - 1) //currently being decompressed by another thread. Try again!

  def decompress(): Try[Reader] =
    decompress(maxTimesToTryDecompress)

  def footer(): Try[SegmentFooter] =
    header().map(_.footer)

  def reader(): Try[Reader] =
    decompress()

  def isHeaderDecompressed(): Boolean =
    groupHeader != null

  def isIndexDecompressed(): Boolean =
    decompressedIndexReader != null

  def isValueDecompressed(): Boolean =
    decompressedValuesReader != null

  def uncompress(): GroupDecompressor =
    this.copy(compressedGroupReader = compressedGroupReader.copy())

  //GroupDecompressor does not need to be a case class. This is just a helper function for test cases.
  override def equals(that: scala.Any): Boolean =
    that match {
      case other: GroupDecompressor =>
        this.compressedGroupReader.equals(other.compressedGroupReader)

      case _ =>
        false
    }
}