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

package swaydb.core.group.compression

import swaydb.compression.DecompressorInternal
import swaydb.core.group.compression.data.{GroupHeader, ValueInfo}
import swaydb.core.io.reader.{GroupReader, Reader}
import swaydb.core.segment.format.a.{SegmentFooter, SegmentHashIndex, SegmentReader}
import swaydb.data.slice.Reader
import swaydb.data.{IO, Reserve}

private[core] case class GroupDecompressor(private val compressedGroupReader: Reader,
                                           groupStartOffset: Int) {

  @volatile private var groupHeader: GroupHeader = _ //header for compressed Segment info
  @volatile private var decompressedIndexReader: GroupReader = _ //reader for keys bytes that contains function to read value bytes
  @volatile private var decompressedValuesReader: Reader = _ //value bytes reader.

  private val busyReadingHeader = Reserve[Unit]() //atomic boolean for reading header
  private val busyIndexDecompressing = Reserve[Unit]() //atomic boolean for decompressing key bytes
  private val busyValueDecompressing = Reserve[Unit]() //atomic boolean for decompressing value bytes

  /**
    * Compressed values do not need to be read & decompressed by multiple thread concurrently. The responsibility
    * of decompression is delegated to the first thread and the others will read from the same thread-safe [[decompressedValuesReader]]
    */

  private def valuesDecompressor(headerSize: Int,
                                 valuesCompressedLength: Int,
                                 valuesDecompressedLength: Int,
                                 valuesCompression: DecompressorInternal): IO[Reader] =
    if (decompressedValuesReader != null) //if values are already decompressed, return values reader!
      IO(decompressedValuesReader.copy())
    else if (Reserve.setBusyOrGet((), busyValueDecompressing).isEmpty) //start values decompression.
      try
        compressedGroupReader.copy().moveTo(groupStartOffset + headerSize + 1).read(valuesCompressedLength) flatMap { //move to the head of the compressed and read compressed value bytes.
          compressedValueBytes =>
            valuesCompression.decompress(compressedValueBytes, valuesDecompressedLength) map { //do decompressing
              valueDecompressedBytes =>
                decompressedValuesReader = Reader(valueDecompressedBytes) //set decompressed bytes so other threads can read concurrently.
                decompressedValuesReader.copy() //return new decompressed value reader. Do a copy to be thread-safe.
            }
        }
      finally
        Reserve.setFree(busyValueDecompressing)
    else //currently being decompressed by another thread. IO again!
      IO.Failure(IO.Error.DecompressingValues(busyValueDecompressing))

  private def readHeader() =
    for {
      compressedReader <- IO(compressedGroupReader.copy().moveTo(groupStartOffset))
      headerSize <- compressedReader.readIntUnsigned()
      header <- compressedReader.read(headerSize) map (Reader(_))
      //format id ignored. Currently there is only one format.
      _ <- header.readIntUnsigned()
      hasRange <- header.readBoolean()
      hasPut <- header.readBoolean()
      //this Compression instance is used for decompressing only so minCompressionPercentage is irrelevant
      keysCompression <- header.readIntUnsigned() flatMap (DecompressorInternal(_))
      keyValueCount <- header.readIntUnsigned()
      bloomFilterItemsCount <- header.readIntUnsigned()
      indexDecompressedLength <- header.readIntUnsigned()
      indexCompressedLength <- header.readIntUnsigned()
      hashIndexDecompressedLength <- header.readIntUnsigned()
      hasValues <- header.hasMore //read values related header bytes only if header contains more data.
      //this Compression instance is used for decompressing only so minCompressionPercentage is irrelevant
      valuesCompression <- if (hasValues) header.readIntUnsigned().flatMap(id => DecompressorInternal(id).map(Some(_))) else IO.none
      valuesDecompressedLength <- if (hasValues) header.readIntUnsigned() else IO.zero
      valuesCompressedLength <- if (hasValues) header.readIntUnsigned() else IO.zero
    } yield {
      GroupHeader(
        headerSize = headerSize,
        hasRange = hasRange,
        hasPut = hasPut,
        indexDecompressor = keysCompression,
        keyValueCount = keyValueCount,
        bloomFilterItemsCount = bloomFilterItemsCount,
        indexDecompressedLength = indexDecompressedLength,
        indexCompressedLength = indexCompressedLength,
        compressedStartIndexOffset = groupStartOffset + headerSize + valuesCompressedLength + 1,
        compressedEndIndexOffset = groupStartOffset + headerSize + valuesCompressedLength + indexCompressedLength,
        decompressedStartIndexOffset = groupStartOffset + headerSize + valuesDecompressedLength + 1,
        decompressedEndIndexOffset = groupStartOffset + headerSize + valuesDecompressedLength + indexDecompressedLength,
        hashIndexStartOffset = groupStartOffset + headerSize + valuesCompressedLength + indexCompressedLength + 1,
        hashIndexEndOffset = groupStartOffset + headerSize + valuesCompressedLength + indexCompressedLength + hashIndexDecompressedLength,
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

  def header(): IO[GroupHeader] =
    if (groupHeader != null)
      IO.Success(groupHeader)
    else if (Reserve.setBusyOrGet((), busyReadingHeader).isEmpty)
      try
        readHeader() map {
          header =>
            groupHeader = header
            header
        }
      finally
        Reserve.setFree(busyReadingHeader)
    else
      IO.Failure(IO.Error.ReadingHeader(busyReadingHeader))

  private def decompressor(): IO[GroupReader] =
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
                  valuesCompression = valueInfo.valuesDecompressor
                )
            } getOrElse IO.emptyReader, //Return empty reader if values are empty
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
  def decompress(): IO[Reader] =
    if (decompressedIndexReader != null) //if keys are already decompressed, return!
      IO(decompressedIndexReader.copy())
    else if (Reserve.setBusyOrGet((), busyIndexDecompressing).isEmpty) //start decompressing keys.
      try
        decompressor() map {
          reader =>
            decompressedIndexReader = reader
            reader.copy()
        }
      finally
        Reserve.setFree(busyIndexDecompressing)
    else
      IO.Failure(IO.Error.DecompressingIndex(busyIndexDecompressing))

  def footer(): IO[SegmentFooter] =
    header().map(_.footer)

  def hashIndexHeader(): IO[SegmentHashIndex.Header] =
    footer() flatMap {
      footer =>
        SegmentReader.readHashIndexHeader(
          reader = compressedGroupReader.copy(),
          footer = footer
        )
    }

  def reader(): IO[Reader] =
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
