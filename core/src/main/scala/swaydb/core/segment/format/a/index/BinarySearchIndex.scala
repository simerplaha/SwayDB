package swaydb.core.segment.format.a.index

import swaydb.core.data.Persistent
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.MatchResult
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

object BinarySearchIndex {

  val formatId: Byte = 1.toByte

  object State {
    def apply(largestValue: Int,
              valuesCount: Int,
              buildFullBinarySearchIndex: Boolean): State =
      State(
        largestValue = largestValue,
        valuesCount = 0,
        buildFullBinarySearchIndex = buildFullBinarySearchIndex,
        bytes = Slice.create[Byte](optimalBytesRequired(largestValue, valuesCount))
      )

    def apply(largestValue: Int,
              valuesCount: Int,
              buildFullBinarySearchIndex: Boolean,
              bytes: Slice[Byte]): State =
      new State(
        byteSizeOfLargestValue = Bytes.sizeOf(largestValue),
        buildFullBinarySearchIndex = buildFullBinarySearchIndex,
        headerSize =
          optimalHeaderSize(
            largestValue = largestValue,
            valuesCount = valuesCount
          ),
        _valuesCount = valuesCount,
        bytes = bytes
      )
  }

  case class Header(valuesCount: Int,
                    headerSize: Int,
                    byteSizeOfLargestValue: Int,
                    isFullBinarySearchIndex: Boolean)

  case class State(byteSizeOfLargestValue: Int,
                   var _valuesCount: Int,
                   headerSize: Int,
                   buildFullBinarySearchIndex: Boolean,
                   bytes: Slice[Byte]) {

    def valuesCount =
      _valuesCount

    def valuesCount_=(count: Int) =
      _valuesCount = count

    def incrementEntriesCount() =
      _valuesCount += 1
  }

  def optimalBytesRequired(largestValue: Int,
                           valuesCount: Int): Int =
    optimalHeaderSize(
      largestValue = largestValue,
      valuesCount = valuesCount
    ) + (Bytes.sizeOf(largestValue) * valuesCount)

  def optimalHeaderSize(largestValue: Int,
                        valuesCount: Int): Int = {

    val headerSize =
      ByteSizeOf.byte + //formatId
        Bytes.sizeOf(valuesCount) +
        Bytes.sizeOf(largestValue) +
        ByteSizeOf.boolean // buildFullBinarySearchIndex

    Bytes.sizeOf(headerSize) +
      headerSize
  }

  def writeHeader(state: State): IO[Unit] =
    IO {
      state.bytes moveWritePosition 0
      state.bytes addIntUnsigned state.headerSize
      state.bytes add formatId
      state.bytes addIntUnsigned state.valuesCount
      state.bytes addIntUnsigned state.byteSizeOfLargestValue
      state.bytes addBoolean state.buildFullBinarySearchIndex
    }

  def readHeader(startOffset: Int,
                 reader: Reader) =
    reader
      .moveTo(startOffset)
      .readIntUnsigned()
      .flatMap {
        headerSize =>
          reader
            .read(headerSize)
            .flatMap {
              headBytes =>
                val headerReader = Reader(headBytes)
                headerReader
                  .get()
                  .flatMap {
                    formatId =>
                      if (formatId != this.formatId)
                        IO.Failure(new Exception(s"Invalid formatID: $formatId"))
                      else
                        for {
                          valuesCount <- headerReader.readIntUnsigned()
                          byteSizeOfLargestValue <- headerReader.readIntUnsigned()
                          isFullBinarySearchIndex <- headerReader.readBoolean()
                        } yield
                          Header(
                            valuesCount = valuesCount,
                            headerSize = headerSize,
                            byteSizeOfLargestValue = byteSizeOfLargestValue,
                            isFullBinarySearchIndex = isFullBinarySearchIndex
                          )
                  }
            }
      }

  def write(value: Int,
            state: State): IO[Unit] =
    IO {
      if (state.bytes.written == 0) state.bytes moveWritePosition state.headerSize

      if (state.byteSizeOfLargestValue <= ByteSizeOf.int)
        state.bytes addIntUnsigned value
      else
        state.bytes addInt value

      state.incrementEntriesCount()
    }

  def find(startOffset: Int,
           footer: Header,
           assertValue: Int => IO[MatchResult]): IO[Option[Persistent]] = {

    val minimumOffset = startOffset + footer.headerSize

    @tailrec
    def hop(start: Int, end: Int): IO[Option[Persistent]] = {
      val mid = start + (end - start) / 2

      val valueOffset = minimumOffset + (mid * footer.byteSizeOfLargestValue)
      if (start > end)
        IO.none
      else
        assertValue(valueOffset) match {
          case IO.Success(value) =>
            value match {
              case MatchResult.Matched(result) =>
                IO.Success(Some(result))

              case MatchResult.Next =>
                hop(start, mid - 1)

              case MatchResult.Stop =>
                hop(mid + 1, end)
            }
          case IO.Failure(error) =>
            IO.Failure(error)
        }
    }

    hop(start = 0, end = footer.valuesCount - 1)
  }
}

