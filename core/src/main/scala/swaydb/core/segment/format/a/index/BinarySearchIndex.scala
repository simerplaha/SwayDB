package swaydb.core.segment.format.a.index

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.MatchResult
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

object BinarySearchIndex {

  val formatId: Byte = 1.toByte

  object State {
    def apply(maxIndexOffset: Int,
              entriesCount: Int,
              bytes: Slice[Byte]): State =
      new State(
        byteSizeOfMaxIndexOffset = Bytes.sizeOf(maxIndexOffset),
        _entriesCount = entriesCount,
        bytes = bytes
      )
  }

  case class Footer(entriesCount: Int,
                    binarySearchIndexStartOffset: Int,
                    byteSizeOfMaxIndexOffset: Int)

  case class State(byteSizeOfMaxIndexOffset: Int,
                   var _entriesCount: Int,
                   bytes: Slice[Byte]) {

    def entriesCount =
      _entriesCount

    def entriesCount_=(count: Int) =
      _entriesCount = count

    def incrementEntriesCount() =
      _entriesCount += 1
  }

  def optimalBytesRequired(maxIndexOffset: Int,
                           keysCount: Int): Int = {
    val maxIndexOffsetBytesCount = Bytes.sizeOf(maxIndexOffset)

    ByteSizeOf.byte + //formatId
      (maxIndexOffsetBytesCount * keysCount) +
      Bytes.sizeOf(keysCount) +
      maxIndexOffsetBytesCount
  }

  def writeFooter(state: State): Unit = {
    state.bytes add formatId
    state.bytes addIntUnsigned state.entriesCount
    state.bytes addIntUnsigned state.byteSizeOfMaxIndexOffset
  }

  def write(indexOffset: Int,
            state: State): IO[Unit] =
    IO {
      if (state.byteSizeOfMaxIndexOffset <= 4)
        state.bytes addIntUnsigned indexOffset
      else
        state.bytes addInt indexOffset

      state.incrementEntriesCount()
    }

  def find(footer: Footer,
           getAtOffset: Int => IO[MatchResult]): IO[Option[Persistent]] = {

    @tailrec
    def hop(start: Int, end: Int): IO[Option[Persistent]] = {
      val mid = start + (end - start) / 2

      if (start > end)
        IO.none
      else
        getAtOffset((mid * footer.byteSizeOfMaxIndexOffset) + footer.binarySearchIndexStartOffset) match {
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

    hop(start = 0, end = footer.entriesCount - 1)
  }
}

