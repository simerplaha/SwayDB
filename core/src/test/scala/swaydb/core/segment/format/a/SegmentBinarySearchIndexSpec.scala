//package swaydb.core.segment.format.a
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.core.TestData._
//import swaydb.core.segment.format.a.index.BinarySearchIndex
//import swaydb.data.IO
//import swaydb.data.slice.Slice
//
//class SegmentBinarySearchIndexSpec extends WordSpec with Matchers {
//
//  "search" in {
//
//    val lines =
//      (1 to 100) map {
//        i =>
//          Slice.writeIntUnsigned(i)
//      }
//
//    val indexBytes = lines.flatten.toSlice
//
//    def getAtIndex(keyToFind: Int)(offset: Int): IO[MatchResult] =
//      indexBytes.take(offset, 1).readIntUnsigned() map {
//        readKey =>
//          println(s"Read key: $readKey")
//          if (keyToFind == readKey)
//            MatchResult.Matched(null)
//          else if (keyToFind > readKey)
//            MatchResult.Stop
//          else
//            MatchResult.Next
//      }
//
//    val got =
//      BinarySearchIndex.find(
//        entriesCount = lines.size,
//        binarySearchIndexStartOffset = 0,
//        byteSizeOfMaxIndexOffset = 1,
//        getAtOffset = getAtIndex(keyToFind = 71)
//      )
//
//    println(got)
//  }
//}
