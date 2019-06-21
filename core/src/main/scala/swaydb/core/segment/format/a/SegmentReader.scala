/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.segment.format.a.index.{HashIndex, SortedIndex}
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.getFromHashIndex).
  * Eventually need to re-factor this code to use for comprehension.
  *
  * Leaving as it is now because it's easier to read.
  */
private[core] object SegmentReader extends LazyLogging {

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
  //    SegmentFooter.read(reader) flatMap {
  //      footer =>
  //        readHashIndexHeader(reader, footer) flatMap {
  //          header =>
  //            get(
  //              matcher = matcher,
  //              startFrom = startFrom,
  //              reader = reader,
  //              hashIndexHeader = ???,
  //              footer = footer
  //            )
  //        }
  //    }
    ???

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    SegmentFooter.read(reader) flatMap (lower(matcher, startFrom, reader, _))

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    SegmentFooter.read(reader) flatMap (higher(matcher, startFrom, reader, _))

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader,
            footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
  //    SortedIndex.find(
  //      matcher = matcher,
  //      startFrom = startFrom,
  //      reader = reader,
  //      footer = footer
  //    )
    ???

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader,
             footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
  //    SortedIndex.find(
  //      matcher = matcher,
  //      startFrom = startFrom,
  //      reader = reader,
  //      footer = footer
  //    )
    ???

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader,
          hashIndexHeader: Option[(HashIndex.Header, HashIndex.Offset)],
          footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    ???
  //    hashIndexHeader map {
  //      case (hashIndexHeader, hashIndexOffset) =>
  //        getFromHashIndex(
  //          matcher = matcher.toNextPrefixCompressedMatcher,
  //          reader = reader,
  //          header = hashIndexHeader,
  //          hashIndexOffset = hashIndexOffset,
  //          footer = footer
  //        ) flatMap {
  //          found =>
  //            //if the hashIndex hit rate is perfect means the key definitely does not exist.
  //            //so either the bloomFilter was disabled or it returned a false positive.
  //            if (found.isEmpty && hashIndexHeader.miss == 0 && ((footer.hasRange && footer.binarySearchIndexOffset.isDefined) || !footer.hasRange))
  //              IO.none
  //            else //still not sure go ahead with walk find.
  //              SortedIndex.find(
  //                matcher = matcher,
  //                //todo compare and start from nearest read key-value.
  //                startFrom = startFrom,
  //                reader = reader, footer = footer
  //              )
  //        }
  //    } getOrElse {
  //      SortedIndex.find(
  //        matcher = matcher,
  //        startFrom = startFrom,
  //        reader = reader, footer = footer
  //      )
  //    }

  private[a] def getFromHashIndex(matcher: KeyMatcher.GetNextPrefixCompressed,
                                  reader: Reader,
                                  header: HashIndex.Header,
                                  hashIndexOffset: HashIndex.Offset,
                                  footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
  //    HashIndex.find[Persistent](
  //      key = matcher.key,
  //      hashIndexStartOffset = hashIndexOffset.start,
  //      hashIndexReader = reader,
  //      hashIndexSize = hashIndexOffset.size,
  //      maxProbe = header.maxProbe,
  //      assertValue =
  //        sortedIndexOffset =>
  //          SortedIndex.findFromIndex(
  //            matcher = matcher,
  //            fromOffset = footer.sortedIndexOffset.start + sortedIndexOffset,
  //            reader = reader,
  //            footer = footer
  //          ) recoverWith {
  //            case _ =>
  //              //currently there is no way to detect starting point for a key-value entry in the sorted index.
  //              //Read requests can be submitted to random parts of the sortedIndex depending on the index returned by the hash.
  //              //Hash index also itself also does not store markers for a valid start sortedIndex offset
  //              //that's why key-values can be read at random parts of the sorted index which can return failures.
  //              //too many failures are not expected because probe should disallow that. And if the Segment is actually corrupted,
  //              //the normal forward read of the index should catch that.
  //              //HashIndex is suppose to make random reads faster, if the hashIndex is too small then there is no use creating one.
  //              IO.none
  //          }
  //    )
    ???
}
