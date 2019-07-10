package swaydb.core.segment.format.a.block

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Persistent
import swaydb.core.io.reader.BlockReader
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.get).
  * Eventually need to re-factor this code to use for comprehension.
  *
  * Leaving as it is now because it's easier to read.
  */
private[core] object SegmentBlockSearcher extends LazyLogging {

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  binarySearchReader: Option[BlockReader[BinarySearchIndex]],
                  sortedIndexReader: BlockReader[SortedIndex],
                  valuesReader: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearchReader map {
      binarySearchIndexReader =>
        BinarySearchIndex.searchLower(
          key = key,
          start = start,
          end = end,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case someLower @ Some(lower) =>
            if (binarySearchIndexReader.block.isFullIndex || lower.nextIndexSize == 0)
              IO.Success(someLower)
            else
              SortedIndex.searchLower(
                key = key,
                startFrom = someLower,
                indexReader = sortedIndexReader,
                valuesReader = valuesReader
              )

          case None =>
            if (binarySearchIndexReader.block.isFullIndex)
              IO.none
            else
              SortedIndex.searchLower(
                key = key,
                startFrom = start,
                indexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      SortedIndex.searchLower(
        key = key,
        startFrom = start,
        indexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   binarySearchReader: Option[BlockReader[BinarySearchIndex]],
                   sortedIndexReader: BlockReader[SortedIndex],
                   valuesReader: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] = {
    if (start.isEmpty)
      IO.none
    else
      SortedIndex.searchHigherSeekOne(
        key = key,
        startFrom = start,
        indexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
  } flatMap {
    found =>
      if (found.isDefined)
        IO.Success(found)
      else
        binarySearchReader map {
          binarySearchIndexReader =>
            BinarySearchIndex.searchHigher(
              key = key,
              start = start,
              end = end,
              binarySearchIndexReader = binarySearchIndexReader,
              sortedIndexReader = sortedIndexReader,
              valuesReader = valuesReader
            ) flatMap {
              case someHigher @ Some(higher) =>
                if (binarySearchIndexReader.block.isFullIndex || higher.nextIndexSize == 0)
                  IO.Success(someHigher)
                else
                  SortedIndex.searchHigher(
                    key = key,
                    startFrom = someHigher,
                    sortedIndexReader = sortedIndexReader,
                    valuesReader = valuesReader
                  )

              case None =>
                if (binarySearchIndexReader.block.isFullIndex)
                  IO.none
                else
                  SortedIndex.searchHigher(
                    key = key,
                    startFrom = start,
                    sortedIndexReader = sortedIndexReader,
                    valuesReader = valuesReader
                  )
            }
        } getOrElse {
          SortedIndex.searchHigher(
            key = key,
            startFrom = start,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          )
        }
  }

  def search(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             hashIndexReader: Option[BlockReader[HashIndex]],
             binarySearchIndexReader: Option[BlockReader[BinarySearchIndex]],
             sortedIndexReader: BlockReader[SortedIndex],
             valuesReaderReader: Option[BlockReader[Values]],
             hasRange: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    hashIndexReader map {
      hashIndexReader =>
        HashIndex.search(
          key = key,
          hashIndexReader = hashIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReaderReader = valuesReaderReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (hashIndexReader.block.isPerfect && !hasRange)
              IO.none
            else
              search(
                key = key,
                start = start,
                end = end,
                binarySearchIndexReader = binarySearchIndexReader,
                sortedIndexReader = sortedIndexReader,
                valuesReader = valuesReaderReader
              )
        }
    } getOrElse {
      search(
        key = key,
        start = start,
        end = end,
        binarySearchIndexReader = binarySearchIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReaderReader
      )
    }

  private def search(key: Slice[Byte],
                     start: Option[Persistent],
                     end: Option[Persistent],
                     binarySearchIndexReader: Option[BlockReader[BinarySearchIndex]],
                     sortedIndexReader: BlockReader[SortedIndex],
                     valuesReader: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearchIndexReader map {
      binarySearchIndexReader =>
        BinarySearchIndex.search(
          key = key,
          start = start,
          end = end,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (binarySearchIndexReader.block.isFullIndex)
              IO.none
            else
              SortedIndex.search(
                key = key,
                startFrom = start,
                indexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      SortedIndex.search(
        key = key,
        startFrom = start,
        indexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }
}
