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
                  binarySearch: Option[BlockReader[BinarySearchIndex]],
                  sortedIndex: BlockReader[SortedIndex],
                  values: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearch map {
      binarySearchIndex =>
        BinarySearchIndex.searchLower(
          key = key,
          start = start,
          end = end,
          binarySearchIndex = binarySearchIndex,
          sortedIndex = sortedIndex,
          values = values
        ) flatMap {
          case someLower @ Some(lower) =>
            if (binarySearchIndex.block.isFullIndex || lower.nextIndexSize == 0)
              IO.Success(someLower)
            else
              SortedIndex.searchLower(
                key = key,
                startFrom = someLower,
                indexReader = sortedIndex,
                valuesReader = values
              )

          case None =>
            if (binarySearchIndex.block.isFullIndex)
              IO.none
            else
              SortedIndex.searchLower(
                key = key,
                startFrom = start,
                indexReader = sortedIndex,
                valuesReader = values
              )
        }
    } getOrElse {
      SortedIndex.searchLower(
        key = key,
        startFrom = start,
        indexReader = sortedIndex,
        valuesReader = values
      )
    }

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   binarySearch: Option[BlockReader[BinarySearchIndex]],
                   sortedIndex: BlockReader[SortedIndex],
                   values: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] = {
    if (start.isEmpty)
      IO.none
    else
      SortedIndex.searchHigherSeekOne(
        key = key,
        startFrom = start,
        indexReader = sortedIndex,
        valuesReader = values
      )
  } flatMap {
    found =>
      if (found.isDefined)
        IO.Success(found)
      else
        binarySearch map {
          binarySearchIndex =>
            BinarySearchIndex.searchHigher(
              key = key,
              start = start,
              end = end,
              binarySearchIndex = binarySearchIndex,
              sortedIndex = sortedIndex,
              values = values
            ) flatMap {
              case someHigher @ Some(higher) =>
                if (binarySearchIndex.block.isFullIndex || higher.nextIndexSize == 0)
                  IO.Success(someHigher)
                else
                  SortedIndex.searchHigher(
                    key = key,
                    startFrom = someHigher,
                    indexReader = sortedIndex,
                    valuesReader = values
                  )

              case None =>
                if (binarySearchIndex.block.isFullIndex)
                  IO.none
                else
                  SortedIndex.searchHigher(
                    key = key,
                    startFrom = start,
                    indexReader = sortedIndex,
                    valuesReader = values
                  )
            }
        } getOrElse {
          SortedIndex.searchHigher(
            key = key,
            startFrom = start,
            indexReader = sortedIndex,
            valuesReader = values
          )
        }
  }

  def search(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             hashIndex: Option[BlockReader[HashIndex]],
             binarySearchIndex: Option[BlockReader[BinarySearchIndex]],
             sortedIndex: BlockReader[SortedIndex],
             valuesReader: Option[BlockReader[Values]],
             hasRange: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] = {
    hashIndex map {
      hashIndex =>
        HashIndex.search(
          key = key,
          hashIndex = hashIndex,
          sortedIndex = sortedIndex,
          values = valuesReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (hashIndex.block.miss == 0 && !hasRange)
              IO.none
            else
              search(
                key = key,
                start = start,
                end = end,
                binarySearchIndex = binarySearchIndex,
                sortedIndex = sortedIndex,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      search(
        key = key,
        start = start,
        end = end,
        binarySearchIndex = binarySearchIndex,
        sortedIndex = sortedIndex,
        valuesReader = valuesReader
      )
    }
  }

  private def search(key: Slice[Byte],
                     start: Option[Persistent],
                     end: Option[Persistent],
                     binarySearchIndex: Option[BlockReader[BinarySearchIndex]],
                     sortedIndex: BlockReader[SortedIndex],
                     valuesReader: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearchIndex map {
      binarySearchIndex =>
        BinarySearchIndex.search(
          key = key,
          start = start,
          end = end,
          binarySearchIndex = binarySearchIndex,
          sortedIndex = sortedIndex,
          values = valuesReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (binarySearchIndex.block.isFullIndex)
              IO.none
            else
              SortedIndex.search(
                key = key,
                startFrom = start,
                indexReader = sortedIndex,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      SortedIndex.search(
        key = key,
        startFrom = start,
        indexReader = sortedIndex,
        valuesReader = valuesReader
      )
    }
}
