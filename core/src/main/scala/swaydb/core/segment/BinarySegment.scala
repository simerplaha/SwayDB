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

package swaydb.core.segment

import java.nio.file.Paths
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Persistent, _}
import swaydb.core.io.reader.BlockReader
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.{KeyMatcher, SegmentBlock, SegmentFooter, SegmentReader}
import swaydb.core.util._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, MaxKey, Reserve}

import scala.annotation.tailrec

private[core] class BinarySegmentInitialiser(id: String,
                                             maxKey: MaxKey[Slice[Byte]],
                                             minKey: Slice[Byte],
                                             unsliceKey: Boolean,
                                             offset: () => IO[SegmentBlock.Offset],
                                             createSegmentReader: () => Reader) {
  private val created = new AtomicBoolean(false)
  @volatile private var segment: BinarySegment = _

  def isInitialised: Boolean =
    segment != null && segment.isOpen

  @tailrec
  final def get(implicit keyOrder: KeyOrder[Slice[Byte]],
                keyValueLimiter: KeyValueLimiter): BinarySegment =
    if (segment != null) {
      segment
    } else if (created.compareAndSet(false, true)) {
      segment =
        new BinarySegment(
          id = id,
          maxKey = maxKey,
          minKey = minKey,
          cache = new ConcurrentSkipListMap[Slice[Byte], Persistent](keyOrder),
          unsliceKey = unsliceKey,
          offset = offset,
          createSegmentReader = createSegmentReader,
          segmentBlockReserve = Reserve()
        )
      segment
    } else {
      get
    }
}

private[core] class BinarySegment(id: String,
                                  maxKey: MaxKey[Slice[Byte]],
                                  minKey: Slice[Byte],
                                  cache: ConcurrentSkipListMap[Slice[Byte], Persistent],
                                  unsliceKey: Boolean,
                                  offset: () => IO[SegmentBlock.Offset],
                                  createSegmentReader: () => Reader,
                                  segmentBlockReserve: Reserve[Unit])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                      keyValueLimiter: KeyValueLimiter) extends LazyLogging {

  import keyOrder._

  @volatile private var segmentBlockCache: Option[IO.Success[SegmentBlock]] = None
  @volatile private var footer: Either[Unit, IO.Success[SegmentFooter]] = Left()
  @volatile private var hashIndex: Either[Unit, IO.Success[Option[HashIndex]]] = Left()
  @volatile private var bloomFilter: Either[Unit, IO.Success[Option[BloomFilter]]] = Left()
  @volatile private var binarySearchIndex: Either[Unit, IO.Success[Option[BinarySearchIndex]]] = Left()
  @volatile private var sortedIndex: Either[Unit, IO.Success[SortedIndex]] = Left()
  @volatile private var values: Either[Unit, IO.Success[Option[Values]]] = Left()

  def close() = {
    segmentBlockCache = None
    footer = Left()
    hashIndex = Left()
    bloomFilter = Left()
    binarySearchIndex = Left()
    sortedIndex = Left()
    values = Left()
  }

  def isOpen: Boolean =
    segmentBlockCache.isDefined ||
      footer.isRight ||
      hashIndex.isRight ||
      bloomFilter.isRight ||
      binarySearchIndex.isRight ||
      sortedIndex.isRight ||
      values.isRight

  private def segmentBlock: IO[SegmentBlock] =
    segmentBlockCache.getOrElse {
      if (Reserve.setBusyOrGet((), segmentBlockReserve).isEmpty)
        try
          offset() flatMap {
            segmentOffset =>
              SegmentBlock.read(
                offset = SegmentBlock.Offset(0, segmentOffset.size),
                segmentReader = createSegmentReader()
              ) map {
                block =>
                  segmentBlockCache = Some(IO.Success(block))
                  block
              }
          }
        finally
          Reserve.setFree(segmentBlockReserve)
      else
        IO.Failure(IO.Error.OpeningFile(Paths.get(id), segmentBlockReserve))
    }

  def createSegmentBlockReader(): IO[BlockReader[SegmentBlock]] =
    segmentBlock map {
      block =>
        block.createBlockReader(createSegmentReader())
    }

  /**
    * Notes for why use putIfAbsent before adding to cache:
    *
    * Sometimes file seeks will be done if the last known cached key-value's ranges are smaller than the
    * key being searched. For example: Search key is 10, but the last lower cache key-value range is 1-5.
    * here it's unknown if a lower key 7 exists without doing a file seek. This is also one of the reasons
    * reverse iterations are slower than forward. There are ways we can improve this which will eventually be implemented.
    *
    */
  private def addToCache(keyValue: Persistent.SegmentResponse): Unit = {
    if (unsliceKey) keyValue.unsliceKeys
    if (cache.putIfAbsent(keyValue.key, keyValue) == null)
      keyValueLimiter.add(keyValue, cache)
  }

  private def addToCache(group: Persistent.Group): Unit = {
    if (unsliceKey) group.unsliceKeys
    if (cache.putIfAbsent(group.key, group) == null)
      keyValueLimiter.add(group, cache)
  }

  private def prepareGet[T](operation: (SegmentFooter, Option[BlockReader[HashIndex]], Option[BlockReader[BinarySearchIndex]], BlockReader[SortedIndex], Option[BlockReader[Values]]) => IO[T]): IO[T] = {
    for {
      footer <- getFooter()
      hashIndex <- createHashIndexReader()
      binarySearchIndex <- createBinarySearchIndexReader()
      sortedIndex <- createSortedIndexReader()
      values <- createValuesReader()
      result <- operation(footer, hashIndex, binarySearchIndex, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  private def prepareGetAll[T](operation: (SegmentFooter, BlockReader[SortedIndex], Option[BlockReader[Values]]) => IO[T]): IO[T] = {
    for {
      footer <- getFooter()
      sortedIndex <- createSortedIndexReader()
      values <- createValuesReader()
      result <- operation(footer, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  private def prepareIteration[T](operation: (SegmentFooter, Option[BlockReader[BinarySearchIndex]], BlockReader[SortedIndex], Option[BlockReader[Values]]) => IO[T]): IO[T] = {
    for {
      footer <- getFooter()
      binarySearchIndex <- createBinarySearchIndexReader()
      sortedIndex <- createSortedIndexReader()
      values <- createValuesReader()
      result <- operation(footer, binarySearchIndex, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  def getFooter(): IO[SegmentFooter] =
    footer
      .getOrElse {
        createSegmentBlockReader() flatMap {
          reader =>
            SegmentFooter.read(reader) map {
              footer =>
                this.footer = Right(IO.Success(footer))
                footer
            }
        }
      }

  def getFooterAndSegmentReader(): IO[(SegmentFooter, Reader)] =
    footer
      .map {
        footer =>
          for {
            reader <- createSegmentBlockReader()
            footer <- footer
          } yield (footer, reader)
      }
      .getOrElse {
        createSegmentBlockReader() flatMap {
          reader =>
            SegmentFooter.read(reader) map {
              footer =>
                this.footer = Right(IO.Success(footer))
                (footer, reader.reset())
            }
        }
      }

  def createOptionalBlockReader[B <: Block](block: Option[B]): IO[Option[BlockReader[B]]] =
    block map {
      index =>
        createBlockReader(index).map(Some(_))
    } getOrElse IO.none

  def createBlockReader[B <: Block](block: B): IO[BlockReader[B]] =
    createSegmentBlockReader() map {
      reader =>
        block.createBlockReader(reader).asInstanceOf[BlockReader[B]]
    }

  def getHashIndex(): IO[Option[HashIndex]] =
    hashIndex
      .getOrElse {
        getFooterAndSegmentReader() flatMap {
          case (footer, reader) =>
            footer.hashIndexOffset map {
              offset =>
                HashIndex.read(offset, reader) map {
                  index =>
                    val someIndex = Some(index)
                    this.hashIndex = Right(IO.Success(someIndex))
                    someIndex
                }
            } getOrElse {
              this.hashIndex = Right(IO.none)
              IO.none
            }
        }
      }

  def createHashIndexReader(): IO[Option[BlockReader[HashIndex]]] =
    getHashIndex() flatMap createOptionalBlockReader

  def getBloomFilter(): IO[Option[BloomFilter]] =
    bloomFilter
      .getOrElse {
        getFooterAndSegmentReader() flatMap {
          case (footer, reader) =>
            footer.bloomFilterOffset map {
              offset =>
                BloomFilter.read(offset, reader) map {
                  index =>
                    val someIndex = Some(index)
                    this.bloomFilter = Right(IO.Success(someIndex))
                    someIndex
                }
            } getOrElse {
              this.bloomFilter = Right(IO.none)
              IO.none
            }
        }
      }

  def createBloomFilterReader(): IO[Option[BlockReader[BloomFilter]]] =
    getBloomFilter() flatMap createOptionalBlockReader

  def getBinarySearchIndex(): IO[Option[BinarySearchIndex]] =
    binarySearchIndex
      .getOrElse {
        getFooterAndSegmentReader() flatMap {
          case (footer, reader) =>
            footer.binarySearchIndexOffset map {
              offset =>
                BinarySearchIndex.read(offset, reader) map {
                  index =>
                    val someIndex = Some(index)
                    this.binarySearchIndex = Right(IO.Success(someIndex))
                    someIndex
                }
            } getOrElse {
              this.binarySearchIndex = Right(IO.none)
              IO.none
            }
        }
      }

  def createBinarySearchReader(): IO[Option[BlockReader[BinarySearchIndex]]] =
    getBinarySearchIndex() flatMap createOptionalBlockReader

  def createBinarySearchIndexReader(): IO[Option[BlockReader[BinarySearchIndex]]] =
    getBinarySearchIndex() flatMap {
      indexOption =>
        indexOption map {
          index =>
            createSegmentBlockReader() map {
              reader =>
                Some(index.createBlockReader(reader))
            }
        } getOrElse IO.none
    }

  def getSortedIndex(): IO[SortedIndex] =
    sortedIndex
      .getOrElse {
        getFooterAndSegmentReader() flatMap {
          case (footer, reader) =>
            SortedIndex.read(footer.sortedIndexOffset, reader) map {
              index =>
                this.sortedIndex = Right(IO.Success(index))
                index
            }
        }
      }

  def createSortedIndexReader(): IO[BlockReader[SortedIndex]] =
    getSortedIndex() flatMap createBlockReader

  def getValues(): IO[Option[Values]] =
    values
      .getOrElse {
        getFooterAndSegmentReader() flatMap {
          case (footer, reader) =>
            footer.valuesOffset map {
              valuesOffset =>
                Values.read(valuesOffset, reader) map {
                  values =>
                    val someValues = Some(values)
                    this.values = Right(IO.Success(someValues))
                    someValues
                }
            } getOrElse {
              this.values = Right(IO.none)
              IO.none
            }
        }
      }

  def createValuesReader(): IO[Option[BlockReader[Values]]] =
    getValues() flatMap createOptionalBlockReader

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    Option(cache.get(key))

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    for {
      bloom <- createBloomFilterReader()
      contains <- bloom.map(BloomFilter.mightContain(key, _)) getOrElse IO.`true`
    } yield contains

  def get(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key > maxKey =>
        IO.none

      case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
        IO.none

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        IO.none

      case _ =>
        val floorValue = Option(cache.floorEntry(key)).map(_.getValue)
        floorValue match {
          case Some(floor: Persistent.SegmentResponse) if floor.key equiv key =>
            IO.Success(Some(floor))

          //check if the key belongs to this group.
          case Some(group: Persistent.Group) if group contains key =>
            group.segment.get(key)

          case Some(floorRange: Persistent.Range) if floorRange contains key =>
            IO.Success(Some(floorRange))

          case _ =>
            mightContain(key) flatMap {
              contains =>
                if (contains)
                  prepareGet {
                    (footer, hashIndex, binarySearchIndex, sortedIndex, values) =>
                      SegmentReader.get(
                        matcher = KeyMatcher.Get(key),
                        startFrom = floorValue,
                        hashIndex = hashIndex,
                        binarySearchIndex = binarySearchIndex,
                        sortedIndex = sortedIndex,
                        valuesReader = values,
                        hasRange = footer.hasRange
                      ) flatMap {
                        case Some(response: Persistent.SegmentResponse) =>
                          addToCache(response)
                          IO.Success(Some(response))

                        case Some(group: Persistent.Group) =>
                          addToCache(group)
                          group.segment.get(key)

                        case None =>
                          IO.none
                      }
                  }
                else
                  IO.none
            }
        }
    }

  private def satisfyLowerFromCache(key: Slice[Byte],
                                    lowerKeyValue: Persistent): Option[Persistent] =
  //if the lowest key-value in the cache is the last key-value, then lower is the next lowest key-value for the key.
    if (lowerKeyValue.nextIndexOffset == -1) //-1 indicated last key-value in the Segment.
      Some(lowerKeyValue)
    else
      lowerKeyValue match {
        case lowerRange: Persistent.Range if lowerRange containsLower key =>
          Some(lowerRange)

        case lowerGroup: Persistent.Group if lowerGroup containsLower key =>
          Some(lowerGroup)

        case _ =>
          Option(cache.ceilingEntry(key)).map(_.getValue) flatMap {
            ceilingKeyValue =>
              if (lowerKeyValue.nextIndexOffset == ceilingKeyValue.indexOffset)
                Some(lowerKeyValue)
              else
                None
          }
      }

  def lower(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    if (key <= minKey)
      IO.none
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          get(maxKey)

        case MaxKey.Range(fromKey, _) if key > fromKey =>
          get(fromKey)

        case _ =>
          val lowerKeyValue = Option(cache.lowerEntry(key)).map(_.getValue)
          val lowerFromCache = lowerKeyValue.flatMap(satisfyLowerFromCache(key, _))
          lowerFromCache map {
            case response: Persistent.SegmentResponse =>
              IO.Success(Some(response))

            case group: Persistent.Group =>
              group.segment.lower(key)
          } getOrElse {
            prepareIteration {
              (footer, binarySearchIndex, sortedIndex, valuesReader) =>
                SegmentReader.lower(
                  matcher = KeyMatcher.Lower(key),
                  startFrom = lowerKeyValue,
                  binarySearch = binarySearchIndex,
                  sortedIndex = sortedIndex,
                  valuesReader
                ) flatMap {
                  case Some(response: Persistent.SegmentResponse) =>
                    addToCache(response)
                    IO.Success(Some(response))

                  case Some(group: Persistent.Group) =>
                    addToCache(group)
                    group.segment.lower(key)

                  case None =>
                    IO.none
                }
            }
          }
      }

  private def satisfyHigherFromCache(key: Slice[Byte],
                                     floorKeyValue: Persistent): Option[Persistent] =
    floorKeyValue match {
      case floorRange: Persistent.Range if floorRange contains key =>
        Some(floorRange)

      case floorGroup: Persistent.Group if floorGroup containsHigher key =>
        Some(floorGroup)

      case _ =>
        Option(cache.higherEntry(key)).map(_.getValue) flatMap {
          higherKeyValue =>
            if (floorKeyValue.nextIndexOffset == higherKeyValue.indexOffset)
              Some(higherKeyValue)
            else
              None
        }
    }

  def floorHigherHint(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    hasPut map {
      hasPut =>
        if (hasPut)
          if (key < minKey)
            Some(minKey)
          else if (key < maxKey.maxKey)
            Some(key)
          else
            None
        else
          None
    }

  def higher(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key >= maxKey =>
        IO.none

      case MaxKey.Range(_, maxKey) if key >= maxKey =>
        IO.none

      case _ =>
        val floorKeyValue = Option(cache.floorEntry(key)).map(_.getValue)
        val higherFromCache = floorKeyValue.flatMap(satisfyHigherFromCache(key, _))

        higherFromCache map {
          case response: Persistent.SegmentResponse =>
            IO.Success(Some(response))

          case group: Persistent.Group =>
            group.segment.higher(key)
        } getOrElse {
          prepareIteration {
            (footer, binarySearchIndex, sortedIndex, values) =>
              val startFrom =
                if (floorKeyValue.isDefined)
                  IO(floorKeyValue)
                else
                  get(key)

              //              startFrom flatMap {
              //                startFrom =>
              //                  SegmentReader.higher(
              //                    matcher = KeyMatcher.Higher(key),
              //                    startFrom = startFrom,
              //                    reader = reader,
              //                    index = footer.sortedIndexOffset
              //                  ) flatMap {
              //                    case Some(response: Persistent.SegmentResponse) =>
              //                      addToCache(response)
              //                      IO.Success(Some(response))
              //
              //                    case Some(group: Persistent.Group) =>
              //                      addToCache(group)
              //                      group.segment.higher(key)
              //
              //                    case None =>
              //                      IO.none
              //                  }
              //              }
              ???
          }
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    prepareGetAll {
      (footer, sortedIndex, values) =>
        SortedIndex
          .readAll(
            keyValueCount = footer.keyValueCount,
            sortedIndexReader = sortedIndex,
            valuesReader = values,
            addTo = addTo
          )
          .onFailureSideEffect {
            _ =>
              logger.trace("{}: Reading sorted index block failed.", id)
          }
    }

  def getHeadKeyValueCount(): IO[Int] =
    getFooter().map(_.keyValueCount)

  def getBloomFilterKeyValueCount(): IO[Int] =
    getFooter().map(_.bloomFilterItemsCount)

  def hasRange: IO[Boolean] =
    getFooter().map(_.hasRange)

  def hasPut: IO[Boolean] =
    getFooter().map(_.hasPut)

  def isCacheEmpty =
    cache.isEmpty()

  def isFooterDefined: Boolean =
    footer.exists(_ => true)

  def isBloomFilterDefined: Boolean =
    bloomFilter.exists(_.exists(_.isDefined))

  def createdInLevel: IO[Int] =
    getFooter().map(_.createdInLevel)

  def isGrouped: IO[Boolean] =
    getFooter().map(_.hasGroup)
}
