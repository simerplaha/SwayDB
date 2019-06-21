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

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Persistent, _}
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.{KeyMatcher, SegmentFooter, SegmentReader}
import swaydb.core.segment.format.a.index.{BinarySearchIndex, BloomFilter, HashIndex, SortedIndex}
import swaydb.core.util._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, MaxKey}

import scala.annotation.tailrec

private[core] class BitwiseSegmentInitialiser(id: String,
                                              maxKey: MaxKey[Slice[Byte]],
                                              minKey: Slice[Byte],
                                              unsliceKey: Boolean,
                                              createReader: () => IO[Reader]) {
  private val created = new AtomicBoolean(false)
  @volatile private var segment: BitwiseSegment = _

  @tailrec
  final def create(implicit keyOrder: KeyOrder[Slice[Byte]],
                   keyValueLimiter: KeyValueLimiter): BitwiseSegment =
    if (segment != null) {
      segment
    } else if (created.compareAndSet(false, true)) {
      segment =
        new BitwiseSegment(
          id = id,
          maxKey = maxKey,
          minKey = minKey,
          cache = new ConcurrentSkipListMap[Slice[Byte], Persistent](keyOrder),
          unsliceKey = unsliceKey,
          createReader = createReader
        )
      segment
    } else {
      create
    }
}

private[core] class BitwiseSegment(id: String,
                                   maxKey: MaxKey[Slice[Byte]],
                                   minKey: Slice[Byte],
                                   cache: ConcurrentSkipListMap[Slice[Byte], Persistent],
                                   unsliceKey: Boolean,
                                   createReader: () => IO[Reader])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                   keyValueLimiter: KeyValueLimiter) extends LazyLogging {

  import keyOrder._

  @volatile private var footer: Either[Unit, IO.Success[SegmentFooter]] = Left()
  @volatile private var hashIndexHeader: Either[Unit, IO.Success[Option[HashIndex]]] = Left()
  @volatile private var bloomFilterHeader: Either[Unit, IO.Success[Option[BloomFilter]]] = Left()
  @volatile private var binarySearchIndexHeader: Either[Unit, IO.Success[Option[BinarySearchIndex]]] = Left()

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
    if (unsliceKey) keyValue.unsliceIndexBytes
    if (cache.putIfAbsent(keyValue.key, keyValue) == null)
      keyValueLimiter.add(keyValue, cache)
  }

  private def addToCache(group: Persistent.Group): Unit = {
    if (unsliceKey) group.unsliceIndexBytes
    if (cache.putIfAbsent(group.key, group) == null)
      keyValueLimiter.add(group, cache)
  }

  private def prepareGet[T](getOperation: (SegmentFooter, Option[HashIndex], Option[BinarySearchIndex], Reader) => IO[T]): IO[T] = {
    for {
      footer <- getFooter()
      hashIndex <- getHashIndex()
      binarySearchIndex <- getBinarySearchIndex()
      reader <- createReader()
      get <- getOperation(footer, hashIndex, binarySearchIndex, reader)
    } yield {
      get
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  private def prepareIteration[T](getOperation: (SegmentFooter, Option[BinarySearchIndex], Reader) => IO[T]): IO[T] = {
    for {
      footer <- getFooter()
      binarySearchIndex <- getBinarySearchIndex()
      reader <- createReader()
      get <- getOperation(footer, binarySearchIndex, reader)
    } yield {
      get
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  def getFooter(): IO[SegmentFooter] =
    footer
      .getOrElse {
        createReader() flatMap {
          reader =>
            SegmentFooter.read(reader) map {
              footer =>
                this.footer = Right(IO.Success(footer))
                footer
            }
        }
      }

  def getFooterAndReader(): IO[(SegmentFooter, Reader)] =
    footer
      .map {
        footer =>
          for {
            reader <- createReader()
            footer <- footer
          } yield (footer, reader)
      }
      .getOrElse {
        createReader() flatMap {
          reader =>
            SegmentFooter.read(reader) map {
              footer =>
                this.footer = Right(IO.Success(footer))
                (footer, reader.reset())
            }
        }
      }

  def getHashIndex(): IO[Option[HashIndex]] =
    hashIndexHeader
      .getOrElse {
        getFooterAndReader() flatMap {
          case (footer, reader) =>
            footer.hashIndexOffset map {
              offset =>
                HashIndex.read(offset, reader) map {
                  index =>
                    val someIndex = Some(index)
                    this.hashIndexHeader = Right(IO.Success(someIndex))
                    someIndex
                }
            } getOrElse {
              this.hashIndexHeader = Right(IO.none)
              IO.none
            }
        }
      }

  def getBloomFilter(): IO[Option[BloomFilter]] =
    bloomFilterHeader
      .getOrElse {
        getFooterAndReader() flatMap {
          case (footer, reader) =>
            footer.bloomFilterOffset map {
              offset =>
                BloomFilter.read(offset, reader) map {
                  index =>
                    val someIndex = Some(index)
                    this.bloomFilterHeader = Right(IO.Success(someIndex))
                    someIndex
                }
            } getOrElse {
              this.bloomFilterHeader = Right(IO.none)
              IO.none
            }
        }
      }

  def getBinarySearchIndex(): IO[Option[BinarySearchIndex]] =
    binarySearchIndexHeader
      .getOrElse {
        getFooterAndReader() flatMap {
          case (footer, reader) =>
            footer.binarySearchIndexOffset map {
              offset =>
                BinarySearchIndex.read(offset, reader) map {
                  index =>
                    val someIndex = Some(index)
                    this.binarySearchIndexHeader = Right(IO.Success(someIndex))
                    someIndex
                }
            } getOrElse {
              this.binarySearchIndexHeader = Right(IO.none)
              IO.none
            }
        }
      }

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    Option(cache.get(key))

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    for {
      bloom <- getBloomFilter()
      contains <- bloom map {
        bloom =>
          createReader() flatMap {
            reader =>
              BloomFilter.mightContain(
                key = key,
                reader = reader,
                bloom = bloom
              )
          }
      } getOrElse IO.`true`
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
            group.segmentCache.get(key)

          case Some(floorRange: Persistent.Range) if floorRange contains key =>
            IO.Success(Some(floorRange))

          case _ =>
            mightContain(key) flatMap {
              contains =>
                if (contains)
                  prepareGet {
                    (footer, hashIndex, binarySearchIndex, reader) =>
                      SegmentReader.get(
                        matcher = KeyMatcher.Get(key),
                        startFrom = floorValue,
                        reader = reader,
                        hashIndex = hashIndex,
                        binarySearchIndex = binarySearchIndex,
                        sortedIndexOffset = footer.sortedIndexOffset,
                        hasRange = footer.hasRange
                      ) flatMap {
                        case Some(response: Persistent.SegmentResponse) =>
                          addToCache(response)
                          IO.Success(Some(response))

                        case Some(group: Persistent.Group) =>
                          addToCache(group)
                          group.segmentCache.get(key)

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
              group.segmentCache.lower(key)
          } getOrElse {
            prepareIteration {
              (footer, binarySearchIndex, reader) =>
                SegmentReader.lower(
                  matcher = KeyMatcher.Lower(key),
                  startFrom = lowerKeyValue,
                  reader = reader,
                  footer = footer
                ) flatMap {
                  case Some(response: Persistent.SegmentResponse) =>
                    addToCache(response)
                    IO.Success(Some(response))

                  case Some(group: Persistent.Group) =>
                    addToCache(group)
                    group.segmentCache.lower(key)

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
            group.segmentCache.higher(key)
        } getOrElse {
          prepareIteration {
            (footer, binarySearchIndex, reader) =>
              val startFrom =
                if (floorKeyValue.isDefined)
                  IO(floorKeyValue)
                else
                  get(key)

              startFrom flatMap {
                startFrom =>
                  SegmentReader.higher(
                    matcher = KeyMatcher.Higher(key),
                    startFrom = startFrom,
                    reader = reader,
                    footer = footer
                  ) flatMap {
                    case Some(response: Persistent.SegmentResponse) =>
                      addToCache(response)
                      IO.Success(Some(response))

                    case Some(group: Persistent.Group) =>
                      addToCache(group)
                      group.segmentCache.higher(key)

                    case None =>
                      IO.none
                  }
              }
          }
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    getFooterAndReader() flatMap {
      case (footer, reader) =>
        SortedIndex
          .readAll(
            offset = footer.sortedIndexOffset,
            keyValueCount = footer.keyValueCount,
            reader = reader,
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
    bloomFilterHeader.exists(_.exists(_.isDefined))

  def createdInLevel: IO[Int] =
    getFooter().map(_.createdInLevel)

  def isGrouped: IO[Boolean] =
    getFooter().map(_.hasGroup)

  def close() = {
    footer = Left()
    hashIndexHeader = Left()
    binarySearchIndexHeader = Left()
    bloomFilterHeader = Left()
  }
}
