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

package swaydb.core.segment

import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap

import bloomfilter.mutable.BloomFilter
import swaydb.core.data.Persistent.{PutReadOnly, RemovedReadOnly}
import swaydb.core.data._
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.map.Map
import swaydb.core.segment.format.one.{SegmentReader, SegmentWriter}
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.util.IDGenerator
import swaydb.core.util.TryUtil._
import swaydb.data.config.Dir
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

private[core] object Segment {

  def memory(path: Path,
             keyValues: Slice[KeyValueWriteOnly],
             bloomFilterFalsePositiveRate: Double,
             removeDeletes: Boolean)(implicit ordering: Ordering[Slice[Byte]]): Try[Segment] = {
    val skipList = new ConcurrentSkipListMap[Slice[Byte], PersistentReadOnly](ordering)
    val bloomFilter = BloomFilter[Slice[Byte]](keyValues.size, bloomFilterFalsePositiveRate)
    keyValues tryForeach {
      keyValue =>
        if (keyValue.isRemove) {
          skipList.put(
            keyValue.key,
            RemovedReadOnly(keyValue.key, 0x00, 0x00, 0x00)
          )
          bloomFilter add keyValue.key
          Success()
        }
        else
          keyValue.getOrFetchValue map {
            valueOption =>
              val (reader, valueSize) =
                valueOption.map {
                  value =>
                    (Reader(value.unslice()), value.size)
                } getOrElse
                  (Reader.emptyReader, 0)
              skipList.put(
                keyValue.key.unslice(),
                PutReadOnly(
                  _key = keyValue.key,
                  valueReader = reader,
                  nextIndexOffset = 0x00,
                  nextIndexSize = 0x00,
                  indexOffset = 0x00,
                  valueOffset = 0x00,
                  valueLength = valueSize
                )
              )
              bloomFilter add keyValue.key
          }
    } match {
      case Some(Failure(exception)) =>
        Failure(exception)

      case None =>
        Success(
          new MemorySegment(
            path = path,
            minKey = keyValues.head.key.unslice(),
            maxKey = keyValues.last.key.unslice(),
            segmentSize = keyValues.last.stats.memorySegmentSize,
            removeDeletes = removeDeletes,
            bloomFilter = bloomFilter,
            cache = skipList
          )
        )
    }
  }

  def persistent(path: Path,
                 bloomFilterFalsePositiveRate: Double,
                 mmapReads: Boolean,
                 mmapWrites: Boolean,
                 cacheKeysOnCreate: Boolean,
                 keyValues: Slice[KeyValueWriteOnly],
                 removeDeletes: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                         keyValueLimiter: (PersistentReadOnly, Segment) => Unit,
                                         fileOpenLimiter: DBFile => Unit,
                                         ec: ExecutionContext): Try[Segment] =
    SegmentWriter.toSlice(keyValues, bloomFilterFalsePositiveRate) flatMap {
      bytes =>
        val writeResult =
        //if both read and writes are mmaped. Keep the file open.
          if (mmapWrites && mmapReads)
            DBFile.mmapWriteAndRead(bytes, path, fileOpenLimiter)
          //if mmapReads is false, write bytes in mmaped mode and then close and re-open for read.
          else if (mmapWrites && !mmapReads)
            DBFile.mmapWriteAndRead(bytes, path) flatMap {
              file =>
                //close immediately to force flush the bytes to disk. Having mmapWrites == true and mmapReads == false,
                //is probably not the most efficient and should be advised not to used.
                file.close flatMap {
                  _ =>
                    DBFile.channelRead(file.path, fileOpenLimiter)
                }
            }
          else if (!mmapWrites && mmapReads)
            DBFile.write(bytes, path) flatMap {
              path =>
                DBFile.mmapRead(path, fileOpenLimiter)
            }
          else
            DBFile.write(bytes, path) flatMap {
              path =>
                DBFile.channelRead(path, fileOpenLimiter)
            }

        writeResult map {
          file =>
            val segment =
              new PersistentSegment(
                file = file,
                mmapReads = mmapReads,
                mmapWrites = mmapWrites,
                cacheKeysOnCreate = cacheKeysOnCreate,
                minKey = keyValues.head.key.unslice(),
                maxKey = keyValues.last.key.unslice(),
                segmentSize = keyValues.last.stats.segmentSize,
                removeDeletes = removeDeletes
              )
            if (cacheKeysOnCreate) segment.populateCacheWithKeys(Reader(bytes))
            segment
        }
    }

  def apply(path: Path,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            cacheKeysOnCreate: Boolean,
            minKey: Slice[Byte],
            maxKey: Slice[Byte],
            segmentSize: Int,
            removeDeletes: Boolean,
            checkExists: Boolean = true)(implicit ordering: Ordering[Slice[Byte]],
                                         keyValueLimiter: (PersistentReadOnly, Segment) => Unit,
                                         fileOpenLimited: DBFile => Unit,
                                         ec: ExecutionContext): Try[Segment] = {

    val file =
      if (mmapReads)
        DBFile.mmapRead(path, fileOpenLimited, checkExists)
      else
        DBFile.channelRead(path, fileOpenLimited, checkExists)

    file map {
      file =>
        new PersistentSegment(
          file = file,
          mmapReads = mmapReads,
          mmapWrites = mmapWrites,
          cacheKeysOnCreate = cacheKeysOnCreate,
          minKey = minKey,
          maxKey = maxKey,
          segmentSize = segmentSize,
          removeDeletes = removeDeletes
        )
    }
  }

  /**
    * Reads the [[PersistentSegment]] when the min, max keys & fileSize is not known.
    *
    * This function requires the Segment to be opened and read. After the Segment is successfully
    * read the file is closed.
    *
    * Normally this function is only useful for Appendix file recovery.
    */
  def apply(path: Path,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            cacheKeysOnCreate: Boolean,
            removeDeletes: Boolean,
            checkExists: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                  keyValueLimiter: (PersistentReadOnly, Segment) => Unit,
                                  fileOpenLimited: DBFile => Unit,
                                  ec: ExecutionContext): Try[Segment] = {

    val file =
      if (mmapReads)
        DBFile.mmapRead(path, fileOpenLimited, checkExists)
      else
        DBFile.channelRead(path, fileOpenLimited, checkExists)

    file flatMap {
      file =>
        file.fileSize flatMap {
          fileSize =>
            SegmentReader.readFooter(Reader(file)) flatMap {
              footer =>
                SegmentReader.readAllReadOnly(footer, Reader(file)) flatMap {
                  keyValues =>
                    //close the file
                    file.close map {
                      _ =>
                        new PersistentSegment(
                          file = file,
                          mmapReads = mmapReads,
                          mmapWrites = mmapWrites,
                          cacheKeysOnCreate = cacheKeysOnCreate,
                          minKey = keyValues.head.key,
                          maxKey = keyValues.last.key,
                          segmentSize = fileSize.toInt,
                          removeDeletes = removeDeletes
                        )
                    }
                }
            }
        }

    }
  }

  def belongsTo(keyValue: KeyValueWriteOnly,
                segment: Segment)(implicit ordering: Ordering[Slice[Byte]]): Boolean = {
    import ordering._
    keyValue.key >= segment.minKey && keyValue.key <= segment.maxKey
  }

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               segment: Segment)(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey), (segment.minKey, segment.maxKey))

  def overlaps(segment1: Segment,
               segment2: Segment)(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    overlaps(segment1.minKey, segment1.maxKey, segment2)

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    nonOverlapping(segments1, segments2, segments1.size)

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment],
                     count: Int)(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] = {
    if (count == 0)
      Iterable.empty
    else {
      val resultSegments = ListBuffer.empty[Segment]
      segments1 foreachBreak {
        segment1 =>
          if (!segments2.exists(segment2 => overlaps(segment1, segment2)))
            resultSegments += segment1
          resultSegments.size == count
      }
      resultSegments
    }
  }

  def overlaps(segments1: Iterable[Segment],
               segments2: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    segments1.filter(segment1 => segments2.exists(segment2 => overlaps(segment1, segment2)))

  def intersects(segments1: Iterable[Segment], segments2: Iterable[Segment]): Boolean =
    if (segments1.isEmpty || segments2.isEmpty)
      false
    else
      segments1.exists(segment1 => segments2.exists(_.path == segment1.path))

  /**
    * Pre condition: Segments should be sorted with their minKey in ascending order.
    */
  def getAllKeyValues(bloomFilterFalsePositiveRate: Double, segments: Iterable[Segment]): Try[Slice[Persistent]] =
    if (segments.isEmpty)
      Success(Slice.create[Persistent](0))
    else if (segments.size == 1)
      segments.head.getAll(bloomFilterFalsePositiveRate)
    else
      segments.tryFoldLeft(0) {
        case (total, segment) =>
          segment.getKeyValueCount().map(_ + total)
      } flatMap {
        totalKeyValues =>
          segments.tryFoldLeft(Slice.create[Persistent](totalKeyValues)) {
            case (allKeyValues, segment) =>
              segment.getAll(bloomFilterFalsePositiveRate, Some(allKeyValues))
          }
      }

  def deleteSegments(segments: Iterable[Segment]): Try[Int] = {
    //    println(s"$id: Trying to delete segments: ${segments.map(_.id.toString).array.toList}")
    segments.tryFoldLeft(0, failFast = false) {
      case (deleteCount, segment) =>
        //        println(s"Deleting ${segment.id}")
        segment.delete.map {
          _ =>
            deleteCount + 1
        }
    }
  }

  def tempMinMaxKeyValues(map: Map[Slice[Byte], Value]): Slice[KeyValueWriteOnly] = {
    for {
      minKey <- map.head
      maxKey <- map.last
    } yield
      Slice(Transient.Put(minKey._1), Transient.Put(maxKey._1))
  } getOrElse Slice.create[KeyValueWriteOnly](0)

  def tempMinMaxKeyValues(segments: Iterable[Segment]): Slice[KeyValueWriteOnly] =
    segments.foldLeft(Slice.create[KeyValueWriteOnly](segments.size * 2)) {
      case (keyValues, segment) =>
        keyValues add Transient.Put(segment.minKey)
        keyValues add Transient.Put(segment.maxKey)
    }

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    busySegments.nonEmpty &&
      Segment.overlaps(
        segments1 = busySegments,
        segments2 =
          SegmentAssigner.assign(
            inputSegments = inputSegments,
            targetSegments = appendixSegments
          )
      ).nonEmpty

  def overlapsWithBusySegments(map: Map[Slice[Byte], Value],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    busySegments.nonEmpty &&
      Segment.overlaps(
        segments1 = busySegments,
        segments2 =
          SegmentAssigner.assign(
            map = map,
            targetSegments = appendixSegments
          )
      ).nonEmpty
}

private[core] trait Segment {
  private[segment] val cache: ConcurrentSkipListMap[Slice[Byte], _]
  val minKey: Slice[Byte]
  val maxKey: Slice[Byte]
  val segmentSize: Int
  val removeDeletes: Boolean

  def getBloomFilter: Try[BloomFilter[Slice[Byte]]]

  def path: Path

  def put(newKeyValues: Slice[KeyValueWriteOnly],
          minSegmentSize: Long,
          bloomFilterFalsePositiveRate: Double,
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Try[Slice[Segment]]

  def copyTo(toPath: Path): Try[Path]

  def getFromCache(key: Slice[Byte]): Option[PersistentReadOnly]

  def mightContain(key: Slice[Byte]): Try[Boolean]

  def get(key: Slice[Byte]): Try[Option[PersistentReadOnly]]

  def lower(key: Slice[Byte]): Try[Option[PersistentReadOnly]]

  def higher(key: Slice[Byte]): Try[Option[PersistentReadOnly]]

  def getAll(bloomFilterFalsePositiveRate: Double, addTo: Option[Slice[Persistent]] = None): Try[Slice[Persistent]]

  def delete: Try[Unit]

  def close: Try[Unit]

  def getKeyValueCount(): Try[Int]

  def clearCache(): Unit =
    cache.clear()

  def removeFromCache(key: Slice[Byte]) =
    cache.remove(key)

  def isInCache(key: Slice[Byte]): Boolean =
    cache containsKey key

  def isCacheEmpty: Boolean =
    cache.isEmpty

  def cacheSize: Int =
    cache.size()

  def isOpen: Boolean

  def isFileDefined: Boolean

  def memory: Boolean

  def persistent: Boolean

  def existsOnDisk: Boolean

}
