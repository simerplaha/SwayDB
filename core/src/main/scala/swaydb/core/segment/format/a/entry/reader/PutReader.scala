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

package swaydb.core.segment.format.a.entry.reader

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.cache.Cache
import swaydb.core.data.Persistent
import swaydb.core.data.Persistent.Partial.Key
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, KeyValueId}
import swaydb.data.slice.ReaderBase

object PutReader extends SortedIndexEntryReader[Persistent.Put] {

  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexAccessPosition: Int,
                              binarySearchIndexPosition: Int,
                              keyInfo: Option[Either[Int, Persistent.Partial.Key]],
                              indexReader: ReaderBase[swaydb.Error.Segment],
                              valueCache: Option[Cache[swaydb.Error.Segment, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]]],
                              indexOffset: Int,
                              nextIndexOffset: Int,
                              nextIndexSize: Int,
                              previous: Option[Persistent.Partial])(implicit timeReader: TimeReader[T],
                                                                    deadlineReader: DeadlineReader[T],
                                                                    valueOffsetReader: ValueOffsetReader[T],
                                                                    valueLengthReader: ValueLengthReader[T],
                                                                    valueBytesReader: ValueReader[T]): IO[swaydb.Error.Segment, Persistent.Put] =
    deadlineReader.read(indexReader, previous) flatMap {
      deadline =>
        valueBytesReader.read(indexReader, previous) flatMap {
          valueOffsetAndLength =>
            timeReader.read(indexReader, previous) flatMap {
              time =>
                keyInfo match {
                  case Some(keyInfo) =>
                    keyInfo match {
                      case Left(keySize) =>
                        KeyReader.read(
                          keyValueIdInt = keyValueId,
                          indexReader = indexReader,
                          keySize = Some(keySize),
                          previous = previous,
                          keyValueId = KeyValueId.Put
                        ) map {
                          key =>
                            //                    if (valueLength > 0 && valueCache.isEmpty)
                            //                      ValuesBlock.valuesBlockNotInitialised
                            //                    else
                            val valueLength = valueOffsetAndLength.map(_._2).getOrElse(0)
                            val valueOffset = valueOffsetAndLength.map(_._1).getOrElse(-1)

                            Persistent.Put.fromCache(
                              key = key,
                              deadline = deadline,
                              valueCache = valueCache getOrElse Cache.emptyValuesBlock,
                              time = time,
                              nextIndexOffset = nextIndexOffset,
                              nextIndexSize = nextIndexSize,
                              indexOffset = indexOffset,
                              valueOffset = valueOffset,
                              valueLength = valueLength,
                              sortedIndexAccessPosition = sortedIndexAccessPosition,
                              binarySearchIndexPosition = binarySearchIndexPosition
                            )
                        }

                      case Right(key) =>
                        key match {
                          case fixed: Key.Fixed =>
                            //                    if (valueLength > 0 && valueCache.isEmpty)
                            //                      ValuesBlock.valuesBlockNotInitialised
                            //                    else
                            val valueLength = valueOffsetAndLength.map(_._2).getOrElse(0)
                            val valueOffset = valueOffsetAndLength.map(_._1).getOrElse(-1)

                            IO.Right {
                              Persistent.Put.fromCache(
                                key = fixed.key,
                                deadline = deadline,
                                valueCache = valueCache getOrElse Cache.emptyValuesBlock,
                                time = time,
                                nextIndexOffset = nextIndexOffset,
                                nextIndexSize = nextIndexSize,
                                indexOffset = indexOffset,
                                valueOffset = valueOffset,
                                valueLength = valueLength,
                                sortedIndexAccessPosition = sortedIndexAccessPosition,
                                binarySearchIndexPosition = binarySearchIndexPosition
                              )
                            }

                          case key @ (_: Key.Range | _: Key.Group) =>
                            IO.failed(s"Expected Fixed key. Actual: ${key.getClass.getSimpleName}")
                        }
                    }

                  case None =>
                    KeyReader.read(
                      keyValueIdInt = keyValueId,
                      indexReader = indexReader,
                      keySize = None,
                      previous = previous,
                      keyValueId = KeyValueId.Put
                    ) map {
                      key =>
                        val valueLength = valueOffsetAndLength.map(_._2).getOrElse(0)
                        val valueOffset = valueOffsetAndLength.map(_._1).getOrElse(-1)

                        Persistent.Put.fromCache(
                          key = key,
                          deadline = deadline,
                          valueCache = valueCache getOrElse Cache.emptyValuesBlock,
                          time = time,
                          nextIndexOffset = nextIndexOffset,
                          nextIndexSize = nextIndexSize,
                          indexOffset = indexOffset,
                          valueOffset = valueOffset,
                          valueLength = valueLength,
                          sortedIndexAccessPosition = sortedIndexAccessPosition,
                          binarySearchIndexPosition = binarySearchIndexPosition
                        )
                    }
                }
            }
        }
    }
}
