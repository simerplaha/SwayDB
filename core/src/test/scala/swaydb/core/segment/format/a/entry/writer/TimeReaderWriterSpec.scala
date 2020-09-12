///*
// * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// *  it under the terms of the GNU Affero General Public License as
// *  published by the Free Software Foundation, either version 3 of the
// *  License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a.entry.writer
//
//import org.scalatest.matchers.should.Matchers

import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.{Memory, Persistent, Time}
//import swaydb.core.io.reader.Reader
//import swaydb.core.segment.format.a.entry.id.BaseEntryId
//import swaydb.core.segment.format.a.entry.reader.TimeReader
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class TimeReaderWriterSpec extends AnyWordSpec with Matchers {
//
//  val keyIds =
//    allBaseEntryIds collect {
//      case entryId: BaseEntryId.Key =>
//        entryId
//    }
//
//  keyIds should not be empty
//
//  "writePartiallyCompressed" should {
//    "write compressed time" in {
//      keyIds foreach {
//        keyId =>
//          (3 to 8) foreach { //for some or all deadline bytes compressed.
//            commonBytes =>
//              val currentTimeBytes =
//                Slice.fill[Byte](commonBytes)(0.toByte) ++ Slice.fill[Byte](8 - commonBytes)(1.toByte)
//
//              val previousTimeBytes =
//                Slice.fill[Byte](commonBytes)(0.toByte) ++ Slice.fill[Byte](8 - commonBytes)(2.toByte)
//
//              val current: Memory.Put =
//                randomPutKeyValue(key = 1, value = None, deadline = None)(TestTimer.single(Time(currentTimeBytes)))
//                  .toTransient(
//                    previous =
//                      Some(randomFixedKeyValue(0)(TestTimer.single(Time(previousTimeBytes))).toTransient)
//                  ).asInstanceOf[Memory.Put]
//
//              val currentTime = TimeWriter.getTime(current)
//
//              implicit val put = TransientToKeyValueIdBinder.PutBinder
//
//              val writeResult =
//                TimeWriter.write(
//                  current = current,
//                  currentTime = currentTime,
//                  compressDuplicateValues = false,
//                  enablePrefixCompression = true,
//                  entryId = keyId,
//                  plusSize = 0,
//                  isKeyCompressed = true,
//                  hasPrefixCompression = randomBoolean()
//                )
//
//              val reader = Reader(writeResult.indexBytes)
//
//              val expectedEntryID = put.keyValueId.adjustBaseIdToKeyValueIdKey_Compressed(keyId.timePartiallyCompressed.noValue.noDeadline.baseId)
//              reader.readUnsignedInt() shouldBe expectedEntryID
//
//              val previous =
//                Persistent.Put(
//                  _key = 1,
//                  deadline = current.previous.flatMap(_.deadline),
//                  valueCache = null,
//                  _time = Time(previousTimeBytes),
//                  nextIndexOffset = 0,
//                  nextKeySize = 0,
//                  indexOffset = 0,
//                  valueOffset = 0,
//                  valueLength = 0,
//                  sortedIndexAccessPosition = 0
//                )
//
//              TimeReader.PartiallyCompressedTimeReader.read(
//                indexReader = reader,
//                previous = Some(previous),
//              ) shouldBe currentTime
//          }
//      }
//    }
//  }
//
//  "writeUncompressed" should {
//    "write compressed time" in {
//      keyIds foreach {
//        keyId =>
//          val current: Memory.Put =
//            randomPutKeyValue(1, None, None)(TestTimer.randomNonEmpty)
//              .toTransient(
//                previous =
//                  Some(randomFixedKeyValue(0)(TestTimer.Empty).toTransient)
//              ).asInstanceOf[Memory.Put]
//
//          val currentTime = TimeWriter.getTime(current)
//
//          implicit val put = TransientToKeyValueIdBinder.PutBinder
//
//          val writeResult =
//            TimeWriter.write(
//              current = current,
//              currentTime = currentTime,
//              compressDuplicateValues = randomBoolean(),
//              enablePrefixCompression = true,
//              entryId = keyId,
//              plusSize = 0,
//              isKeyCompressed = true,
//              hasPrefixCompression = randomBoolean()
//            )
//
//          val reader = Reader(writeResult.indexBytes)
//
//          val expectedEntryID = put.keyValueId.adjustBaseIdToKeyValueIdKey_Compressed(keyId.timeUncompressed.noValue.noDeadline.baseId)
//          reader.readUnsignedInt() shouldBe expectedEntryID
//
//          val previous =
//            Persistent.Put(
//              _key = 0,
//              deadline = current.previous.flatMap(_.deadline),
//              valueCache = null,
//              _time = TestTimer.random.next,
//              nextIndexOffset = 0,
//              nextKeySize = 0,
//              indexOffset = 0,
//              valueOffset = 0,
//              valueLength = 0,
//              sortedIndexAccessPosition = 0
//            )
//
//          TimeReader.UnCompressedTimeReader.read(
//            indexReader = reader,
//            previous =
//              //doesn't matter if previous is supplied or not
//              eitherOne(
//                Some(previous),
//                None
//              )
//          ) shouldBe currentTime
//      }
//    }
//  }
//
//  "noTime" should {
//    "write no time" in {
//      keyIds foreach {
//        keyId =>
//          TransientToKeyValueIdBinder.allBinders foreach {
//            implicit adjustedEntryId =>
//
//              val writeResult =
//                TimeWriter.write(
//                  current = randomPutKeyValue(1, None, None)(TestTimer.Empty).toTransient,
//                  currentTime = Time.empty,
//                  compressDuplicateValues = false,
//                  enablePrefixCompression = true,
//                  entryId = keyId,
//                  plusSize = 0,
//                  isKeyCompressed = true,
//                  hasPrefixCompression = randomBoolean()
//                )
//
//              val reader = Reader(writeResult.indexBytes)
//
//              val expectedEntryID = adjustedEntryId.keyValueId.adjustBaseIdToKeyValueIdKey_Compressed(keyId.noTime.noValue.noDeadline.baseId)
//              reader.readUnsignedInt() shouldBe expectedEntryID
//
//              val previous =
//                Persistent.Put(
//                  _key = 0,
//                  deadline = None,
//                  valueCache = null,
//                  _time = TestTimer.random.next,
//                  nextIndexOffset = 0,
//                  nextKeySize = 0,
//                  indexOffset = 0,
//                  valueOffset = 0,
//                  valueLength = 0,
//                  sortedIndexAccessPosition = 0
//                )
//
//              TimeReader.NoTimeReader.read(
//                indexReader = reader,
//                previous =
//                  //doesn't matter if previous is supplied or not
//                  eitherOne(
//                    Some(previous),
//                    None
//                  )
//              ) shouldBe Time.empty
//          }
//      }
//    }
//  }
//}
