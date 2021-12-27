///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.level
//
//import swaydb.effect.IOValues._
//import swaydb.config.MMAP
//import swaydb.core.CommonAssertions._
//import swaydb.core.CoreTestData._
//import swaydb.core.segment.data.{Memory, Value}
//import swaydb.core.segment.ref.search.ThreadReadState
//import swaydb.core.{ACoreSpec, GenForceSave}
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//import swaydb.utils.OperatingSystem
//import swaydb.testkit.TestKit._
//
//class LevelReadNoneSpec0 extends LevelReadNoneSpec
//
//class LevelReadNoneSpec1 extends LevelReadNoneSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = GenForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = GenForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = GenForceSave.mmap())
//}
//
//class LevelReadNoneSpec2 extends LevelReadNoneSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = GenForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = GenForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = GenForceSave.standard())
//}
//
//class LevelReadNoneSpec3 extends LevelReadNoneSpec {
//  override def isMemorySpec = true
//}
//
//sealed trait LevelReadNoneSpec extends AnyWordSpec {
//
//  //  override def deleteFiles = false
//
//  val keyValuesCount = 100
//
//  val times = 1
//
//  "return None" when {
//
//    "level is empty" in {
//      assertLevel(
//        level0KeyValues =
//          (_, _, _) =>
//            Slice.empty,
//
//        assertAllLevels =
//          (_, _, _, level) =>
//            Seq(
//              () => level.get(genStringOption(), ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty,
//              () => level.higher(genStringOption(), ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty,
//              () => level.lower(genStringOption(), ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty,
//              () => level.head(ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty,
//              () => level.last(ThreadReadState.random).runRandomIO.get.toOptionPut shouldBe empty
//            ).runThisRandomly()
//      )
//    }
//
//    "level is nonEmpty but contains no put" in {
//      runThis(times) {
//        assertLevel(
//          level0KeyValues =
//            (_, _, testTimer) =>
//              randomizedKeyValues(keyValuesCount, addPut = false, addUpdates = true, startId = Some(0))(testTimer),
//
//          assertAllLevels =
//            (level0KeyValues, _, _, level) =>
//              assertEmpty(level0KeyValues, level)
//        )
//      }
//    }
//
//    "level contains expired puts" in {
//      runThis(times) {
//        assertLevel(
//          level0KeyValues =
//            (_, _, testTimer) =>
//              (1 to keyValuesCount).map {
//                key =>
//                  randomPutKeyValue(key, deadline = Some(expiredDeadline()))(testTimer)
//              }.toSlice,
//
//          assertAllLevels =
//            (level0KeyValues, _, _, level) =>
//              assertEmpty(level0KeyValues, level)
//        )
//      }
//    }
//
//    "level is non empty but the searched key do not exist" in {
//      runThis(10.times) {
//        implicit val keyOrder = KeyOrder.integer
//
//        assertLevel(
//          level0KeyValues =
//            (_, _, testTimer) =>
//              randomizedKeyValues(keyValuesCount, startId = Some(1))(testTimer),
//
//          assertLevel0 =
//            (level0KeyValues, _, _, level) => {
//              val existing = unexpiredPuts(level0KeyValues)
//
//              import keyOrder._
//              val nonExistingKeys: List[Int] =
//                (level0KeyValues.head.key.readInt() - 100 to getMaxKey(level0KeyValues.last).maxKey.readInt() + 100)
//                  .filterNot(intKey => existing.exists(_.key equiv Slice.writeInt(intKey)))
//                  .toList
//
//              Seq(
//                () => assertGetNone(nonExistingKeys, level),
//                () => assertGet(existing, level),
//                () => assertReads(existing, level),
//                () => assertReads(existing, level),
//                () =>
//                  nonExistingKeys foreach {
//                    nonExistentKey =>
//                      val expectedHigher = existing.find(put => put.hasTimeLeft() && put.key.readInt() > nonExistentKey).map(_.key.readInt())
//                      level.higher(nonExistentKey, ThreadReadState.random).runRandomIO.get.toOptionPut.map(_.key.readInt()) shouldBe expectedHigher
//                  },
//                () =>
//                  nonExistingKeys foreach {
//                    nonExistentKey =>
//                      val expectedLower = existing.reverse.find(put => put.hasTimeLeft() && put.key.readInt() < nonExistentKey).map(_.key.readInt())
//                      level.lower(nonExistentKey, ThreadReadState.random).runRandomIO.get.toOptionPut.map(_.key.readInt()) shouldBe expectedLower
//                  }
//              ).runThisRandomlyInParallel()
//            }
//        )
//      }
//    }
//
//    "for remove ranges with expired Put fromValue" in {
//      runThis(times) {
//        assertLevel(
//          level0KeyValues =
//            (_, _, testTimer) => {
//              implicit val time = testTimer
//              Slice(
//                Memory.Range(1, 10, Value.put(1, expiredDeadline()), Value.remove(None)),
//                Memory.Range(20, 30, Value.put(2, expiredDeadline()), Value.remove(None))
//              )
//            },
//
//          assertAllLevels =
//            (keyValues, _, _, level) =>
//              assertEmpty(keyValues, level)
//        )
//      }
//    }
//
//    "put existed but was removed or expired" in {
//      runThis(times) {
//        assertLevel(
//
//          level0KeyValues =
//            (level1KeyValues, _, testTimer) => {
//              implicit val time = testTimer
//              eitherOne(
//                //either do a range remove
//                left =
//                  randomRemoveRanges(level1KeyValues).toSlice,
//                //or do fixed removes via function or fixed.
//                right =
//                  level1KeyValues mapToSlice {
//                    keyValue =>
//                      randomRemoveOrUpdateOrFunctionRemove(keyValue.key)
//                  }
//              )
//            },
//
//          level1KeyValues =
//            (level2KeyValues, testTimer) => {
//              implicit val time = testTimer
//              level2KeyValues should have size 0
//
//              randomPutKeyValues(keyValuesCount, startId = Some(0), addPutDeadlines = false, addExpiredPutDeadlines = false)
//            },
//
//          assertLevel0 =
//            (level0KeyValues, level1KeyValues, level2KeyValues, level) =>
//              assertEmpty(level1KeyValues, level),
//
//          assertLevel1 =
//            (level1KeyValues, level2KeyValues, level) =>
//              assertGet(level1KeyValues, level)
//        )
//      }
//    }
//  }
//}
