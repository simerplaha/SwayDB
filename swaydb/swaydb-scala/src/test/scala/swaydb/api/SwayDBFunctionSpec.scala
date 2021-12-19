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
//package swaydb.api
//
//import swaydb.effect.IOValues._
//import swaydb.PureFunctionScala._
//import swaydb.config.Functions
//import swaydb.core.CommonAssertions._
//import swaydb.core.CoreTestSweeper._
//import swaydb.core.CoreTestData._
//import swaydb.core.CoreTestSweeper
//import swaydb.macros.Sealed
//import swaydb.serializers.Default._
//import swaydb.serializers.Serializer
//import swaydb.testkit.RunThis._
//import swaydb.{Apply, IO, Prepare, PureFunction, StorageIntImplicits}
//
//import scala.collection.parallel.CollectionConverters._
//import scala.concurrent.duration.DurationInt
//import swaydb.testkit.TestKit._
//import swaydb.core.file.CoreFileTestKit._
//
//sealed trait Key
//object Key {
//
//  case class Id(id: Int) extends Key
//  sealed trait Function extends Key
//
//  case object IncrementValue extends Key.Function with OnValue[Int] {
//    override val id: String =
//      "1"
//
//    override def apply(value: Int): Apply.Map[Int] =
//      Apply.Update(value + 1)
//  }
//
//  case object DoNothing extends Key.Function with OnKeyValue[Key, Int] {
//    override val id: String =
//      "2"
//
//    override def apply(key: Key, value: Int): Apply.Map[Int] =
//      Apply.Nothing
//  }
//
//  import boopickle.Default._
//
//  implicit val serializer = swaydb.serializers.BooPickle[Key]
//}
//
//class SwayDBFunctionSpec0 extends SwayDBFunctionSpec {
//
//  def newDB[K, V]()(implicit functionStore: Functions[PureFunction.Map[K, V]],
//                    keySerializer: Serializer[K],
//                    valueSerializer: Serializer[V],
//                    sweeper: CoreTestSweeper): swaydb.Map[K, V, PureFunction.Map[K, V], IO.ApiIO] =
//    swaydb.persistent.Map[K, V, PureFunction.Map[K, V], IO.ApiIO](genDirPath()).right.value.sweep(_.delete().get)
//}
//
//class SwayDBFunctionSpec1 extends SwayDBFunctionSpec {
//
//  def newDB[K, V]()(implicit functionStore: Functions[PureFunction.Map[K, V]],
//                    keySerializer: Serializer[K],
//                    valueSerializer: Serializer[V],
//                    sweeper: CoreTestSweeper): swaydb.Map[K, V, PureFunction.Map[K, V], IO.ApiIO] =
//    swaydb.persistent.Map[K, V, PureFunction.Map[K, V], IO.ApiIO](genDirPath(), logSize = 1.byte).right.value.sweep(_.delete().get)
//}
//
//class SwayDBFunctionSpec2 extends SwayDBFunctionSpec {
//
//  def newDB[K, V]()(implicit functionStore: Functions[PureFunction.Map[K, V]],
//                    keySerializer: Serializer[K],
//                    valueSerializer: Serializer[V],
//                    sweeper: CoreTestSweeper): swaydb.Map[K, V, PureFunction.Map[K, V], IO.ApiIO] =
//    swaydb.memory.Map[K, V, PureFunction.Map[K, V], IO.ApiIO](logSize = 1.byte).right.value.sweep(_.delete().get)
//}
//
//class SwayDBFunctionSpec3 extends SwayDBFunctionSpec {
//
//  def newDB[K, V]()(implicit functionStore: Functions[PureFunction.Map[K, V]],
//                    keySerializer: Serializer[K],
//                    valueSerializer: Serializer[V],
//                    sweeper: CoreTestSweeper): swaydb.Map[K, V, PureFunction.Map[K, V], IO.ApiIO] =
//    swaydb.memory.Map[K, V, PureFunction.Map[K, V], IO.ApiIO]().right.value.sweep(_.delete().get)
//}
//
////class SwayDBFunctionSpec4 extends SwayDBFunctionSpec {
////
////  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
////    swaydb.memory.zero.Map[Key, Int, Key.Function, IO.ApiIO](logSize = 1.byte).right.value
////}
////
////class SwayDBFunctionSpec5 extends SwayDBFunctionSpec {
////  override def newDB(): Map[Key, Int, Key.Function, IO.ApiIO] =
////    swaydb.memory.zero.Map[Key, Int, Key.Function, IO.ApiIO]().right.value
////}
//
//sealed trait SwayDBFunctionSpec extends ACoreSpec {
//
//  def newDB[K, V]()(implicit functionStore: Functions[PureFunction.Map[K, V]],
//                    keySerializer: Serializer[K],
//                    valueSerializer: Serializer[V],
//                    sweeper: CoreTestSweeper): swaydb.Map[K, V, PureFunction.Map[K, V], IO.ApiIO]
//
//  "it" should {
//
//    val functions = MacroSealed.list[Key.Function].collect { case function: PureFunction.Map[Key, Int] => function }
//    implicit val functionsMap = Functions[PureFunction.Map[Key, Int]](functions)
//
//    "perform concurrent atomic updates to a single key" in {
//      runThis(times = repeatTest, log = true) {
//        CoreTestSweeper {
//          implicit sweeper =>
//
//            val db = newDB()
//
//            db.put(Key.Id(1), 0).get
//
//            (1 to 1000).par foreach {
//              _ =>
//                db.applyFunction(Key.Id(1), Key.IncrementValue).get
//            }
//
//            db.get(Key.Id(1)).get should contain(1000)
//        }
//      }
//    }
//
//    "perform concurrent atomic updates to multiple keys" in {
//      runThis(times = repeatTest, log = true) {
//        CoreTestSweeper {
//          implicit sweeper =>
//            val db = newDB()
//
//            (1 to 1000) foreach {
//              i =>
//                db.put(Key.Id(i), 0).get
//            }
//
//            (1 to 100).par foreach {
//              _ =>
//                (1 to 1000).par foreach {
//                  i =>
//                    db.applyFunction(Key.Id(i), Key.IncrementValue).get
//                }
//            }
//
//            (1 to 1000).par foreach {
//              i =>
//                db.get(Key.Id(i)).get should contain(100)
//            }
//
//        }
//      }
//    }
//
//    "batch commit updates" in {
//      runThis(times = repeatTest, log = true) {
//        CoreTestSweeper {
//          implicit sweeper =>
//            val db = newDB()
//
//            val puts: List[Prepare[Key.Id, Int, Nothing]] =
//              (1 to 1000).map(key => Prepare.Put(Key.Id(key), key)).toList
//
//            db.commit(puts).get
//
//            val prepareApplyFunction: List[Prepare[Key.Id, Nothing, Key.IncrementValue.type]] =
//              (1 to 1000).map(key => Prepare.ApplyFunction(Key.Id(key), Key.IncrementValue)).toList
//
//            db.commit(prepareApplyFunction).get
//
//            (1 to 1000) foreach {
//              key =>
//                db.get(Key.Id(key)).get should contain(key + 1)
//            }
//
//        }
//      }
//    }
//
//    "Nothing should not update data" in {
//      runThis(times = repeatTest, log = true) {
//        CoreTestSweeper {
//          implicit sweeper =>
//            val db = newDB()
//
//            (1 to 1000) foreach {
//              i =>
//                db.put(Key.Id(i), 0).get
//            }
//
//            (1 to 100).par foreach {
//              _ =>
//                (1 to 1000).par foreach {
//                  i =>
//                    db.applyFunction(Key.Id(i), Key.DoNothing).get
//                }
//            }
//
//            (1 to 1000).par foreach {
//              i =>
//                db.get(Key.Id(i)).get should contain(0)
//            }
//        }
//      }
//    }
//  }
//
//  "apply all functions" in {
//    case class Key(int: Int)
//    case class Value(someValue: String)
//
//    import boopickle.Default._
//    implicit val keySerializer = swaydb.serializers.BooPickle[Key]
//    implicit val valueSerializer = swaydb.serializers.BooPickle[Value]
//
//    //test head should have 1
//    val onKey: OnKey[Key, Value] = _ => Apply.Update(Value(1 + randomString()))
//    //test head should have 2
//    val onKeyDeadline: OnKeyDeadline[Key, Value] = (key, _) => Apply.Update(Value(2 + " " + key.int + " set key"))
//    //test head should have 3
//    val onKeyValue: OnKeyValue[Key, Value] = (key, value) => Apply.Update(Value(3 + " " + value.someValue + " " + randomString()))
//    //test head should have 4
//    val onValue: OnValue[Value] = value => Apply.Update(Value(4 + " " + value.someValue + " " + randomString()))
//    //test head should have original value but deadline is set
//    val onValueDeadline: OnValueDeadline[Value] = (_, deadline) => Apply.Expire(deadline.map(_ + 10.seconds).getOrElse(10.seconds.fromNow))
//    //test no change
//    val onKeyValueDeadline: OnKeyValueDeadline[Key, Value] = (_, _, _) => Apply.Nothing
//
//    CoreTestSweeper {
//      implicit sweeper =>
//
//        implicit val functions = Functions(onKey, onKeyDeadline, onKeyValue, onValue, onValueDeadline, onKeyValueDeadline)
//        val map = newDB[Key, Value]()
//
//        (1 to 60).foreach(i => map.put(Key(i), Value("")))
//
//        /**
//         * Write using range or individual
//         */
//        eitherOne(
//          (1 to 10).foreach(i => map.applyFunction(Key(i), onKey)),
//          map.applyFunction(Key(1), Key(10), onKey)
//        )
//
//        eitherOne(
//          (11 to 20).foreach(i => map.applyFunction(Key(i), onKeyDeadline)),
//          map.applyFunction(Key(11), Key(20), onKeyDeadline)
//        )
//
//        eitherOne(
//          (21 to 30).foreach(i => map.applyFunction(Key(i), onKeyValue)),
//          map.applyFunction(Key(21), Key(30), onKeyValue)
//        )
//
//        eitherOne(
//          (31 to 40).foreach(i => map.applyFunction(Key(i), onValue)),
//          map.applyFunction(Key(31), Key(40), onValue)
//        )
//
//        eitherOne(
//          (41 to 50).foreach(i => map.applyFunction(Key(i), onValueDeadline)),
//          map.applyFunction(Key(41), Key(50), onValueDeadline)
//        )
//
//        eitherOne(
//          (51 to 60).foreach(i => map.applyFunction(Key(i), onKeyValueDeadline)),
//          map.applyFunction(Key(51), Key(60), onKeyValueDeadline)
//        )
//
//        /**
//         * Assert
//         */
//        (1 to 10) foreach {
//          i =>
//            map.get(Key(i)).get.get.someValue should startWith("1")
//            map.expiration(Key(i)).value shouldBe empty
//        }
//
//        (11 to 20) foreach {
//          i =>
//            map.get(Key(i)).get.get.someValue should startWith("2")
//            map.expiration(Key(i)).value shouldBe empty
//        }
//
//        (21 to 30) foreach {
//          i =>
//            map.get(Key(i)).get.get.someValue should startWith("3")
//            map.expiration(Key(i)).value shouldBe empty
//        }
//
//        (31 to 40) foreach {
//          i =>
//            map.get(Key(i)).get.get.someValue should startWith("4")
//            map.expiration(Key(i)).value shouldBe empty
//        }
//
//        (41 to 50) foreach {
//          i =>
//            map.get(Key(i)).get.get.someValue shouldBe empty
//            map.expiration(Key(i)).value shouldBe defined
//        }
//
//        (51 to 60) foreach {
//          i =>
//            map.get(Key(i)).get.get.someValue shouldBe empty
//            map.expiration(Key(i)).value shouldBe empty
//        }
//    }
//  }
//
//}
