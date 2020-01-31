///*
// * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
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
//package swaydb.memory.zero
//
//import com.typesafe.scalalogging.LazyLogging
//import swaydb.configs.level.DefaultMemoryZeroConfig
//import swaydb.core.Core
//import swaydb.core.function.FunctionStore
//import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//import swaydb.serializers.Serializer
//import swaydb.{Error, IO, KeyOrderConverter}
//
//import scala.reflect.ClassTag
//
//object Map extends LazyLogging {
//
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit def functionStore: FunctionStore = FunctionStore.memory()
//
//  /**
//   * A single level zero only database. Does not need compaction.
//   */
//
//  def apply[K, V, F, BAG[_]](mapSize: Int = 4.mb,
//                             acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
//                                                                                                   valueSerializer: Serializer[V],
//                                                                                                   functionClassTag: ClassTag[F],
//                                                                                                   bag: swaydb.Bag[BAG],
//                                                                                                   keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[K]] = Left(KeyOrder.default)): IO[Error.Boot, swaydb.Map[K, V, F, BAG]] = {
//    implicit val bytesKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytes(keyOrder)
//
//    Core(
//      enableTimer = functionClassTag != ClassTag.Nothing,
//      config = DefaultMemoryZeroConfig(
//        mapSize = mapSize,
//        acceleration = acceleration
//      )
//    ) map {
//      db =>
//        swaydb.Map[K, V, F, BAG](db.toBag)
//    }
//  }
//}
