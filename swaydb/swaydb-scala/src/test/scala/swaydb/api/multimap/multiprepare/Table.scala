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
//package swaydb.api.multimap.multiprepare
//
//import boopickle.Default.{Pickle, Unpickle, _}
//import swaydb.serializers.Serializer
//import swaydb.slice.Slice
//
///**
// * All Tables for [[MultiMapMultiPrepareSpec]].
// */
//sealed trait Table
//object Table {
//  sealed trait UserTables extends Table
//  sealed trait User extends UserTables
//  case object User extends User
//  sealed trait Activity extends UserTables
//  case object Activity extends Activity
//
//  sealed trait ProductTables extends Table
//  sealed trait Product extends ProductTables
//  case object Product extends Product
//  sealed trait Order extends ProductTables
//  case object Order extends Order
//
//  implicit val serializer = new Serializer[Table] {
//    override def write(data: Table): Slice[Byte] =
//      Slice.wrap(Pickle.intoBytes(data).array())
//
//    override def read(slice: Slice[Byte]): Table =
//      Unpickle[Table].fromBytes(slice.toByteBufferWrap())
//  }
//}
