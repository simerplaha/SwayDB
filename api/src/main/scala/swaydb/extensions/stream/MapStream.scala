///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.extensions.stream
//
//import swaydb.data.io.Tag.Core.IO
//import swaydb.{IO, Streamed}
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.extensions.Key
//import swaydb.serializers.Serializer
//
//import scala.annotation.tailrec
//
///**
//  * TODO - [[MapStream]] and [[MapKeysStream]] are similar and need a higher type - tagless final.
//  *
//  * Sample order
//  *
//  * Key.MapStart(1),
//  *   MapKey.EntriesStart(1)
//  *     MapKey.Entry(1, 1)
//  *   MapKey.EntriesEnd(1)
//  *   MapKey.SubMapsStart(1)
//  *     MapKey.SubMap(1, 1000)
//  *   MapKey.SubMapsEnd(1)
//  * MapKey.End(1)
//  **/
//
//object MapStream {
//
//  //type casting here because a value set by the User will/should always have a serializer. If the inner value if
//  //Option[V] then the full value would be Option[Option[V]] and the serializer is expected to handle serialization for
//  //the inner Option[V] which would be of the User's type V.
//  private def toKV[K, V](keyValue: (Key[K], Option[V]))(implicit optionValueSerializer: Serializer[Option[V]]): (K, V) = {
//    val value = keyValue._2.getOrElse(optionValueSerializer.read(Slice.emptyBytes).asInstanceOf[V])
//    keyValue._1 match {
//      case key: Key.MapEntry[K] =>
//        (key.dataKey, value)
//
//      case key: Key.SubMap[K] =>
//        (key.dataKey, value)
//
//      //FIXME: make this is not type-safe. This should never occur.
//      case keyValue =>
//        throw new Exception(s"Can serialise toKV: $keyValue")
//    }
//  }
//
//  private[stream] def checkStep[K](key: Key[K],
//                                   isReverse: Boolean,
//                                   mapsOnly: Boolean,
//                                   thisMapKeyBytes: Slice[Byte])(implicit keySerializer: Serializer[K]): Step =
//    if (KeyOrder.default.compare(Key.writeKeys(key.parentMapKeys, keySerializer), thisMapKeyBytes) != 0) //Exit if it's moved onto another map
//      Step.Stop
//    else
//      key match {
//        case Key.MapStart(_) =>
//          if (isReverse) //exit iteration if it's going backward since Start is the head key
//            Step.Stop
//          else
//            Step.Next
//
//        case Key.MapEntriesStart(_) =>
//          if (isReverse) //exit iteration if it's going backward as previous entry is pointer entry Start
//            Step.Stop
//          else
//            Step.Next
//
//        case Key.MapEntry(_, _) =>
//          if (mapsOnly)
//            if (isReverse) //Exit if it's fetching subMaps only it's already reached an entry means it's has already crossed subMaps block
//              Step.Stop
//            else
//              Step.Next
//          else
//            Step.Success
//
//        case Key.MapEntriesEnd(_) =>
//          //if it's not going backwards and it's trying to fetch subMaps only then move forward
//          if (!isReverse && !mapsOnly)
//            Step.Stop
//          else
//            Step.Next
//
//        case Key.SubMapsStart(_) =>
//          //if it's not going backward and it's trying to fetch subMaps only then move forward
//          if (!isReverse && !mapsOnly)
//            Step.Stop
//          else
//            Step.Next
//
//        case Key.SubMap(_, _) =>
//          if (!mapsOnly) //if subMaps are excluded
//            if (isReverse) //if subMaps are excluded & it's going in reverse continue iteration.
//              Step.Next
//            else //if it's going forward with subMaps excluded then end iteration as it's already iterated all key-value entries.
//              Step.Stop
//          else
//            Step.Success
//
//        case Key.SubMapsEnd(_) =>
//          //Exit if it's not going forward.
//          if (!isReverse)
//            Step.Stop
//          else
//            Step.Next
//
//        case Key.MapEnd(_) =>
//          //Exit if it's not going in reverse.
//          if (!isReverse)
//            Step.Stop
//          else
//            Step.Next
//      }
//
//  @tailrec
//  private def step[K, V](stream: swaydb.Stream[(Key[K], Option[V]), IO],
//                         previous: (Key[K], Option[V]),
//                         isReverse: Boolean,
//                         mapsOnly: Boolean,
//                         thisMapKeyBytes: Slice[Byte])(implicit keySerializer: Serializer[K]): IO[Core.Error, Option[(Key[K], Option[V])]] =
//    stream.next(previous) match {
//      case IO.Success(some @ Some((key, value))) =>
//        checkStep(key = key, isReverse = isReverse, mapsOnly = mapsOnly, thisMapKeyBytes = thisMapKeyBytes) match {
//          case Step.Stop =>
//            IO.none
//
//          case Step.Next =>
//            stream.next(key, value) match {
//              case IO.Success(Some(keyValue)) =>
//                step(
//                  stream = stream,
//                  previous = keyValue,
//                  isReverse = isReverse,
//                  mapsOnly = mapsOnly,
//                  thisMapKeyBytes = thisMapKeyBytes
//                )
//
//              case IO.Success(None) =>
//                IO.none
//
//              case IO.Failure(error) =>
//                IO.Failure(error)
//            }
//
//          case Step.Success =>
//            IO.Success(some)
//        }
//
//      case IO.Success(None) =>
//        IO.none
//
//      case IO.Failure(error) =>
//        IO.Failure(error)
//    }
//}
//
//case class MapStream[K, V](mapKey: Seq[K],
//                           mapsOnly: Boolean = false,
//                           userDefinedFrom: Boolean = false,
//                           map: swaydb.Map[Key[K], Option[V], IO])(implicit keySerializer: Serializer[K],
//                                                                   mapKeySerializer: Serializer[Key[K]],
//                                                                   optionValueSerializer: Serializer[Option[V]]) extends Streamed[(K, V), Core.IO] { self =>
//
//  private val endEntriesKey = Key.MapEntriesEnd(mapKey)
//  private val endSubMapsKey = Key.SubMapsEnd(mapKey)
//
//  private val thisMapKeyBytes = Key.writeKeys(mapKey, keySerializer)
//
//  def from(key: K): MapStream[K, V] =
//    if (mapsOnly)
//      copy(map = map.from(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
//    else
//      copy(map = map.from(Key.MapEntry(mapKey, key)), userDefinedFrom = true)
//
//  def before(key: K): MapStream[K, V] =
//    if (mapsOnly)
//      copy(map = map.before(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
//    else
//      copy(map = map.before(Key.MapEntry(mapKey, key)), userDefinedFrom = true)
//
//  def fromOrBefore(key: K): MapStream[K, V] =
//    if (mapsOnly)
//      copy(map = map.fromOrBefore(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
//    else
//      copy(map = map.fromOrBefore(Key.MapEntry(mapKey, key)), userDefinedFrom = true)
//
//  def after(key: K): MapStream[K, V] =
//    if (mapsOnly)
//      copy(map = map.after(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
//    else
//      copy(map = map.after(Key.MapEntry(mapKey, key)), userDefinedFrom = true)
//
//  def fromOrAfter(key: K): MapStream[K, V] =
//    if (mapsOnly)
//      copy(map = map.fromOrAfter(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
//    else
//      copy(map = map.fromOrAfter(Key.MapEntry(mapKey, key)), userDefinedFrom = true)
//
//  private def before(key: Key[K], reverse: Boolean): MapStream[K, V] =
//    copy(map = map.before(key).copy(reverseIteration = reverse))
//
//  private def reverse(reverse: Boolean): MapStream[K, V] =
//    copy(map = map.copy(reverseIteration = reverse))
//
//  def isReverse: Boolean =
//    self.map.reverseIteration
//
//  private def headOptionInner: IO[Core.Error, Option[(Key[K], Option[V])]] = {
//    val stream = map.stream
//    map.headOption match {
//      case IO.Success(someKeyValue @ Some(keyValue @ (key, _))) =>
//        MapStream.checkStep(
//          key = key,
//          isReverse = map.reverseIteration,
//          mapsOnly = mapsOnly,
//          thisMapKeyBytes = thisMapKeyBytes
//        ) match {
//          case Step.Stop =>
//            IO.none
//
//          case Step.Next =>
//            MapStream.step(
//              stream = stream,
//              previous = keyValue,
//              isReverse = map.reverseIteration,
//              mapsOnly = mapsOnly,
//              thisMapKeyBytes = thisMapKeyBytes
//            )
//
//          case Step.Success =>
//            IO.Success(someKeyValue)
//        }
//
//      case IO.Success(None) =>
//        IO.none
//
//      case IO.Failure(error) =>
//        IO.Failure(error)
//    }
//  }
//
//  override def headOption: IO[Core.Error, Option[(K, V)]] =
//    headOptionInner.map(_.map(MapStream.toKV(_)))
//
//  override def drop(count: Int): swaydb.Stream[(K, V), IO] =
//    stream drop count
//
//  override def dropWhile(f: ((K, V)) => Boolean): swaydb.Stream[(K, V), IO] =
//    stream dropWhile f
//
//  override def take(count: Int): swaydb.Stream[(K, V), IO] =
//    stream take count
//
//  override def takeWhile(f: ((K, V)) => Boolean): swaydb.Stream[(K, V), IO] =
//    stream takeWhile f
//
//  override def map[B](f: ((K, V)) => B): swaydb.Stream[B, IO] =
//    stream map f
//
//  override def flatMap[B](f: ((K, V)) => swaydb.Stream[B, IO]): swaydb.Stream[B, IO] =
//    stream flatMap f
//
//  override def foreach[U](f: ((K, V)) => U): swaydb.Stream[Unit, IO] =
//    stream foreach f
//
//  override def filter(f: ((K, V)) => Boolean): swaydb.Stream[(K, V), IO] =
//    stream filter f
//
//  override def filterNot(f: ((K, V)) => Boolean): swaydb.Stream[(K, V), IO] =
//    stream filterNot f
//
//  override def foldLeft[B](initial: B)(f: (B, (K, V)) => B): IO[B] =
//    stream.foldLeft(initial)(f)
//
//  override def size: IO[Int] =
//    map.keys.size
//
//  def stream: swaydb.Stream[(K, V), IO] =
//    new swaydb.Stream[(K, V), IO] {
//      /**
//        * Stores raw key-value from previous read. This is a temporary solution because
//        * this class extends Stream[(K, V)] and the types are being lost on stream.next here since previous
//        * (Key[K], Option[V]) is not known.
//        */
//      private var previousRaw: (Key[K], Option[V]) = _
//
//      override def headOption: IO[Core.Error, Option[(K, V)]] =
//        self.headOptionInner.map(_.map {
//          raw =>
//            previousRaw = raw
//            MapStream.toKV(raw)
//        })
//
//      override private[swaydb] def next(previous: (K, V)): IO[Core.Error, Option[(K, V)]] =
//        MapStream.step(
//          stream = self.map.stream,
//          previous = previousRaw,
//          isReverse = isReverse,
//          mapsOnly = mapsOnly,
//          thisMapKeyBytes = thisMapKeyBytes
//        ).map(_.map {
//          raw =>
//            previousRaw = raw
//            MapStream.toKV(raw)
//        })
//    }
//
//  /**
//    * Returns the start key when doing reverse iteration.
//    *
//    * If subMaps are included then it will return the starting point to be [[Key.SubMapsEnd]]
//    * which will iterate backward until [[Key.MapEntriesStart]]
//    * else returns the starting point to be [[Key.MapEntriesEnd]] to fetch entries only.
//    */
//  def reverse: MapStream[K, V] =
//    if (userDefinedFrom) //if user has defined from then do not override it and just set reverse to true.
//      reverse(reverse = true)
//    else if (mapsOnly) //if from is not already set & map are included in the iteration then start from subMap's last key
//      before(key = endSubMapsKey, reverse = true)
//    else //if subMaps are excluded, start from key's last key.
//      before(key = endEntriesKey, reverse = true)
//
//  /**
//    * lastOption should always force formKey to be the [[endSubMapsKey]]
//    * because from is always set in [[swaydb.extensions.Maps]] and regardless from where the iteration starts the
//    * most efficient way to fetch the last is from the key [[endSubMapsKey]].
//    */
//  override def lastOption: IO[Core.Error, Option[(K, V)]] =
//    reverse.headOption
//
//  override def toString(): String =
//    classOf[MapStream[_, _]].getClass.getSimpleName
//}
