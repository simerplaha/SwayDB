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

package swaydb.core.queue

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Memory, Persistent}
import swaydb.core.queue.Command.{AddWeighed, WeighAndAdd}
import swaydb.data.slice.Slice

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.ref.WeakReference

private sealed trait Command {
  val keyValueRef: WeakReference[KeyValue.CacheAble]
  val skipListRef: WeakReference[ConcurrentSkipListMap[Slice[Byte], _]]
}

private object Command {

  case class WeighAndAdd(keyValueRef: WeakReference[Persistent.SegmentResponse],
                         skipListRef: WeakReference[ConcurrentSkipListMap[Slice[Byte], _]]) extends Command

  case class AddWeighed(keyValueRef: WeakReference[KeyValue.ReadOnly.Group],
                        skipListRef: WeakReference[ConcurrentSkipListMap[Slice[Byte], _]],
                        weight: Int) extends Command
}

private[core] object KeyValueLimiter {

  def apply(cacheSize: Long, delay: FiniteDuration)(implicit ex: ExecutionContext): KeyValueLimiter =
    new KeyValueLimiterImpl(
      cacheSize = cacheSize,
      delay = delay
    )

  val none: KeyValueLimiter = NoneKeyValueLimiter

  def weight(keyValue: Persistent.SegmentResponse) = {
    val otherBytes = (Math.ceil(keyValue.key.size + keyValue.valueLength / 8.0) - 1.0) * 8
    //        if (keyValue.hasRemoveMayBe) (168 + otherBytes).toLong else (264 + otherBytes).toLong
    ((264 * 2) + otherBytes).toLong
  }
}

private[core] sealed trait KeyValueLimiter {
  def add(keyValue: Persistent.SegmentResponse,
          skipList: ConcurrentSkipListMap[Slice[Byte], _]): Unit

  def add(keyValue: KeyValue.ReadOnly.Group,
          skipList: ConcurrentSkipListMap[Slice[Byte], _]): Unit

  def terminate(): Unit
}

private class KeyValueLimiterImpl(cacheSize: Long,
                                  delay: FiniteDuration)(implicit ex: ExecutionContext) extends LazyLogging with KeyValueLimiter {

  val nextGuessWeight = new AtomicInteger(10)

  private def keyValueWeigher(entry: Command): Long =
    entry match {
      case WeighAndAdd(keyValue, _) =>
        keyValue.get map {
          keyValue =>
            KeyValueLimiter.weight(keyValue)
        } getOrElse 264L //264L for the weight of WeakReference itself.

      case custom: AddWeighed =>
        custom.weight
    }

  /**
    * Lazy initialisation because this queue is not require for Memory database that do not use compression.
    */
  private lazy val queue = LimitQueue[Command](cacheSize, delay, keyValueWeigher) {
    command =>
      for {
        skipList <- command.skipListRef.get
        keyValue <- command.keyValueRef.get
      } yield {
        keyValue match {
          case persistentGroup: Persistent.Group =>

            /**
              * Before removing Persistent.Group, check if uncompressing it is enough,
              * if it's already uncompressed only then remove or else uncompress and re-add to the queue.
              * Header is not checked here because it's always going to be uncompressed since it's always pre-read before the Group is added to he Queue in [[add]].
              */
            if (persistentGroup.isIndexDecompressed || persistentGroup.isValueDecompressed) {
              val uncompressedGroup = persistentGroup.uncompress()
              skipList.asInstanceOf[ConcurrentSkipListMap[Slice[Byte], Persistent]].put(persistentGroup.key, uncompressedGroup)
              add(uncompressedGroup, skipList)
            } else {
              skipList.remove(keyValue.key)
            }

          case _: Persistent.SegmentResponse =>
            skipList.remove(keyValue.key)

          //Memory.Group key-values are only uncompressed. DO NOT REMOVE THEM!
          case memoryGroup: Memory.Group =>
            skipList.asInstanceOf[ConcurrentSkipListMap[Slice[Byte], Memory]].put(memoryGroup.key, memoryGroup.uncompress())
        }
      }
  }

  def terminate() =
    queue.terminate()

  def add(keyValue: Persistent.SegmentResponse,
          skipList: ConcurrentSkipListMap[Slice[Byte], _]): Unit =
    queue ! Command.WeighAndAdd(new WeakReference(keyValue), new WeakReference[ConcurrentSkipListMap[Slice[Byte], _]](skipList))

  /**
    * If there was failure reading the Group's header guess it's weight. Successful reads are priority over 100% cache's accuracy.
    * The cache's will eventually adjust to be accurate but until then guessed weights should be used. The accuracy of guessed
    * weights can also be used.
    */
  def add(keyValue: KeyValue.ReadOnly.Group,
          skipList: ConcurrentSkipListMap[Slice[Byte], _]): Unit =
    keyValue.header() map {
      header =>
        val weight = header.indexDecompressedLength + header.valueInfo.map(_.valuesDecompressedLength).getOrElse(0) + (264 * 2)
        nextGuessWeight lazySet ((weight + nextGuessWeight.get) / 2)
        queue ! Command.AddWeighed(new WeakReference(keyValue), new WeakReference[ConcurrentSkipListMap[Slice[Byte], _]](skipList), weight)
    } getOrElse {
      queue ! Command.AddWeighed(new WeakReference(keyValue), new WeakReference[ConcurrentSkipListMap[Slice[Byte], _]](skipList), nextGuessWeight.get)
    }
}

private object NoneKeyValueLimiter extends KeyValueLimiter {

  override def add(keyValue: KeyValue.ReadOnly.Group,
                   skipList: ConcurrentSkipListMap[Slice[Byte], _]): Unit =
    ()

  override def add(keyValue: Persistent.SegmentResponse,
                   skipList: ConcurrentSkipListMap[Slice[Byte], _]): Unit =
    ()

  override def terminate(): Unit = ()
}
