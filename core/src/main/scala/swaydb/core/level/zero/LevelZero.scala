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

package swaydb.core.level.zero

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file.{Path, Paths, StandardOpenOption}

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.KeyValue.{KeyValueInternal, KeyValueTuple, _}
import swaydb.core.data._
import swaydb.core.io.file.IO
import swaydb.core.level.LevelRef
import swaydb.core.level.actor.LevelCommand.WakeUp
import swaydb.core.level.actor.LevelZeroAPI
import swaydb.core.map.serializer.Level0KeyValuesSerializer
import swaydb.core.map.{MapEntry, Maps}
import swaydb.core.retry.Retry
import swaydb.core.util.MinMax
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.data.storage.Level0Storage
import swaydb.data.storage.Level0Storage.{Memory, Persistent}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[core] object LevelZero extends LazyLogging {

  def apply(mapSize: Long,
            storage: Level0Storage,
            nextLevel: LevelRef,
            acceleration: Level0Meter => Accelerator,
            readRetryLimit: Int)(implicit ordering: Ordering[Slice[Byte]],
                                 ec: ExecutionContext): Try[LevelZero] = {
    implicit val serializer: Level0KeyValuesSerializer = Level0KeyValuesSerializer(ordering)
    val mapsAndPathAndLock =
      storage match {
        case Persistent(mmap, databaseDirectory, recovery) =>
          val path = databaseDirectory.resolve(0.toString)
          IO createDirectoriesIfAbsent path
          logger.info("{}: Acquiring lock.", path)
          val lockFile = path.resolve("LOCK")
          IO createFileIfAbsent lockFile
          Try(FileChannel.open(lockFile, StandardOpenOption.WRITE).tryLock()) flatMap {
            lock =>
              logger.info("{}: Recovering Maps.", path)
              Maps.persistent[Slice[Byte], (ValueType, Option[Slice[Byte]])](path, mmap, mapSize, acceleration, recovery) map {
                maps =>
                  (maps, path, Some(lock))
              }
          }

        case Memory =>
          Success(Maps.memory[Slice[Byte], (ValueType, Option[Slice[Byte]])](mapSize, acceleration), Paths.get("MEMORY_DB").resolve(0.toString), None)
      }
    mapsAndPathAndLock map {
      case (maps, path, lock: Option[FileLock]) =>
        new LevelZero(path, mapSize, readRetryLimit, maps, nextLevel, lock)
    }
  }
}

private[core] class LevelZero(val path: Path,
                              mapSize: Long,
                              readRetryLimit: Int,
                              val maps: Maps[Slice[Byte], (ValueType, Option[Slice[Byte]])],
                              val nextLevel: LevelRef,
                              lock: Option[FileLock])(implicit ordering: Ordering[Slice[Byte]],
                                                      val serializer: Level0KeyValuesSerializer,
                                                      ec: ExecutionContext) extends LevelZeroRef with LazyLogging {

  logger.info("{}: Level0 started.", path)

  implicit val orderOnReadOnly = ordering.on[KeyValueType](_.key)
  implicit val orderKeyValueInternal = ordering.on[KeyValueInternal](_._1)

  private val actor =
    LevelZeroActor(this)

  maps setOnFullListener {
    () =>
      Future(actor ! WakeUp)
  }

  def releaseLocks: Try[Unit] =
    Try(lock.foreach(_.release())) flatMap {
      _ =>
        nextLevel.releaseLocks
    }

  def withRetry[T](tryBlock: => Try[T]): Try[T] =
    Retry[T](resourceId = path.toString, maxRetryLimit = readRetryLimit, until = Retry.levelReadRetryUntil) {
      try
        tryBlock
      catch {
        case ex: Exception =>
          Failure(ex)
      }
    }

  def !(command: LevelZeroAPI): Unit =
    actor ! command

  def assertKey(key: Slice[Byte])(block: => Try[Level0Meter]): Try[Level0Meter] =
    if (key.isEmpty)
      Failure(new IllegalArgumentException("Empty key."))
    else
      block

  def put(key: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.add(key, (ValueType.Add, None))
    }

  def put(key: Slice[Byte], value: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps.add(key, (ValueType.Add, Some(value)))
    }

  def put(key: Slice[Byte], value: Option[Slice[Byte]]): Try[Level0Meter] =
    assertKey(key) {
      maps.add(key, (ValueType.Add, value))
    }

  def put(entry: MapEntry[Slice[Byte], (ValueType, Option[Slice[Byte]])]): Try[Level0Meter] =
    maps add entry

  def remove(key: Slice[Byte]): Try[Level0Meter] =
    assertKey(key) {
      maps add(key, (ValueType.Remove, None))
    }

  def headFromMaps =
    maps.reduce[KeyValueInternal](_.first, MinMax.min(_, _))

  def lastFromMaps =
    maps.reduce[KeyValueInternal](_.last, MinMax.max(_, _))

  def higherFromMaps(key: Slice[Byte]): Option[KeyValueInternal] =
    maps.reduce[KeyValueInternal](_.higher(key), MinMax.min(_, _))

  def lowerFromMaps(key: Slice[Byte]): Option[KeyValueInternal] =
    maps.reduce[KeyValueInternal](_.lower(key), MinMax.max(_, _))

  def head: Try[Option[KeyValueTuple]] =
    withRetry {
      headKeyValue.flatMap(_.map(_.toTuple) getOrElse Success(None))
    }

  def headKey: Try[Option[Slice[Byte]]] =
    withRetry {
      headKeyValue.map(_.map(_.key))
    }

  private def headKeyValue: Try[Option[KeyValueType]] =
    withRetry {
      val fromMaps: Option[KeyValueInternal] = headFromMaps
      nextLevel.head flatMap {
        fromLevels =>
          MinMax.min(fromMaps.map(_.toKeyValueType), fromLevels) match {
            case Some(keyValue) =>
              if (keyValue.isDelete)
                higherKeyValue(keyValue.key)
              else
                Success(Some(keyValue))

            case None =>
              Success(None)
          }
      }
    }

  def last: Try[Option[KeyValueTuple]] =
    withRetry {
      lastKeyValue.flatMap(_.map(_.toTuple) getOrElse Success(None))
    }

  def lastKey: Try[Option[Slice[Byte]]] =
    withRetry {
      lastKeyValue.map(_.map(_.key))
    }

  private def lastKeyValue: Try[Option[KeyValueType]] =
    withRetry {
      val fromMaps: Option[KeyValueInternal] = lastFromMaps
      nextLevel.last flatMap {
        fromLevels =>
          MinMax.max(fromMaps.map(_.toKeyValueType), fromLevels) match {
            case Some(keyValue) =>
              if (keyValue.isDelete)
                lowerKeyValue(keyValue.key)
              else
                Success(Some(keyValue))
            case None =>
              Success(None)
          }
      }
    }

  def higher(key: Slice[Byte]): Try[Option[KeyValueTuple]] =
    withRetry {
      higherKeyValue(key).flatMap(_.map(_.toTuple) getOrElse Success(None))
    }

  def higherKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    withRetry {
      higherKeyValue(key).map(_.map(_.key))
    }

  @tailrec
  private def higherKeyValue(key: Slice[Byte]): Try[Option[KeyValueType]] = {
    val fromMaps: Option[KeyValueInternal] = higherFromMaps(key)
    nextLevel.higher(key) match {
      case Success(fromLevels) =>
        MinMax.min(fromMaps.map(_.toKeyValueType), fromLevels) match {
          case Some(keyValue) =>
            //            println(s"Higher: ${keyValue.key.read[Int]}")
            if (keyValue.isDelete)
              higherKeyValue(keyValue.key)
            else
              Success(Some(keyValue))

          case None =>
            Success(None)
        }

      case Failure(exception) =>
        Failure(exception)
    }
  }

  def lower(key: Slice[Byte]): Try[Option[KeyValueTuple]] =
    withRetry {
      lowerKeyValue(key).flatMap(_.map(_.toTuple) getOrElse Success(None))
    }

  def lowerKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    withRetry {
      lowerKeyValue(key).map(_.map(_.key))
    }

  @tailrec
  private def lowerKeyValue(key: Slice[Byte]): Try[Option[KeyValueType]] = {
    val fromMaps: Option[KeyValueInternal] = lowerFromMaps(key)
    nextLevel.lower(key) match {
      case Success(fromLevels) =>
        MinMax.max(fromMaps.map(_.toKeyValueType), fromLevels) match {
          case Some(keyValue) =>
            if (keyValue.isDelete)
              lowerKeyValue(keyValue.key)
            else
              Success(Some(keyValue))

          case None =>
            Success(None)
        }

      case Failure(exception) =>
        Failure(exception)
    }
  }

  def contains(key: Slice[Byte]): Try[Boolean] =
    withRetry {
      maps.get(key) match {
        case Some((_, (valueType, _))) =>
          Success(valueType.notDelete)

        case None =>
          nextLevel.get(key) flatMap {
            case Some(keyValue) =>
              Success(keyValue.notDelete)

            case _ =>
              Success(false)
          }
      }
    }

  def get(key: Slice[Byte]): Try[Option[Option[Slice[Byte]]]] =
    withRetry {
      maps.get(key) match {
        case Some((_, (valueType, value))) =>
          if (valueType.isDelete)
            Success(None)
          else
            Success(Some(value))

        case None =>
          nextLevel.get(key) flatMap {
            case Some(keyValue) =>
              if (keyValue.isDelete)
                Success(None)
              else
                keyValue.getOrFetchValue map (Some(_))
            case _ =>
              Success(None)
          }
      }
    }

  def getKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    withRetry {
      maps.get(key) match {
        case Some((key, (valueType, _))) =>
          if (valueType.isDelete)
            Success(None)
          else
            Success(Some(key))

        case None =>
          nextLevel.get(key) flatMap {
            case Some(keyValue) =>
              if (keyValue.isDelete)
                Success(None)
              else
                Success(Some(keyValue.key))
            case _ =>
              Success(None)
          }
      }
    }

  def getKeyValue(key: Slice[Byte]): Try[Option[KeyValueTuple]] =
    withRetry {
      maps.get(key) match {
        case Some((key, (valueType, value))) =>
          if (valueType.isDelete)
            Success(None)
          else
            Success(Some(key, value))

        case None =>
          nextLevel.get(key) flatMap {
            case Some(keyValue) =>
              if (keyValue.isDelete)
                Success(None)
              else
                keyValue.getOrFetchValue.map {
                  value =>
                    Some(keyValue.key, value)
                }
            case _ =>
              Success(None)
          }
      }
    }

  def valueSize(key: Slice[Byte]): Try[Option[Int]] =
    withRetry {
      maps.get(key) match {
        case Some((_, (valueType, value))) =>
          if (valueType.isDelete)
            Success(None)
          else
            Success(value.map(_.size))

        case None =>
          nextLevel.get(key).map(_.map(_.valueLength))
      }
    }

  def keyValueCount: Try[Int] =
    withRetry {
      val keyValueCountInMaps = maps.keyValueCount.getOrElse(0)
      nextLevel.keyValueCount.map(_ + keyValueCountInMaps)
    }

  override def sizeOfSegments: Long =
    nextLevel.sizeOfSegments

  override def beforeKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    lowerKey(key)

  override def before(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    lower(key)

  override def afterKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    higherKey(key)

  override def after(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    higher(key)

  def existsOnDisk: Boolean =
    IO.exists(path)

  def close: Try[Unit] = {
    maps.close.failed.foreach {
      exception =>
        logger.error(s"$path: Failed to close maps", exception)
    }
    nextLevel.close
  }

  override def level0Meter: Level0Meter =
    maps.getMeter

  override def level1Meter: LevelMeter =
    nextLevel.meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    nextLevel.meterFor(levelNumber)

  override def mightContain(key: Slice[Byte]): Try[Boolean] =
    withRetry {
      maps.get(key) match {
        case Some((_, (valueType, _))) =>
          Success(valueType.notDelete)

        case None =>
          nextLevel mightContain key
      }
    }
}