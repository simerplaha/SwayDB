/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.gc

import com.sun.management.GarbageCollectionNotificationInfo
import swaydb.utils.StorageUnits._

import javax.management.{Notification, NotificationEmitter, NotificationListener}
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._

/**
 * These functions are not used right but can be used
 * for calculating optimal file sizes.
 */
object GCStats {

  private def free() =
    Runtime.getRuntime.freeMemory()

  private def max() =
    Runtime.getRuntime.maxMemory()

  private def total() =
    Runtime.getRuntime.totalMemory()

  private def allocated(): Long =
    total() - free()

  def available(): Double =
    max() - allocated()

  def availableMB(): Double =
    available() / 1000000D

  def maxMB(): Double =
    max() / 1000000D

  def freeMB(): Double =
    free() / 1000000D

  //Will complete the future when at least requiredSpace heap space is available
  def whenAvailable(requiredSpace: Long): Future[Unit] =
    if (available() >= requiredSpace) {
      Future.unit
    } else {
      val promise = Promise[Unit]()
      for (beans <- java.lang.management.ManagementFactory.getGarbageCollectorMXBeans.asScala) {
        val listener: NotificationListener =
          (notification: Notification, _) =>
            if (notification.getType.equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION))
              if (available() >= requiredSpace)
                promise.completeWith(Future.unit)

        val emitter = beans.asInstanceOf[NotificationEmitter]
        emitter.addNotificationListener(listener, null, null)
      }

      promise.future
    }
}
