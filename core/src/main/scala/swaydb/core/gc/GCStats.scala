/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
