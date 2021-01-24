/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.DefActor
import swaydb.core.level.compaction.Compactor
import swaydb.core.level.compaction.throttle.behaviour._

import scala.concurrent.Future

/**
 * The compaction Actor state (subtype of [[Compactor]]) which gets
 * initialised under an [[DefActor]] via [[ThrottleCompactorCreator]].
 *
 * Implements all the compaction APIs and mutation is only managed here.
 */

object ThrottleCompactor {

  def apply(state: ThrottleCompactorContext)(self: DefActor[ThrottleCompactor, Unit]) =
    new ThrottleCompactor(state, Future.unit)(self)
}

private[core] class ThrottleCompactor private(@volatile private var context: ThrottleCompactorContext,
                                              @volatile private var currentFuture: Future[Unit])(implicit self: DefActor[ThrottleCompactor, Unit]) extends Compactor with LazyLogging {

  implicit val ec = self.ec

  @inline private def onComplete(f: => Future[ThrottleCompactorContext]): Unit =
    currentFuture onComplete {
      _ =>
        this.currentFuture =
          f map {
            newContext =>
              this.context = newContext
          }
    }

  override def wakeUp(): Unit =
    onComplete(BehaviorWakeUp.wakeUp(context))

  def terminateASAP(): Unit =
    context.setTerminateASAP()
}
