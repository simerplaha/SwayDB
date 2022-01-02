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

package swaydb.testkit

import swaydb.testkit.RunThis.runThis

import scala.collection.parallel.CollectionConverters.IterableIsParallelizable

/**
 * Base implementations on how [[TestSweeper]]s can be initialised.
 *
 * @tparam SWEEPER the target test sweeper.
 */
trait TestSweeperCompanion[SWEEPER <: TestSweeper] {

  /**
   * A new test
   *
   * @return a new instances of the current [[SWEEPER]]
   */
  def newTestScope(): SWEEPER

  final def repeat(times: Int, log: Boolean = true)(code: SWEEPER => Unit): Unit = {
    import swaydb.testkit.RunThis._
    runThis(times, log)(apply[Unit](code))
  }

  final def foreachRepeat[S](times: Int, states: Iterable[S])(code: (SWEEPER, S) => Unit): Unit =
    runThis(times, log = true) {
      foreach(states)(code)
    }

  final def foreach[S](states: Iterable[S])(code: (SWEEPER, S) => Unit): Unit =
    states foreach {
      state =>
        apply {
          sweeper =>
            code(sweeper, state)
        }
    }

  final def foreachParallel[S](states: Iterable[S])(code: (SWEEPER, S) => Unit): Unit =
    states.par foreach {
      state =>
        apply {
          sweeper =>
            code(sweeper, state)
        }
    }

  final def apply[T](code: SWEEPER => T): T = {
    val sweeper = newTestScope()
    val result = code(sweeper)
    sweeper.terminate()
    result
  }

}
