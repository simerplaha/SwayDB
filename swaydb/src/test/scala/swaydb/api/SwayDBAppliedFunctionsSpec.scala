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

package swaydb.api

import swaydb.PureFunctionScala._
import swaydb.core.TestCaseSweeper
import swaydb.core.TestCaseSweeper._
import swaydb.core.log.applied.AppliedFunctionsLog
import swaydb.Benchmark
import swaydb.config.Functions
import swaydb.serializers.Default._
import swaydb.testkit.RunThis.{eventual, runThis}
import swaydb.{Glass, _}

import java.nio.file.Files
import scala.concurrent.duration.DurationInt
import scala.util.Try

class SwayDBAppliedFunctionsSpec extends TestBaseEmbedded {

  override val keyValueCount: Int = 1000

  "it" should {

    "not create appliedFunctions map" when {
      "functions are disabled" in {
        runThis(times = repeatTest, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              val dir = createRandomDir
              val map = swaydb.persistent.Map[Int, String, Nothing, Glass](dir).sweep(_.delete())

              Files.exists(dir) shouldBe true
              Files.exists(dir.resolve("applied-functions")) shouldBe false

              map.clearAppliedFunctions().size shouldBe 0
          }
        }
      }
    }

    "create appliedFunctions map" when {
      "functions are enabled" in {
        runThis(times = repeatTest, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              val dir = createRandomDir

              val function: OnKey[Int, String] =
                _ =>
                  fail("There is no data for this function to execute")

              implicit val functions = Functions[PureFunction.Map[Int, String]](function)
              val map = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Glass](dir).sweep(_.delete())

              Files.exists(dir) shouldBe true
              Files.exists(dir.resolve(AppliedFunctionsLog.folderName).resolve("0.log")) shouldBe true

              map.clearAppliedFunctions().size shouldBe 0
          }
        }
      }
    }
  }

  "store applied functions" when {
    "there is no data for the function to apply on" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val dir = createRandomDir

            val function1: OnKey[Int, String] =
              (_: Int) =>
                fail("There is no data for this function to execute")

            val function2: OnKey[Int, String] =
              (_: Int) =>
                fail("There is no data for this function to execute")

            implicit val functions = Functions[PureFunction.Map[Int, String]](function1, function2)
            val map = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Glass](dir).sweep(_.delete())

            map.applyFunction(1, 100, function1)

            def assertAppliedFunctions(map: Map[Int, String, PureFunction.Map[Int, String], Glass]) = {
              //function1
              map.isFunctionApplied(function1) shouldBe true
              //function2 not applied
              map.isFunctionApplied(function2) shouldBe false
              //we don't know because compaction has not finished yet.
              map.clearAppliedFunctions().size shouldBe 0
            }

            assertAppliedFunctions(map)

            map.close()
        }
      }
    }

    "there is data for the function to apply" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val dir = createRandomDir

            val noDataToApply: OnKey[Int, String] =
              (_: Int) =>
                fail("There is no data for this function to execute")

            val hasData: OnKey[Int, String] =
              (_: Int) =>
                Apply.Nothing

            implicit val functions = Functions[PureFunction.Map[Int, String]](noDataToApply, hasData)
            val map = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Glass](dir)

            (101 to 200) foreach {
              i =>
                map.put(i, i.toString)
            }

            map.applyFunction(1, 100, noDataToApply)
            map.applyFunction(101, 200, hasData)

            map.mightContainFunction(noDataToApply) shouldBe true
            map.mightContainFunction(hasData) shouldBe true
            //function1
            map.isFunctionApplied(noDataToApply) shouldBe true
            //function2 not applied
            map.isFunctionApplied(hasData) shouldBe true
            //clear yields no
            map.clearAppliedFunctions().size shouldBe 0

            map.close()

            //reopen so that flush occurs and compaction gets triggered.
            val reopened = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Glass](dir).sweep(_.delete())

            eventual(10.seconds) {
              reopened.mightContainFunction(noDataToApply) shouldBe false
              reopened.mightContainFunction(hasData) shouldBe false
            }

            reopened.clearAppliedFunctions().size shouldBe 2
            //function1
            reopened.isFunctionApplied(noDataToApply) shouldBe false
            //function2 not applied
            reopened.isFunctionApplied(hasData) shouldBe false
        }
      }
    }
  }

  "fail start" when {
    "applied function is missing" in {
      runThis(times = repeatTest, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val dir = createRandomDir

            val noDataToApply: OnKey[Int, String] =
              (key: Int) => fail("There is no data for this function to execute")

            val hasData: OnKey[Int, String] =
              (key: Int) =>
                Apply.Nothing

            {
              implicit val functions = Functions[PureFunction.Map[Int, String]](noDataToApply, hasData)
              val map = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Glass](dir)

              map.applyFunction(1, 100, noDataToApply)
              map.applyFunction(101, 200, hasData)

              map.close()
            }

            /**
             * Reopen with missing funtions
             */
            {
              implicit val functions = Functions[PureFunction.Map[Int, String]](noDataToApply)
              //reopen so that flush occurs and compaction gets triggered.
              val exception = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Try](dir).failed.get
              val missingFunctions = exception.asInstanceOf[Exception.MissingFunctions]
              missingFunctions.functions shouldBe Seq(hasData.id)
            }
        }
      }
    }
  }

  "store large number of functions" in {
    runThis(times = repeatTest, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          val dir = createRandomDir

          val functions =
            (1 to 1000) map {
              i =>
                new OnKey[Int, String] {
                  override def apply(key: Int): Apply.Map[String] =
                    fail("There is no data for this function to execute")

                  override def id: String =
                    "Function" + i.toString
                }
            }

          implicit val f = Functions[PureFunction.Map[Int, String]](functions)

          {
            val map = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Glass](dir)

            Benchmark(s"applying ${functions.size} functions") {
              functions.zipWithIndex foreach {
                case (function, index) =>
                  map.applyFunction(index, function)
              }
            }

            map.close()
          }

          /**
           * Reopen with missing functions
           */
          {
            //reopen so that flush occurs and compaction gets triggered.
            val map = swaydb.persistent.Map[Int, String, PureFunction.Map[Int, String], Glass](dir).sweep(_.delete())
            eventual(10.seconds) {
              functions.forall(function => !map.mightContainFunction(function))
            }

            eventual(10.seconds) {
              map.clearAppliedAndRegisteredFunctions() should have size 0
            }
          }
      }
    }
  }
}
