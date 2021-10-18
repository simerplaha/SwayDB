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

package swaydb.utils

import com.typesafe.scalalogging.LazyLogging

private[swaydb] object FunctionSafe extends LazyLogging {

  def safe[T](default: => T, function: => T): T =
    try
      function
    catch {
      case exception: Throwable =>
        logger.error("Make sure your functions do not throw exceptions. Using default value.", exception)
        default
    }

  def safeBoolean[T](function: => Boolean): Boolean =
    try
      function
    catch {
      case exception: Throwable =>
        logger.error("Make sure your functions do not throw exceptions. Using default value 'false'.", exception)
        false
    }
}
