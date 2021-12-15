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

package swaydb.slice

import com.typesafe.scalalogging.LazyLogging

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Provides BuildFrom implementation for Scala 2.12
 */
trait SliceBuildFrom extends LazyLogging {

  implicit def buildFrom[T: ClassTag]: CanBuildFrom[Slice[_], T, Slice[T]] =
    new CanBuildFrom[Slice[_], T, Slice[T]] {
      def apply(from: Slice[_]) =
        new SliceBuilder[T](from.size)

      def apply(): mutable.Builder[T, Slice[T]] = {
        //Use an Array or another data-type instead of Slice if dynamic extensions are required.
        //Dynamic extension is disabled so that we do not do unnecessary copying just for the sake of convenience.
        //Slice is used heavily internally and we should avoid all operations that might be expensive.
        val exception = new Exception("Cannot create slice with no size defined. If dynamic extension is required consider using another data-type.")
        logger.error(exception.getMessage, exception)
        throw exception
      }
    }
}
