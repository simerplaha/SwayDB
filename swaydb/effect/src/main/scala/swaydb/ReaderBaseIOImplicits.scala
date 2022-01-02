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

package swaydb

import swaydb.slice.ReaderBase

import scala.annotation.tailrec

object ReaderBaseIOImplicits {

  implicit class ReaderIOImplicits[B <: ReaderBase](reader: B) {

    @tailrec
    @inline final def foldLeftIO[E: IO.ExceptionHandler, R](result: R)(f: (R, B) => IO[E, R]): IO[E, R] =
      IO(reader.hasMore) match {
        case IO.Left(error) =>
          IO.Left(error)

        case IO.Right(yes) if yes =>
          f(result, reader) match {
            case IO.Right(newResult) =>
              foldLeftIO(newResult)(f)

            case IO.Left(error) =>
              IO.Left(error)
          }

        case _ =>
          IO.Right(result)
      }
  }
}
