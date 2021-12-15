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

private[swaydb] object PipeOps {

  class Pipe[A](a: A) {
    @inline final def ==>[B](f: A => B) = f(a)
  }

  object Pipe {
    @inline final def apply[A](v: A) = new Pipe(v)
  }

  @inline final implicit def pipe[A](a: A): Pipe[A] = Pipe(a)
}

