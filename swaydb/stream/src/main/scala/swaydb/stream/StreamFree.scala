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

package swaydb.stream

import swaydb.utils.OptionMutable
import swaydb.{Bag, Streamer}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[swaydb] object StreamFree {

  val takeOne = Some(1)

  def tabulate[A](n: Int)(f: Int => A): StreamFree[A] =
    apply[A](
      new Iterator[A] {
        var used = 0

        override def hasNext: Boolean =
          used < n

        override def next(): A = {
          val nextA = f(used)
          used += 1
          nextA
        }
      }
    )

  def apply[A](it: Iterator[A]): StreamFree[A] =
    new StreamFree[A] {
      @inline final private def stepBagLess[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        if (it.hasNext)
          bag.success(it.next())
        else
          bag.success(null.asInstanceOf[A])

      /**
       * Iterators created manually will require saving from Exceptions within the StreamBag.
       * This is not included in the StreamBag implementation itself because SwayDB handles
       * Exceptions in LevelZero and try catch is expensive.
       */
      @inline final private def stepSafe[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        bag.flatMap(bag(it.hasNext)) {
          hasNext =>
            if (hasNext)
              bag(it.next())
            else
              bag.success(null.asInstanceOf[A])
        }

      @inline private final def step[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        if (bag == Bag.glass)
          stepBagLess(bag)
        else
          stepSafe(bag)

      override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        step(bag)

      override private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]) =
        step(bag)
    }

  def join[A, B >: A](first: A, tail: StreamFree[B]): StreamFree[B] =
    new StreamFree[B]() {
      var processHead = false

      override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[B] =
        bag.success(first)

      override private[swaydb] def nextOrNull[BAG[_]](previous: B)(implicit bag: Bag[BAG]) =
        if (!processHead)
          bag.transform(tail.headOrNull) {
            head =>
              processHead = true
              head
          }
        else
          tail.nextOrNull(previous)
    }
}

/**
 * A [[StreamFree]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 */
private[swaydb] trait StreamFree[A] { self =>

  private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A]
  private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]): BAG[A]

  final def head[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] =
    bag.map(headOrNull)(Option(_))

  def map[B](f: A => B): StreamFree[B] =
    new step.Map(
      previousStream = self,
      f = f
    )

  def mapBags[B, BAG[_]](f: A => BAG[B]): StreamFree[B] =
    new step.MapBags(
      previousStream = self,
      f = f
    )

  def flatMap[B](f: A => StreamFree[B]): StreamFree[B] =
    new step.FlatMap(
      previousStream = self,
      f = f
    )

  def drop(count: Int): StreamFree[A] =
    if (count <= 0)
      this
    else
      new step.Drop[A](
        previousStream = self,
        drop = count
      )

  def dropWhile(f: A => Boolean): StreamFree[A] =
    new step.DropWhile[A](
      previousStream = self,
      condition = f
    )

  def take(count: Int): StreamFree[A] =
    new step.Take(
      previousStream = self,
      take = count
    )

  def takeWhile(f: A => Boolean): StreamFree[A] =
    new step.TakeWhile[A](
      previousStream = self,
      condition = f
    )

  def filter(f: A => Boolean): StreamFree[A] =
    new step.Filter[A](
      previousStream = self,
      condition = f
    )

  def filterNot(f: A => Boolean): StreamFree[A] =
    filter(!f(_))

  def collect[B](pf: PartialFunction[A, B]): StreamFree[B] =
    new step.Collect[A, B](
      previousStream = self,
      pf = pf
    )

  def flatten[BAG[_], B](implicit bag: Bag[BAG],
                         evd: A <:< BAG[B]): StreamFree[B] =
    new step.Flatten[A, B](previousStream = self)

  def collectFirst[B, BAG[_]](pf: PartialFunction[A, B])(implicit bag: Bag[BAG]): BAG[Option[B]] =
    bag.map(collectFirstOrNull(pf))(Option(_))

  def collectFirstOrNull[B, BAG[_]](pf: PartialFunction[A, B])(implicit bag: Bag[BAG]): BAG[B] =
    collect(pf).headOrNull

  def count[BAG[_]](f: A => Boolean)(implicit bag: Bag[BAG]): BAG[Int] =
    foldLeft(0) {
      case (c, item) if f(item) => c + 1
      case (c, _)               => c
    }

  /**
   * Reads all items from the StreamBag and returns the last.
   *
   * For a more efficient one use swaydb.Map.lastOption or swaydb.Set.lastOption instead.
   */
  def last[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] = {
    val last =
      foldLeft(OptionMutable.Null: OptionMutable[A]) {
        (previous, next) =>
          if (previous.isNoneC) {
            OptionMutable.Some(next)
          } else {
            previous.getC setValue next
            previous
          }
      }

    bag.transform(last)(_.toOption)
  }

  /**
   * Materializes are executes the stream.
   */
  def foldLeft[B, BAG[_]](initial: B)(f: (B, A) => B)(implicit bag: Bag[BAG]): BAG[B] =
    bag.safe { //safe execution of the stream to recover errors.
      step.Step.foldLeft(
        initial = initial,
        afterOrNull = null.asInstanceOf[A],
        stream = self,
        drop = 0,
        take = None
      ) {
        (b, a) =>
          bag.success(f(b, a))
      }
    }

  def foldLeftBags[B, BAG[_]](initial: B)(f: (B, A) => BAG[B])(implicit bag: Bag[BAG]): BAG[B] =
    bag.safe { //safe execution of the stream to recover errors.
      step.Step.foldLeft(
        initial = initial,
        afterOrNull = null.asInstanceOf[A],
        stream = self,
        drop = 0,
        take = None
      )(f)
    }

  def foreach[BAG[_]](f: A => Unit)(implicit bag: Bag[BAG]): BAG[Unit] =
    foldLeft(()) {
      case (_, item) =>
        f(item)
    }

  def partitionBuffer[BAG[_]](f: A => Boolean)(implicit bag: Bag[BAG]): BAG[(ListBuffer[A], ListBuffer[A])] =
    foldLeft((ListBuffer.empty[A], ListBuffer.empty[A])) {
      case (buckets @ (left, right), elem) =>
        if (f(elem))
          left += elem
        else
          right += elem

        buckets
    }

  /**
   * Folds over all elements in the StreamBag to calculate it's total size.
   */
  def count[BAG[_]](implicit bag: Bag[BAG]): BAG[Int] =
    foldLeft(0) {
      case (size, _) =>
        size + 1
    }

  private def materializeBuilder[BAG[_], X[_]](implicit bag: Bag[BAG],
                                               builder: mutable.Builder[A, X[A]]): BAG[mutable.Builder[A, X[A]]] =
    foldLeft(builder) {
      case (builder, item) =>
        builder += item
        builder
    }

  /**
   * Materialises/closes and processes the stream to a [[Seq]].
   */
  def materializeFromBuilder[BAG[_], X[_]](implicit bag: Bag[BAG],
                                           builder: mutable.Builder[A, X[A]]): BAG[X[A]] =
    bag.transform(materializeBuilder)(_.result())

  /**
   * Executes this StreamBag within the provided [[Bag]].
   */

  def materialize[BAG[_]](implicit bag: Bag[BAG]): BAG[Iterable[A]] = {
    implicit val listBuffer: mutable.Builder[A, ListBuffer[A]] = ListBuffer.newBuilder[A]
    bag.transform(materializeBuilder)(_.result())
  }

  def materializeBuffer[BAG[_]](implicit bag: Bag[BAG]): BAG[ListBuffer[A]] = {
    implicit val listBuffer: mutable.Builder[A, ListBuffer[A]] = ListBuffer.newBuilder[A]
    bag.transform(materializeBuilder)(_.result())
  }

  /**
   * A [[Streamer]] is a simple interface to a [[Stream]] instance which
   * only one has function [[Streamer.nextOrNull]] that can be used to
   * create other interop implementations with other Streaming libraries.
   */
  def streamer[BAG[_]](implicit bag: Bag[BAG]): Streamer[A, BAG] =
    new Streamer[A, BAG] {
      var previous: A = _

      override def nextOrNull: BAG[A] = {
        val next =
          if (previous == null)
            self.headOrNull
          else
            self.nextOrNull(previous)

        bag.transform(next) {
          next =>
            previous = next
            next
        }
      }

      override def nextOption: BAG[Option[A]] =
        bag.transform(nextOrNull)(Option(_))
    }

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[A]] =
    new Iterator[BAG[A]] {
      val stream = streamer
      var nextBag: BAG[A] = _
      var failedStream: Boolean = false

      override def hasNext: Boolean =
        if (failedStream) {
          false
        } else {
          nextBag = stream.nextOrNull
          if (bag.isSuccess(nextBag)) {
            bag.getUnsafe(nextBag) != null
          } else {
            failedStream = true
            true
          }
        }

      override def next(): BAG[A] =
        nextBag
    }
}
