package swaydb.core.actor

import java.util.TimerTask
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class WiredActor[+T](impl: T, delays: Option[FiniteDuration])(implicit ec: ExecutionContext) {

  private val actor =
    delays map {
      delays =>
        Actor.timer[() => Unit](delays)((function, _) => function())
    } getOrElse {
      Actor[() => Unit]((function, _) => function())
    }

  def ask[R](function: T => R): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryComplete(Try(function(impl))))
    promise.future
  }

  def askFlatMap[R](function: T => Future[R]): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryCompleteWith(function(impl)))
    promise.future
  }

  def ask[R](function: (T, WiredActor[T]) => R): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryComplete(Try(function(impl, this))))
    promise.future
  }

  def askFlatMap[R](function: (T, WiredActor[T]) => Future[R]): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryCompleteWith(function(impl, this)))
    promise.future
  }

  def send[R](function: T => R): Unit =
    actor ! (() => function(impl))

  def send[R](function: (T, WiredActor[T]) => R): Unit =
    actor ! (() => function(impl, this))

  def scheduleAsk[R](delay: FiniteDuration)(function: T => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.tryComplete(Try(function(impl))), delay)
    (promise.future, timerTask)
  }

  def scheduleAskFlatMap[R](delay: FiniteDuration)(function: T => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.completeWith(function(impl)), delay)
    (promise.future, timerTask)
  }

  def scheduleAskWithSelf[R](delay: FiniteDuration)(function: (T, WiredActor[T]) => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.tryComplete(Try(function(impl, this))), delay)
    (promise.future, timerTask)
  }

  def scheduleAskWithSelfFlatMap[R](delay: FiniteDuration)(function: (T, WiredActor[T]) => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.completeWith(function(impl, this)), delay)
    (promise.future, timerTask)
  }

  def scheduleSend[R](delay: FiniteDuration)(function: T => R): TimerTask =
    actor.schedule(() => function(impl), delay)

  def scheduleSend[R](delay: FiniteDuration)(function: (T, WiredActor[T]) => R): TimerTask =
    actor.schedule(() => function(impl, this), delay)
}