package swaydb.core.actor

import java.util.TimerTask
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

object WiredActor {
  def apply[T, S](impl: T, state: S)(implicit ec: ExecutionContext): WiredActor[T, S] =
    new WiredActor(impl, None, state)

  def apply[T, S](impl: T, delays: FiniteDuration, state: S)(implicit ec: ExecutionContext): WiredActor[T, S] =
    new WiredActor(impl, Some(delays), state)
}

class WiredActor[+T, +S](impl: T, delays: Option[FiniteDuration], state: S)(implicit val ec: ExecutionContext) {

  private val actor: Actor[() => Unit, S] =
    delays map {
      delays =>
        Actor.timer[() => Unit, S](state, delays)((function, _) => function()).asInstanceOf[Actor[() => Unit, S]]
    } getOrElse {
      Actor[() => Unit, S](state)((function, _) => function()).asInstanceOf[Actor[() => Unit, S]]
    }

  def unsafeGetState: S =
    actor.state

  def unsafeGetImpl: T =
    impl

  def ask[R](function: (T, S) => R): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryComplete(Try(function(impl, unsafeGetState))))
    promise.future
  }

  def askFlatMap[R](function: (T, S) => Future[R]): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryCompleteWith(function(impl, unsafeGetState)))
    promise.future
  }

  def ask[R](function: (T, S, WiredActor[T, S]) => R): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryComplete(Try(function(impl, unsafeGetState, this))))
    promise.future
  }

  def askFlatMap[R](function: (T, S, WiredActor[T, S]) => Future[R]): Future[R] = {
    val promise = Promise[R]()
    actor ! (() => promise.tryCompleteWith(function(impl, unsafeGetState, this)))
    promise.future
  }

  def send[R](function: (T, S) => R): Unit =
    actor ! (() => function(impl, unsafeGetState))

  def send[R](function: (T, S, WiredActor[T, S]) => R): Unit =
    actor ! (() => function(impl, unsafeGetState, this))

  def scheduleAsk[R](delay: FiniteDuration)(function: (T, S) => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.tryComplete(Try(function(impl, unsafeGetState))), delay)
    (promise.future, timerTask)
  }

  def scheduleAskFlatMap[R](delay: FiniteDuration)(function: (T, S) => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.completeWith(function(impl, unsafeGetState)), delay)
    (promise.future, timerTask)
  }

  def scheduleAskWithSelf[R](delay: FiniteDuration)(function: (T, S, WiredActor[T, S]) => R): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.tryComplete(Try(function(impl, unsafeGetState, this))), delay)
    (promise.future, timerTask)
  }

  def scheduleAskWithSelfFlatMap[R](delay: FiniteDuration)(function: (T, S, WiredActor[T, S]) => Future[R]): (Future[R], TimerTask) = {
    val promise = Promise[R]()
    val timerTask = actor.schedule(() => promise.completeWith(function(impl, unsafeGetState, this)), delay)
    (promise.future, timerTask)
  }

  def scheduleSend[R](delay: FiniteDuration)(function: (T, S) => R): TimerTask =
    actor.schedule(() => function(impl, unsafeGetState), delay)

  def terminate(): Unit =
    actor.terminate()
}