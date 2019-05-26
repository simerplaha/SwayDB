package swaydb.core.util

import swaydb.data.Reserve
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

/**
  * Reserves a range of keys for processing by a single thread.
  *
  * This is used to ensure that multiple threads do not concurrent perform compaction on overlapping keys within
  * the same Level.
  */
object ReserveRange {

  case class Range[T](from: Slice[Byte], to: Slice[Byte], reserve: Reserve[T])
  case class State[T](ranges: ListBuffer[Range[T]])

  def create[T](): State[T] =
    State(ListBuffer.empty)

  def get[T](from: Slice[Byte], to: Slice[Byte])(implicit state: State[T],
                                                 ordering: Ordering[Slice[Byte]]): Option[T] = {
    import ordering._
    state.synchronized {
      state
        .ranges
        .find(range => from.equiv(range.from) && to.equiv(range.to))
        .flatMap(_.reserve.info)
    }
  }


  def reserveOrGet[T](from: Slice[Byte], to: Slice[Byte], info: T)(implicit state: State[T],
                                                                   ordering: Ordering[Slice[Byte]]): Option[T] =
    state.synchronized {
      reserveOrGetRange(
        from = from,
        to = to,
        info = info
      ).flatMap(_.reserve.info)
    }

  def reserveOrListen[T](from: Slice[Byte], to: Slice[Byte], info: T)(implicit state: State[T],
                                                                      ordering: Ordering[Slice[Byte]]): Option[Future[Unit]] =
    state.synchronized {
      reserveOrGetRange(
        from = from,
        to = to,
        info = info
      ) map {
        range =>
          val promise = Promise[Unit]()
          range.reserve.savePromise(promise)
          promise.future
      }
    }

  def free[T](from: Slice[Byte], to: Slice[Byte])(implicit state: State[T],
                                                  ordering: Ordering[Slice[Byte]]): Unit =
    state.synchronized {
      import ordering._
      state
        .ranges
        .find(range => from.equiv(range.from) && to.equiv(range.to))
        .foreach {
          range =>
            state.ranges -= range
            Reserve.setFree(range.reserve)
        }
    }


  private def reserveOrGetRange[T](from: Slice[Byte], to: Slice[Byte], info: T)(implicit state: State[T],
                                                                                ordering: Ordering[Slice[Byte]]): Option[Range[T]] =
    state.synchronized {
      state
        .ranges
        .find(range => Slice.intersects((from, to), (range.from, range.to)))
        .orElse {
          state.ranges += ReserveRange.Range(from, to, Reserve(info))
          None
        }
    }
}
