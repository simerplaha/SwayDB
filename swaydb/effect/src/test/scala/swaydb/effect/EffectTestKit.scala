package swaydb.effect

import swaydb.testkit.TestKit._

import scala.util.Random

object EffectTestKit {

  def randomThreadSafeIOStrategy(cacheOnAccess: Boolean = randomBoolean(),
                                 includeReserved: Boolean = true): IOStrategy.ThreadSafe =
    if (cacheOnAccess && includeReserved && randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = true) //this not being stored will result in too many retries.
    else
      IOStrategy.SynchronisedIO(cacheOnAccess)

  def randomIOStrategyWithCacheOnAccess(cacheOnAccess: Boolean): IOStrategy =
    if (randomBoolean())
      IOStrategy.SynchronisedIO(cacheOnAccess)
    else if (randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = cacheOnAccess) //not used in stress tests.
    else
      IOStrategy.ConcurrentIO(cacheOnAccess)

  def randomIOAccess(cacheOnAccess: => Boolean = randomBoolean()) =
    Random.shuffle(
      Seq(
        IOStrategy.ConcurrentIO(cacheOnAccess),
        IOStrategy.SynchronisedIO(cacheOnAccess),
        IOStrategy.AsyncIO(cacheOnAccess = true)
      )
    ).head

}
