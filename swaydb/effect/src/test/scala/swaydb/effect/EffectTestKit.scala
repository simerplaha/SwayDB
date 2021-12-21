package swaydb.effect

import EffectTestSweeper._
import swaydb.slice.Slice
import swaydb.testkit.TestKit._
import swaydb.utils.Extension

import java.nio.file.{Files, Path, Paths}
import scala.util.Random
import swaydb.utils.UtilsTestKit._

object EffectTestKit {

  def genThreadSafeIOStrategy(cacheOnAccess: Boolean = randomBoolean(),
                              includeReserved: Boolean = true): IOStrategy.ThreadSafe =
    if (cacheOnAccess && includeReserved && randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = true) //this not being stored will result in too many retries.
    else
      IOStrategy.SynchronisedIO(cacheOnAccess)

  def genIOStrategyWithCacheOnAccess(cacheOnAccess: Boolean): IOStrategy =
    if (randomBoolean())
      IOStrategy.SynchronisedIO(cacheOnAccess)
    else if (randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = cacheOnAccess) //not used in stress tests.
    else
      IOStrategy.ConcurrentIO(cacheOnAccess)

  def genIOAccess(cacheOnAccess: => Boolean = randomBoolean()) =
    Random.shuffle(
      Seq(
        IOStrategy.ConcurrentIO(cacheOnAccess),
        IOStrategy.SynchronisedIO(cacheOnAccess),
        IOStrategy.AsyncIO(cacheOnAccess = true)
      )
    ).head

  def genIOStrategy(cacheOnAccess: Boolean = randomBoolean(),
                    includeReserved: Boolean = true): IOStrategy =
    if (randomBoolean())
      IOStrategy.SynchronisedIO(cacheOnAccess)
    else if (cacheOnAccess && includeReserved && randomBoolean())
      IOStrategy.AsyncIO(cacheOnAccess = true) //this not being stored will result in too many retries.
    else
      IOStrategy.ConcurrentIO(cacheOnAccess)

  def genFilePath(extension: Extension = Extension.gen())(implicit sweeper: EffectTestSweeper): Path =
    sweeper.testDirectory.resolve(s"${sweeper.idGenerator.nextId()}.${extension.toString}").sweep()

  def genDirWithFilePath(extension: Extension = Extension.gen())(implicit sweeper: EffectTestSweeper): Path = {
    val filePath = genFilePath(extension)
    Effect.createDirectoriesIfAbsent(filePath.getParent)
    filePath
  }

  def genFile()(implicit sweeper: EffectTestSweeper): Path = {
    val filePath = genFilePath()
    Effect.createDirectoryIfAbsent(filePath.getParent)
    Effect.createFile(filePath)
  }

  def genFile(bytes: Slice[Byte], extension: Extension)(implicit sweeper: EffectTestSweeper): Path =
    Effect.write(
      to = Effect.createDirectoryIfAbsent(sweeper.testDirectory).resolve(sweeper.idGenerator.nextId() + s"${extension.toStringWithDot}"),
      bytes = bytes.toByteBufferWrap()
    ).sweep()

  def genIntPath()(implicit sweeper: EffectTestSweeper): Path =
    sweeper.testDirectory.resolve(sweeper.idGenerator.toString)

  def genIntDir()(implicit sweeper: EffectTestSweeper): Path =
    Effect.createDirectoriesIfAbsent(genIntPath()).sweep()

  def genDirPath()(implicit sweeper: EffectTestSweeper): Path =
    sweeper.testDirectory.resolve(sweeper.idGenerator.nextId().toString).sweep()

  def genDir()(implicit sweeper: EffectTestSweeper): Path =
    Effect.createDirectoriesIfAbsent(genDirPath()).sweep()

}
