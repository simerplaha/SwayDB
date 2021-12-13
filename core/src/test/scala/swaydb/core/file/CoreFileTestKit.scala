package swaydb.core.file

import swaydb.core.{TestForceSave, TestSweeper, TestTuple2}
import swaydb.core.CommonAssertions.randomThreadSafeIOStrategy
import swaydb.core.TestSweeper._
import swaydb.effect.Effect
import swaydb.slice.Slice
import swaydb.testkit.TestKit.randomCharacters
import swaydb.utils.OperatingSystem

import java.nio.file.Path
import scala.util.Random

import org.scalatest.PrivateMethodTester._

object CoreFileTestKit {

  private[file] def invokePrivateFunction_file(coreFile: CoreFile): CoreFileType =
    coreFile invokePrivate PrivateMethod[CoreFileType](Symbol("file"))()

  def invokePrivateFunction_file(reader: FileReader): CoreFile =
    reader invokePrivate PrivateMethod[CoreFile](Symbol("file"))()

  def randomFilePath()(implicit sweeper: TestSweeper): Path =
    sweeper.testDir().resolve(s"${randomCharacters()}.test").sweep()

  def createFile(bytes: Slice[Byte])(implicit sweeper: TestSweeper): Path =
    Effect.write(
      to = sweeper.testDir().resolve(sweeper.idGenerator.nextSegment),
      bytes = bytes.toByteBufferWrap
    ).sweep()

  def createRandomFileReader(path: Path)(implicit sweeper: TestSweeper): FileReader =
    if (Random.nextBoolean())
      createMMAPFileReader(path)
    else
      createStandardFileFileReader(path)

  def createFileReaders(path: Path)(implicit sweeper: TestSweeper): TestTuple2[FileReader] =
    TestTuple2(
      left = createMMAPFileReader(path),
      right = createStandardFileFileReader(path)
    )

  def createMMAPFileReader(bytes: Slice[Byte])(implicit sweeper: TestSweeper): FileReader =
    createMMAPFileReader(createFile(bytes))

  /**
   * Creates all file types currently supported which are MMAP and StandardFile.
   */
  def createFiles(mmapPath: Path,
                  mmapBytes: Slice[Byte],
                  channelPath: Path,
                  standardBytes: Slice[Byte])(implicit sweeper: TestSweeper): TestTuple2[CoreFile] =
    TestTuple2(
      left = createMMAPWriteAndRead(mmapPath, mmapBytes),
      right = createStandardWriteAndRead(channelPath, standardBytes)
    )

  def createFiles(mmapBytes: Slice[Byte], standardBytes: Slice[Byte])(implicit sweeper: TestSweeper): TestTuple2[CoreFile] =
    TestTuple2(
      left = createMMAPWriteAndRead(randomFilePath(), mmapBytes),
      right = createStandardWriteAndRead(randomFilePath(), standardBytes)
    )

  def createMMAPWriteAndRead(path: Path, bytes: Slice[Byte])(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    CoreFile.mmapWriteAndRead(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      deleteAfterClean = OperatingSystem.isWindows,
      forceSave = TestForceSave.mmap(),
      bytes = bytes
    ).sweep()
  }

  def createWriteableMMAPFile(path: Path, bufferSize: Int)(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    CoreFile.mmapInit(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      deleteAfterClean = OperatingSystem.isWindows,
      forceSave = TestForceSave.mmap(),
      bufferSize = bufferSize
    ).sweep()
  }

  def createWriteableStandardFile(path: Path)(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    CoreFile.standardWrite(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      forceSave = TestForceSave.standard()
    )
  }

  def createStandardWriteAndRead(path: Path, bytes: Slice[Byte])(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    val file =
      CoreFile.standardWrite(
        path = path,
        fileOpenIOStrategy = randomThreadSafeIOStrategy(),
        autoClose = true,
        forceSave = TestForceSave.standard()
      ).sweep()

    file.append(bytes)
    file.close()

    CoreFile.standardRead(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true
    ).sweep()
  }

  def createMMAPFileReader(path: Path)(implicit sweeper: TestSweeper): FileReader = {
    import sweeper._

    val file =
      CoreFile.mmapRead(
        path = path,
        fileOpenIOStrategy = randomThreadSafeIOStrategy(),
        autoClose = true,
        deleteAfterClean = OperatingSystem.isWindows
      ).sweep()

    new FileReader(file = file)
  }

  def createStandardFileFileReader(bytes: Slice[Byte])(implicit sweeper: TestSweeper): FileReader =
    createStandardFileFileReader(createFile(bytes))

  def createStandardFileFileReader(path: Path)(implicit sweeper: TestSweeper): FileReader = {
    import sweeper._

    val file =
      CoreFile.standardRead(
        path = path,
        fileOpenIOStrategy = randomThreadSafeIOStrategy(),
        autoClose = true
      ).sweep()

    new FileReader(file = file)
  }

  def createRandomFileReader(bytes: Slice[Byte])(implicit sweeper: TestSweeper): FileReader =
    createRandomFileReader(createFile(bytes))

  def randomIntDirectory()(implicit sweeper: TestSweeper): Path =
    sweeper.testDirPath.resolve(sweeper.idGenerator.toString)

  def createRandomIntDirectory()(implicit sweeper: TestSweeper): Path =
    Effect.createDirectoriesIfAbsent(randomIntDirectory()).sweep()

  def randomDir()(implicit sweeper: TestSweeper): Path =
    sweeper.testDirPath.resolve(s"${randomCharacters()}").sweep()

  def createRandomDir()(implicit sweeper: TestSweeper): Path =
    Effect.createDirectory(randomDir()).sweep()

}

