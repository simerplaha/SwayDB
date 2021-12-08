package swaydb.core.file

import swaydb.core.{ACoreSpec, TestCaseSweeper, TestForceSave}
import swaydb.core.CommonAssertions.randomThreadSafeIOStrategy
import swaydb.core.file.reader.FileReader
import swaydb.core.TestCaseSweeper._
import swaydb.effect.Effect
import swaydb.slice.Slice
import swaydb.utils.OperatingSystem

import java.nio.file.Path
import scala.util.Random

trait AFileSpec extends ACoreSpec {

  def createFile(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): Path =
    Effect.write(testClassDir.resolve(idGenerator.nextSegment).sweep(), bytes.toByteBufferWrap)

  def createRandomFileReader(path: Path)(implicit sweeper: TestCaseSweeper): FileReader =
    if (Random.nextBoolean())
      createMMAPFileReader(path)
    else
      createStandardFileFileReader(path)

  def createFileReaders(path: Path)(implicit sweeper: TestCaseSweeper): List[FileReader] =
    List(
      createMMAPFileReader(path),
      createStandardFileFileReader(path)
    )

  def createMMAPFileReader(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): FileReader =
    createMMAPFileReader(createFile(bytes))

  /**
   * Creates all file types currently supported which are MMAP and StandardFile.
   */
  def createFiles(mmapPath: Path,
                  mmapBytes: Slice[Byte],
                  channelPath: Path,
                  standardBytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): List[CoreFile] =
    List(
      createMMAPWriteAndRead(mmapPath, mmapBytes),
      createStandardWriteAndRead(channelPath, standardBytes)
    )

  def createFiles(mmapBytes: Slice[Byte], standardBytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): List[CoreFile] =
    List(
      createMMAPWriteAndRead(randomFilePath, mmapBytes),
      createStandardWriteAndRead(randomFilePath, standardBytes)
    )

  def createMMAPWriteAndRead(path: Path, bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): CoreFile = {
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

  def createWriteableMMAPFile(path: Path, bufferSize: Int)(implicit sweeper: TestCaseSweeper): CoreFile = {
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

  def createWriteableStandardFile(path: Path)(implicit sweeper: TestCaseSweeper): CoreFile = {
    import sweeper._

    CoreFile.standardWrite(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      forceSave = TestForceSave.channel()
    )
  }

  def createStandardWriteAndRead(path: Path, bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): CoreFile = {
    import sweeper._

    val file =
      CoreFile.standardWrite(
        path = path,
        fileOpenIOStrategy = randomThreadSafeIOStrategy(),
        autoClose = true,
        forceSave = TestForceSave.channel()
      ).sweep()

    file.append(bytes)
    file.close()

    CoreFile.standardRead(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true
    ).sweep()
  }

  def createMMAPFileReader(path: Path)(implicit sweeper: TestCaseSweeper): FileReader = {
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

  def createStandardFileFileReader(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): FileReader =
    createStandardFileFileReader(createFile(bytes))

  def createStandardFileFileReader(path: Path)(implicit sweeper: TestCaseSweeper): FileReader = {
    import sweeper._

    val file =
      CoreFile.standardRead(
        path = path,
        fileOpenIOStrategy = randomThreadSafeIOStrategy(),
        autoClose = true
      ).sweep()

    new FileReader(file = file)
  }

  def createRandomFileReader(bytes: Slice[Byte])(implicit sweeper: TestCaseSweeper): FileReader =
    createRandomFileReader(createFile(bytes))

}
