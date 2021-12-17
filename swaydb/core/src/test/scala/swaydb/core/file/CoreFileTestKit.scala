package swaydb.core.file

import org.scalatest.PrivateMethodTester._
import swaydb.core.{TestForceSave, TestSweeper}
import swaydb.core.TestSweeper._
import swaydb.core.segment.block.BlockCacheSource
import swaydb.effect.Effect
import swaydb.effect.EffectTestKit.randomThreadSafeIOStrategy
import swaydb.slice.{Slice, SliceRO}
import swaydb.testkit.TestKit._
import swaydb.utils.OperatingSystem

import java.nio.file.Path
import scala.util.Random

object CoreFileTestKit {

  private[file] def invokePrivate_file(coreFile: CoreFile): CoreFileType =
    coreFile invokePrivate PrivateMethod[CoreFileType](Symbol("file"))()

  def invokePrivate_file(reader: FileReader): CoreFile =
    reader invokePrivate PrivateMethod[CoreFile](Symbol("file"))()

  def randomFilePath()(implicit sweeper: TestSweeper): Path =
    sweeper.testDir().resolve(s"${randomCharacters()}.test").sweep()

  def createFile(bytes: Slice[Byte])(implicit sweeper: TestSweeper): Path =
    Effect.write(
      to = sweeper.testDir().resolve(sweeper.idGenerator.nextSegmentId()),
      bytes = bytes.toByteBufferWrap()
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
      left = createMMAPWriteableReadable(mmapPath, mmapBytes),
      right = createStandardWriteableReadable(channelPath, standardBytes)
    )

  def createFiles(bytes: Slice[Byte])(implicit sweeper: TestSweeper): TestTuple2[CoreFile] =
    createFiles(
      mmapBytes = bytes,
      standardBytes = bytes
    )

  def createFiles(mmapBytes: Slice[Byte], standardBytes: Slice[Byte])(implicit sweeper: TestSweeper): TestTuple2[CoreFile] =
    TestTuple2(
      left = createMMAPWriteableReadable(randomFilePath(), mmapBytes),
      right = createStandardWriteableReadable(randomFilePath(), standardBytes)
    )

  def createMMAPWriteableReadable(path: Path, bytes: Slice[Byte])(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    CoreFile.mmapWriteableReadable(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      deleteAfterClean = OperatingSystem.isWindows(),
      forceSave = TestForceSave.mmap(),
      bytes = bytes
    ).sweep()
  }

  def createWriteableMMAPFile(path: Path, bufferSize: Int)(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    CoreFile.mmapEmptyWriteableReadable(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      deleteAfterClean = OperatingSystem.isWindows(),
      forceSave = TestForceSave.mmap(),
      bufferSize = bufferSize
    ).sweep()
  }

  def createWriteableStandardFile(path: Path)(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    CoreFile.standardWritable(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true,
      forceSave = TestForceSave.standard()
    )
  }

  def createStandardWriteableReadable(path: Path, bytes: Slice[Byte])(implicit sweeper: TestSweeper): CoreFile = {
    import sweeper._

    val file =
      CoreFile.standardWritable(
        path = path,
        fileOpenIOStrategy = randomThreadSafeIOStrategy(),
        autoClose = true,
        forceSave = TestForceSave.standard()
      ).sweep()

    file.append(bytes)
    file.close()

    CoreFile.standardReadable(
      path = path,
      fileOpenIOStrategy = randomThreadSafeIOStrategy(),
      autoClose = true
    ).sweep()
  }

  def createMMAPFileReader(path: Path)(implicit sweeper: TestSweeper): FileReader = {
    import sweeper._

    val file =
      CoreFile.mmapReadable(
        path = path,
        fileOpenIOStrategy = randomThreadSafeIOStrategy(),
        autoClose = true,
        deleteAfterClean = OperatingSystem.isWindows()
      ).sweep()

    new FileReader(file = file)
  }

  def createStandardFileFileReader(bytes: Slice[Byte])(implicit sweeper: TestSweeper): FileReader =
    createStandardFileFileReader(createFile(bytes))

  def createStandardFileFileReader(path: Path)(implicit sweeper: TestSweeper): FileReader = {
    import sweeper._

    val file =
      CoreFile.standardReadable(
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

  implicit class CoreFileImplicits(file: CoreFile) {
    def toBlockCacheSource: BlockCacheSource =
      new BlockCacheSource {
        override def blockCacheMaxBytes: Int =
          file.fileSize()

        override def readFromSource(position: Int, size: Int): Slice[Byte] =
          file.read(position = position, size = size)

        override def readFromSource(position: Int, size: Int, blockSize: Int): SliceRO[Byte] =
          file.read(position = position, size = size, blockSize = blockSize)
      }
  }

}

