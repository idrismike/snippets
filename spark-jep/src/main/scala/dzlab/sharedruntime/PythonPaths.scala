package dzlab.sharedruntime

import org.slf4j.LoggerFactory

import java.io.File

object PythonPaths {
  lazy implicit val log = LoggerFactory.getLogger(this.getClass.getName)

  def pathOrchestrationShared(module: String) =
    s"${getPythonFilesPath}/$module"

  def getPythonFilesPath: String = {
    val pyfilesPath = System.getenv("PYTHON_FILES_PATH")
    if (pyfilesPath == null)
      throw new RuntimeException("PYTHON_FILES_PATH is unset, please set PYTHON_FILES_PATH first.")
    pyfilesPath
  }
  val includePaths = getAllPyDirs(getPythonFilesPath)

  def getAllPyDirs(pyfilesPath: String): List[String] = {
    val d = new File(pyfilesPath)
    log.info(s"$pyfilesPath")
    if (d.exists && d.isDirectory) {
      val listOfDirs = d.listFiles.filter(_.isDirectory).toList
      val paths      = listOfDirs.map(module => s"$pyfilesPath/$module").toList
      paths
    } else List.empty[String]
  }
}
