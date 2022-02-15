package dzlab.sharedruntime

import dzlab.sharedruntime.util.CompositeAliasingClassLoader
import jep.{ClassEnquirer, ClassList, Jep, JepConfig}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.slf4j.LoggerFactory

import scala.language.implicitConversions

/**
 *
 */
object Scalathon {

  protected lazy val logger         = LoggerFactory.getLogger("io.kse.datafabric.runtime")
  val stdout: ByteArrayOutputStream = new ByteArrayOutputStream()

  val config = {
    val ci  = ClassList.getInstance()
    val ccl = new CompositeAliasingClassLoader()
    ccl.add(getClass.getClassLoader)

    /**
     * #design ability to import Scala classes from Python
     * #todo had trouble
     */
    //ccl.add(classOf[DataWindowRaw].getClassLoader)
    new JepConfig()
    //.redirectStdout(stdout)
    //.setIncludePath("/Users/idris52/Git/sdp-lakehouseingest_poc/modules/data-services/device-profile/client/src/test/resources/python")
    /*.addSharedModules(
        /**
     * Go around known bug in Python with _strptime
     *
     * #resource
     * https://groups.google.com/d/msg/jep-project/-CC0NAKGjFg/pvhu0rtZBgAJ
     * https://bugs.python.org/issue27400
     */
        "_strptime",
        "numpy",
        "rpy2",
        "pandas",
        "pyarrow",
        "pyspark",
        "tensorflow"
      )*/
    /**
       * A way to be able to execute Python code embedded in the code base (se python folders)
       * #resource
       * https://stackoverflow.com/questions/897792/where-is-pythons-sys-path-initialized-from
       */
      .addIncludePaths(PythonPaths.includePaths: _*)
      /**
       * #design Introduced a mechanism to enable importing in Python "module.py" Java and Scala classes
       * which already exist in the current classloader but are not importable.
       * This approach also uses an aliasing classloader which goes around that importing
       * {{{
       *   from io_kse.utils import SparkConnection
       * }}}
       * confuses jep because "io" is also a Python package - it is undecidable if that is the base of a
       * Python or Java package.
       *
       * #resource https://groups.google.com/d/msg/jep-project/uuMVFAKhIY0/vih6VDceCQAJ
       */
      .setClassEnquirer(new ClassEnquirer {

        val originalPrefix = "io.kse"
        val tweakedPrefix  = "io_kse"
        val aliasedPackages =
          List(tweakedPrefix, s"$tweakedPrefix.utils", s"$tweakedPrefix.datafabric", s"$tweakedPrefix.datafabric.shared")

        override def isJavaPackage(name: String): Boolean =
          if (ci.isJavaPackage(name)) {
            logger.debug(s"isJavaPackage($name) = true")
            true
          } else if (aliasedPackages.contains(name)) {
            logger.debug(s"isJavaPackage($name) = true")
            true
          } else {
            logger.debug(s"isJavaPackage($name) = false")
            false
          }

        override def getClassNames(pkgName: String): Array[String] = {

          logger.debug(s"getClassNames($pkgName)")
          val classNames =
            if (pkgName.startsWith(tweakedPrefix)) {
              ci.getClassNames(pkgName.replace(tweakedPrefix, originalPrefix))
            } else {
              ci.getClassNames(pkgName)
            }

          if (classNames.isEmpty) {
            logger.warn(s"getClassNames($pkgName) returns nothing")
          }
          classNames
        }

        override def getSubPackages(pkgName: String): Array[String] = {
          logger.debug(s"getSubPackages($pkgName)")

          val pkgNames = ci.getSubPackages(pkgName)

          //          val pkgNames: Array[String] =
          //          if(pkgName.startsWith(tweakedPrefix)) {
          //            var subpkgs = ci.getSubPackages(pkgName.replace(tweakedPrefix, originalPrefix))
          //
          //            if(subpkgs.isEmpty) {
          //              if(pkgName.equals("io_kse.datafabric.shared.datamgmt"))
          //                  Array("io.kse.datafabric.orchestration.shared.datamgmt.impl")
          //                else
          //              Array.empty[String]
          //            } else
          //              Array.empty[String]
          //          } else {
          //            ci.getSubPackages(pkgName)
          //          }

          if (pkgNames.isEmpty) {
            logger.warn(s"getSubPackages($pkgName) returns nothing")
          }
          pkgNames
        }
      })
      .setClassLoader(ccl)
  }

  /**
   *
   */
  def pythonImport(scriptName: String)(implicit jep: Jep): String =
    try {
      logger.info(s"Loading script $scriptName.py ...")

      /**
       * Going around Jep is not able to run scripts from classpath / resources
       * Read from bundled resources and generate a tmp file to runScript from.
       */
      import java.nio.charset.StandardCharsets
      val source = scala.io.Source.fromFile(s"$scriptName.py", StandardCharsets.UTF_8.name())
      //val source = scala.io.Source.fromURL(s"$scriptName.py", StandardCharsets.UTF_8.name())
      //val source = scala.io.Source.fromFile(getClass.getResource(s"$scriptName.py", StandardCharsets.UTF_8.name()))
      val script =
        try source.getLines().reduceLeft(_ + "\n" + _)
        finally source.close()

      import java.nio.file.{Files, Paths}
      val file = Files.write(Files.createTempFile(Paths.get("/tmp"), "ypy_", ".py"), script.getBytes(StandardCharsets.UTF_8))

      logger.info(
        s"Executing $scriptName.py from $file ."
      )
      logger.info(s"name ... ${file.getFileName}....")

      // #todo need this ?
      // jep.setClassLoader(getClass.getClassLoader)
      jep.runScript(file.toAbsolutePath.toString /*, getClass.getClassLoader*/ )

      logger.info("Done.")

      //#todo can we delete this before jep instance is closed?
      file.getFileName.toString //Files.delete(file)
    } catch {
      case e: Exception =>
        logger.error(s"Error in pythonImport ${e.getMessage}")
        // #todo consider wrapping into PlatformException
        throw e
    }

  class PyFunction[R](method: Symbol)(implicit jep: Jep) extends ((Any*) => R) {

    override def apply(params: Any*): R = {
      val paramObjects = params.map(_.asInstanceOf[Object])
      val result       = jep.invoke(sym2string(method), paramObjects: _*)
      //if(result != null)
      result.asInstanceOf[R]
      //      else
      //        throw new NullPointerException(s"Calling this...")
    }
  }

  def sym2string(sym: Symbol) = {
    val back = sym.toString()
    back.substring(1, back.length())
  }

  implicit def sym2Method[R](sym: Symbol)(implicit jep: Jep): (Any*) => R =
    new PyFunction[R](sym)
}
