package dzlab

import dzlab.sharedruntime.{JepRuntimeManager, PythonPaths}
import dzlab.sharedruntime.Scalathon.pythonImport

import collection.JavaConverters._
import java.util.ArrayList
import jep.{Jep, JepConfig, JepException, NDArray, NamingConventionClassEnquirer, SharedInterpreter}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import jep.{JepConfig, JepException, NamingConventionClassEnquirer, SharedInterpreter}
import org.apache.commons.lang.exception.ExceptionUtils

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.io.Source

object SparkSpacyExample extends Code with App {
  val conf = new SparkConf()
  conf.setMaster("k8s://https://spark-dev-dns-7e313b0b.hcp.westeurope.azmk8s.io:443")
  //conf.set("spark.kubernetes.container.image", "acrsparksdp.azurecr.io/sharedruntime:test")
  conf.set("spark.kubernetes.container.image", "acrsparksdp.azurecr.io/sharedruntime:exec")
  conf.set("spark.kubernetes.container.image.pullSecrets", "acr-sdp-secret")
  conf.set("spark.kubernetes.container.image.pullPolicy", "Always")
  conf.set("spark.kubernetes.namespace", "sdp-data-lakehouse-collection-app-idris52")
  conf.set("spark.executor.memory", "2g")
  val path = "/root/miniconda3/lib/python3.8/site-packages:/root/miniconda3/lib/python3.8/site-packages/jep"
  val driverhost = s"spark-k8s-driver-svc.sdp-data-lakehouse-collection-app-idris52.svc.cluster.local"
  conf.set("spark.driver.host", driverhost)
  conf.set("spark.executor.extraJavaOptions", s"-Djava.library.path=$path")
  conf.set("spark.executor.extraLibraryPath", s"-Djava.library.path=$path")
  conf.set("spark.executorEnv.PYTHONHOME", s"/root/miniconda3/")
  conf.set("spark.executorEnv.JAVA_LIBRARY_PATH", s"$path")
  conf.set("spark.executorEnv.LD_LIBRARY_PATH", s"$path")
  conf.set("spark.executorEnv.PYTHON_FILES_PATH", s"/mnt/sharedLibs/python/")
  conf.set("spark.kubernetes.executor.podTemplateFile","/mnt/sharedLibs/podconf/podtemplate.yaml")
  //conf.set("spark.executorEnv.PYTHONHOME", s"/root/miniconda3/")
  conf.set("spark.jars", s"${getListOfJars("/opt/docker/lib", ",")}")
  conf.set(
    "spark.executor.instances", "2")
  conf.set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-operator-spark")
  val sc = SparkSession
    .builder
    .config(conf)
    .appName("Spark Pi")
    .getOrCreate()
  val scriptFile = "/mnt/sharedLibs/python/spacy_ner.py"
  println(s"Calling mappartitions")
    val s = sc.sparkContext.parallelize((1 to 10000).toList, 2).mapPartitionsWithIndex((i, d) =>{

      /*PythonInterpreter.set("tvalue", "How do I fill a DataSet or a DataTable from a LINQ query resultset ?")
      val pth =
        """print(tvalue)
          |print(list)
          |str=tvalue
          |""".stripMargin
      PythonInterpreter.exec(pth)
      val s = PythonInterpreter.getValue("str")
      println(s"value if any ${s}")*/
      callCountItems(d.toList)
      val itrmap = d.map(f => (i, f))
      println(s"${itrmap}")
      itrmap
    }).collect()
  println(s.toList)

  def getListOfJars(dir: String, sep: String): String = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      val listOfFiles = d.listFiles.filter(_.isFile).toList
      val jars        = listOfFiles.filter(l => l.isFile && l.getName.endsWith("jar"))

      println(jars.mkString(","))
      jars.mkString(sep)
    } else
      ""
  }
}
object PythonInterpreter {
  protected val logger = Logger.getLogger(this.getClass)

  private var threadPool: ExecutorService = null

  private val context = new ExecutionContext {
    threadPool = Executors.newFixedThreadPool(
      1,
      new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = Executors.defaultThreadFactory().newThread(r)
          t.setName("jep-thread " + t.getId)
          t.setDaemon(true)
          t
        }
      }
    )

    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable): Unit =
      throw t
  }
  def getSharedInterpreter(): SharedInterpreter = {
    check()
    sharedInterpreter
  }

  def check(): Unit =
    if (sharedInterpreter == null) {
      init()

    }

  def init(): Unit = synchronized {
    if (sharedInterpreter == null) {
      sharedInterpreter = createInterpreter()
    }
  }

  def get(): Jep = {
    check()
    sharedInterpreter
  }

  private var sharedInterpreter: SharedInterpreter = null
  private def createInterpreter(): SharedInterpreter = {
    logger.info(s"python home is ${System.getenv("PYTHONHOME")}")
    /*if (System.getenv("PYTHONHOME") == null) {
      throw new RuntimeException("PYTHONHOME is unset, please set PYTHONHOME first.")
    }*/
    // Load TFNet before create interpreter, or the TFNet will throw an OMP error #13
    // TFNetNative.isLoaded
    val createInterp = () => {
      val config: JepConfig = new JepConfig()
      config.setClassEnquirer(new NamingConventionClassEnquirer())
      SharedInterpreter.setConfig(config)
      val sharedInterpreter = new SharedInterpreter()
      sharedInterpreter
    }
    logger.info("Creating jep interpreter...")
    threadExecute(createInterp)
  }

  private def threadExecute[T](task: () => T, timeout: Duration = Duration("500s")): T =
    try {
      val re = Array(task)
        .map(t =>
          Future {
            t()
          }(context)
        )
        .map { future =>
          Await.result(future, timeout)
        }
      re(0)
    } catch {
      case t: Throwable =>
        // Don't use logger here, or spark local will stuck when catch an exception.
        // println("Warn: " + ExceptionUtils.getStackTrace(t))
        throw new JepException(t)
    }

  def exec(s: String): Unit = {
    logger.debug(s"jep exec ${s}")
    check()
    val func = () => {
      sharedInterpreter.exec(s)
    }
    threadExecute(func)
  }

  def execPy(fileName: String): Unit = {
    logger.debug(s"jep exec ${fileName}")
    check()
    val func = () => {
      sharedInterpreter.exec(fileName)
    }
    threadExecute(func)
  }

  def set(s: String, o: AnyRef): Unit = {
    logger.debug(s"jep set ${s}")
    check()
    val func = () => {
      sharedInterpreter.set(s, o)
    }
    threadExecute(func)
  }

  def getValue[T](name: String): T = {
    logger.debug(s"jep getValue ${name}")
    check()
    val func = () => {
      val re = sharedInterpreter.getValue(name)
      re
    }
    threadExecute(func).asInstanceOf[T]
  }
  def invoke[T](method: String, params:AnyRef*):T = {
    check()
    val func = () => {
      val re = sharedInterpreter.invoke(method, params: _*)
      re
    }
    threadExecute(func).asInstanceOf[T]
  }
}
trait Code {
  import org.slf4j.Logger
  import sharedruntime.Scalathon._
  import jep.Jep
  implicit val rm = JepRuntimeManager

  def callCountItems(list: List[Int]): Long = {
    println(s"received list is  ${list}, getting jep")
    implicit val jep: Jep = rm.jep()
    pythonImport(s"${PythonPaths.pathOrchestrationShared("ingest")}/list_test")
    val result: Long = 'count_records (list.toSeq)
    println(s"rows are ${result}")
    result
  }
}