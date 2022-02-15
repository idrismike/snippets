package dzlab.sharedruntime

import dzlab.sharedruntime.util.{ExecutionContextLoggingHelper, SystemUtils}
import jep.{Jep, SubInterpreter}

/**
 * Manages the runtime [[Jep]] instances : they need to be thread bound, because they have "thread affinity"
 *
 *
 */
trait JepRuntimeManager extends ExecutionContextLoggingHelper with Serializable {

  /**
   * Uses thread local management for [[Jep]] instances
   */
  lazy val local: ThreadLocal[Jep] = new ThreadLocal[Jep]

  protected def getCurrent: Jep = local.get()

  protected def setCurrent(jep: Jep) = {

    if (local.get() != null) {
      val msg = s"setCurrent : attach instance overrides the current one $jep."
      logger.error(msg)
      throw new RuntimeException(msg)
    }
    if (jep == null) {
      val msg = s"setCurrent : attach null instance."
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    logger.debug(s"setCurrent : attach instance $jep")
    local.set(jep)
  }

  /**
   * @param ensureInitialized flag to ensure a jep instance exists on the current thread.
   */
  def jep(ensureInitialized: Boolean = true): Jep = {

    val jep: Jep =
      if (getCurrent == null && ensureInitialized) {
        println(s"config ${Scalathon.config}")

        val jepNew = new SubInterpreter(Scalathon.config)
        logger.debug(s"jep() : new instance $jepNew.")
        setCurrent(jepNew)
        jepNew

      } else {
        val jepCurrent = getCurrent

        if (jepCurrent == null)
          logger.debug(s"jep(): no instance present.")
        else
          logger.debug(s"jep(): reuse instance $jepCurrent.")
        jepCurrent
      }

    if (ensureInitialized) {
      assert(jep != null)
    }

    if (jep != null) {
      jep.isValidThread()
      logger.info(s"jep(): access in PID / thread = ${SystemUtils.getPID} / ${Thread.currentThread().getName} / $jep.")
    }
    jep
  }

  def closeJep(): Unit = {

    val jep = getCurrent

    if (jep != null) {
      logger.debug(s"closeJep(): close instance $jep")
      jep.close()
    } else {
      logger.debug(s"closeJep(): no instance.")
    }
  }
}

object JepRuntimeManager extends JepRuntimeManager

import jep.{JepConfig, JepException, NamingConventionClassEnquirer, SharedInterpreter}
import org.apache.log4j.Logger

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

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
  def getSharedInterpreter(): SharedInterpreter =
    sharedInterpreter

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

  private def threadExecute[T](task: () => T, timeout: Duration = Duration("100s")): T =
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
}
