package dzlab.sharedruntime.util

import dzlab.sharedruntime.JepRuntimeManager
import org.slf4j.LoggerFactory

/**
 * Uses Scala ~jep-> Python ~rpy2-> R in an env built from conda.base-env.yml
 *
 * Sole purpose of this spec is to prove that the overall technology stack (infrastructure) works together.
 */
trait ExecutionContextLoggingHelper {

  protected lazy val logger = LoggerFactory.getLogger("io.kse.orchestration.runtime")

  def printContext(context: String = "")( /*implicit jep: Jep*/ ) = {
    val mb      = 1024 * 1024
    val runtime = Runtime.getRuntime
    val memory =
      s"Memory / free / total / max: " +
          s"${(runtime.totalMemory - runtime.freeMemory) / mb} / " +
          s"${runtime.freeMemory / mb} / ${runtime.totalMemory / mb} " +
          s"${runtime.maxMemory / mb} \n"

    logger.info(
      s"Executing context = $context in thread / Jep = ${Thread.currentThread().getName} / ${JepRuntimeManager
        .jep(false)} \n$memory"
    )
  }
}
