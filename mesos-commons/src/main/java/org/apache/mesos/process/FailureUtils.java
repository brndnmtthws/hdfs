package org.apache.mesos.process;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Failure utilities.
 */
public class FailureUtils {
  private static final Log log = LogFactory.getLog(FailureUtils.class);

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = "DM_EXIT",
    justification = "Framework components should fail fast sometimes.")
  public static void exit(String msg, Integer exitCode) {
    log.fatal(msg);
    System.exit(exitCode);
  }
}
