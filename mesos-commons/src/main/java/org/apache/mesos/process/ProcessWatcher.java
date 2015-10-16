package org.apache.mesos.process;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Invokes the specified handler on process exit.
 */
public class ProcessWatcher {
  private final Log log = LogFactory.getLog(ProcessWatcher.class);
  private ProcessFailureHandler handler;

  public ProcessWatcher(ProcessFailureHandler handler) {
    this.handler = handler;
  }

  public void watch(final Process proc) {
    log.info("Watching process: " + proc);

    Runnable r = new Runnable() {
      public void run() {
        try {
          proc.waitFor();
        } catch (Exception ex) {
          log.error("Process excited with exception: " + ex);
        }

        log.error("Handling failure of process: " + proc);
        handler.handle();
      }
    };

    new Thread(r).start();
  }
}
