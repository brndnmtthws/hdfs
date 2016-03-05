package org.apache.mesos.stream;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;

/**
 * Provides Steam utility functions.
 */
public class StreamUtil {

  /**
   * Redirects a process to STDERR and STDOUT for logging and debugging purposes.
   */
  public static void redirectProcess(Process process, PrintStream out, PrintStream err) {
    StreamRedirect stdoutRedirect = new StreamRedirect(process.getInputStream(), out);
    new Thread(stdoutRedirect).start();
    StreamRedirect stderrRedirect = new StreamRedirect(process.getErrorStream(), err);
    new Thread(stderrRedirect).start();
  }

  public static void redirectProcess(Process process) {
    redirectProcess(process, System.out, System.err);
  }

  public static void closeQuietly(Socket socket) {
    IOUtils.closeQuietly(socket);
  }

  public static void closeQuietly(InputStream input) {
    IOUtils.closeQuietly(input);
  }

  public static void closeQuietly(OutputStream output) {
    IOUtils.closeQuietly(output);
  }
}
