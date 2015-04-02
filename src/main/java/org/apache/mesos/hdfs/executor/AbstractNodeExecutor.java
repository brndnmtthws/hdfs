package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.StreamRedirect;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class AbstractNodeExecutor implements Executor {

  public static final Log log = LogFactory.getLog(AbstractNodeExecutor.class);
  protected ExecutorInfo executorInfo;
  protected SchedulerConf schedulerConf;

  /**
   * Constructor which takes in configuration.
   **/
  @Inject
  AbstractNodeExecutor(SchedulerConf schedulerConf) {
    this.schedulerConf = schedulerConf;
  }

  /**
   * Main method which injects the configuration and state and creates the driver.
   **/
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(AbstractNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Register the framework with the executor.
   **/
  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    // Set up data dir
    setUpDataDir();
    if (!schedulerConf.usingNativeHadoopBinaries())
      createSymbolicLink();
    log.info("Executor registered with the slave");
  }

  /**
   * Delete and recreate the data directory.
   **/
  private void setUpDataDir() {
    // Create primary data dir if it does not exist
    File dataDir = new File(schedulerConf.getDataDir());
    if (!dataDir.exists()) {
      dataDir.mkdirs();
    }

    // Create secondary data dir if it does not exist
    File secondaryDataDir = new File(schedulerConf.getSecondaryDataDir());
    if (!secondaryDataDir.exists()) {
      secondaryDataDir.mkdirs();
    }
  }

  /**
   * Delete a file or directory.
   **/
  protected void deleteFile(File fileToDelete) {
    if (fileToDelete.isDirectory()) {
      String[] entries = fileToDelete.list();
      for (String entry : entries) {
        File childFile = new File(fileToDelete.getPath(), entry);
        deleteFile(childFile);
      }
    }
    fileToDelete.delete();
  }

  /**
   * Create Symbolic Link for the HDFS binary.
   **/
  private void createSymbolicLink() {
    log.info("Creating a symbolic link for HDFS binary");
    try {
      // Find Hdfs binary in sandbox
      File sandboxHdfsBinary = new File(System.getProperty("user.dir"));
      Path sandboxHdfsBinaryPath = Paths.get(sandboxHdfsBinary.getAbsolutePath());

      // Create mesosphere opt dir (parent dir of the symbolic link) if it does not exist
      File frameworkMountDir = new File(schedulerConf.getFrameworkMountPath());
      if (!frameworkMountDir.exists()) {
        frameworkMountDir.mkdirs();
      }

      // Delete and recreate directory for symbolic link every time
      String hdfsBinaryPath = schedulerConf.getFrameworkMountPath()
          + "/" + HDFSConstants.HDFS_BINARY_DIR;
      File hdfsBinaryDir = new File(hdfsBinaryPath);

      // Try to delete the symbolic link in case a dangling link is present
      try {
        ProcessBuilder processBuilder = new ProcessBuilder("unlink", hdfsBinaryPath);
        Process process = processBuilder.start();
        redirectProcess(process);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
          log.info("Unable to unlink old sym link. Link may not exist. Exit code: " + exitCode);
        }
      } catch (IOException e) {
        log.fatal("Could not unlink " + hdfsBinaryPath + ": " + e);
        System.exit(1);
      }

      // Delete the file if it exists
      if (hdfsBinaryDir.exists()) {
        deleteFile(hdfsBinaryDir);
      }

      // Create symbolic link
      Path hdfsLinkDirPath = Paths.get(hdfsBinaryPath);
      Files.createSymbolicLink(hdfsLinkDirPath, sandboxHdfsBinaryPath);
      log.info("The linked HDFS binary path is: " + sandboxHdfsBinaryPath);
      log.info("The symbolic link path is: " + hdfsLinkDirPath);
      // Adding binary to the PATH environment variable
      addBinaryToPath(hdfsBinaryPath);
    } catch (Exception e) {
      log.fatal("Error creating the symbolic link to hdfs binary: " + e);
      System.exit(1);
    }
  }

  /**
   * Add hdfs binary to the PATH environment variable by linking it to /usr/bin/hadoop. This
   * requires that /usr/bin/ is on the Mesos slave PATH, which is defined as part of the standard
   * Mesos slave packaging.
   **/
  private void addBinaryToPath(String hdfsBinaryPath) throws IOException, InterruptedException {
    if (schedulerConf.usingNativeHadoopBinaries())
      return;
    String pathEnvVarLocation = "/usr/bin/hadoop";
    String scriptContent = "#!/bin/bash \n" + hdfsBinaryPath + "/bin/hadoop \"$@\"";
    FileWriter fileWriter = new FileWriter(pathEnvVarLocation);
    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
    bufferedWriter.write(scriptContent);
    bufferedWriter.close();
    ProcessBuilder processBuilder = new ProcessBuilder("chmod", "a+x", pathEnvVarLocation);
    Process process = processBuilder.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      log.fatal("Error creating the symbolic link to hdfs binary."
          + "Failure running chmod a+x " + pathEnvVarLocation);
      System.exit(1);
    }
  }

  /**
   * Starts a task's process so it goes into running state.
   **/
  protected void startProcess(ExecutorDriver driver, Task task) {
    reloadConfig();
    if (task.process == null) {
      try {
        ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c", task.cmd);
        task.process = processBuilder.start();
        redirectProcess(task.process);
      } catch (IOException e) {
        log.error(e);
        task.process.destroy();
        sendTaskFailed(driver, task);
      }
    } else {
      log.error("Tried to start process, but process already running");
    }
  }

  /**
   * Reloads the cluster configuration so the executor has the correct configuration info.
   **/
  protected void reloadConfig() {
    if (schedulerConf.usingNativeHadoopBinaries()) return;
    // Find config URI
    String configUri = "";
    for (CommandInfo.URI uri : executorInfo.getCommand().getUrisList()) {
      if (uri.getValue().contains("hdfs-site.xml")) {
        configUri = uri.getValue();
      }
    }
    if (configUri.isEmpty()) {
      log.error("Couldn't find hdfs-site.xml URI");
      return;
    }
    try {
      log.info(String.format("Reloading hdfs-site.xml from %s", configUri));
      ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c",
          String.format("curl -o hdfs-site.xml %s && cp hdfs-site.xml etc/hadoop/", configUri));
      Process process = processBuilder.start();
      //TODO(nicgrayson) check if the config has changed
      redirectProcess(process);
      int exitCode = process.waitFor();
      log.info("Finished reloading hdfs-site.xml, exited with status " + exitCode);
      if (exitCode != 0) {
        log.error("Error reloading hdfs-site.xml.");
      }
    } catch (InterruptedException | IOException e) {
      log.error("Caught exception", e);
    }
  }

  /**
   * Redirects a process to STDERR and STDOUT for logging and debugging purposes.
   **/
  protected void redirectProcess(Process process) {
    StreamRedirect stdoutRedirect = new StreamRedirect(process.getInputStream(), System.out);
    stdoutRedirect.start();
    StreamRedirect stderrRedirect = new StreamRedirect(process.getErrorStream(), System.err);
    stderrRedirect.start();
  }

  /**
   * Run a command and wait for it's successful completion.
   **/
  protected void runCommand(ExecutorDriver driver, Task task, String command) {
    reloadConfig();
    try {
      log.info(String.format("About to run command: %s", command));
      ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c", command);
      Process init = processBuilder.start();
      redirectProcess(init);
      int exitCode = init.waitFor();
      log.info("Finished running command, exited with status " + exitCode);
      if (exitCode != 0) {
        log.error("Unable to run command: " + command);
        task.process.destroy();
        sendTaskFailed(driver, task);
      }
    } catch (InterruptedException | IOException e) {
      log.error(e);
      task.process.destroy();
      sendTaskFailed(driver, task);
    }
  }

  /**
   * Abstract method to launch a task.
   **/
  public abstract void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo);

  /**
   * Let the scheduler know that the task has failed.
   **/
  private void sendTaskFailed(ExecutorDriver driver, Task task) {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(task.taskInfo.getTaskId())
        .setState(TaskState.TASK_FAILED)
        .build());
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    log.info("Executor reregistered with the slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    log.info("Executor disconnected from the slave");
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    reloadConfig();
    String messageStr = new String(msg);
    log.info("Executor received framework message: " + messageStr);
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    log.error(this.getClass().getName() + ".error: " + message);
  }

  /**
   * The task class for use within the executor
   **/
  public class Task {
    public TaskInfo taskInfo;
    public String cmd;
    public Process process;

    Task(TaskInfo taskInfo) {
      this.taskInfo = taskInfo;
      this.cmd = taskInfo.getData().toStringUtf8();
      log.info(String.format("Launching task, taskId=%s cmd='%s'", taskInfo.getTaskId().getValue(),
          cmd));
    }
  }
}
