package org.apache.mesos.hdfs.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Used for file system operations
 */
public final class FileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {
  }

  public static void createDir(File dataDir) {
    if (dataDir.exists()) {
      LOG.warn("data dir exits:" + dataDir);
    } else if (!dataDir.mkdirs()) {
      LOG.error("unable to create dir: " + dataDir);
    }
  }

  /**
   * Delete a file or directory.
   */
  public static boolean deleteFile(File fileToDelete) {
    if (fileToDelete.isDirectory()) {
      String[] entries = fileToDelete.list();
      for (String entry : entries) {
        File childFile = new File(fileToDelete.getPath(), entry);
        deleteFile(childFile);
      }
    }
    return fileToDelete.delete();
  }
}
