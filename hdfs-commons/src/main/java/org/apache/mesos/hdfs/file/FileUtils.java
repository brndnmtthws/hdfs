package org.apache.mesos.hdfs.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Used for hdfs file system operations.
 */
public final class FileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {
  }

  public static void createDir(File dataDir) {
    if (dataDir.exists()) {
      LOG.info("data dir exits:" + dataDir);
    } else if (!dataDir.mkdirs()) {
      LOG.error("unable to create dir: " + dataDir);
    }
  }

  /**
   * Delete a file or directory.
   */
  public static boolean deleteDirectory(File fileToDelete) {
    boolean deleted = false;

    try {
      if (fileToDelete.isDirectory()) {
        org.apache.commons.io.FileUtils.deleteDirectory(fileToDelete);
        deleted = true;
      } else {
        LOG.error("File is not a directory: " + fileToDelete);
      }
    } catch (IOException e) {
      LOG.error("Unable to delete directory: " + fileToDelete);
    }

    return deleted;
  }
}
