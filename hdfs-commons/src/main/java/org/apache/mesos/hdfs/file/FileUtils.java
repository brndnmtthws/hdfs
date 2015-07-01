package org.apache.mesos.hdfs.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author kensipe
 */
public class FileUtils {

  private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

  public FileUtils() {
  }

  public static void createDir(File dataDir) {
    if (dataDir.exists()) {
      logger.warn("data dir exits:" + dataDir);
    } else if (!dataDir.mkdirs()) {
      logger.error("unable to create dir: " + dataDir);
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
