package org.apache.mesos.hdfs.util;

import java.io.File;

public class PathUtil {
  public static String combine(String path1, String path2) {
    File file1 = new File(path1);
    File file2 = new File(file1, path2);
    return file2.getPath();
  }

  public static String combine(String path1, String path2, String path3) {
    return combine(combine(path1, path2), path3);
  }

  public static String combine(String path1, String path2, String path3, String path4) {
    return combine(combine(combine(path1, path2), path3), path4);
  }
}
