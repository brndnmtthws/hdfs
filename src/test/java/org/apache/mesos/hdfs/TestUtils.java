package org.apache.mesos.hdfs;

import java.io.File;
import java.net.URL;

public class TestUtils {
  public static File computeTestDataRoot(Class anyTestClass) {
    final String clsUri = anyTestClass.getName().replace('.', '/') + ".class";
    final URL url = anyTestClass.getClassLoader().getResource(clsUri);
    final String clsPath = url.getPath();
    final File root = new File(clsPath.substring(0, clsPath.length() - clsUri.length()));
    return new File(root.getParentFile(), "test-data");
  }
}
