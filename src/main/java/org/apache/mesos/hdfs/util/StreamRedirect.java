package org.apache.mesos.hdfs.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class StreamRedirect extends Thread {
  InputStream stream;
  PrintStream outputStream;

  public StreamRedirect(InputStream stream, PrintStream outputStream) {
    this.stream = stream;
    this.outputStream = outputStream;
  }

  public void run() {
    try {
      InputStreamReader streamReader = new InputStreamReader(stream);
      BufferedReader streamBuffer = new BufferedReader(streamReader);

      String streamLine = null;
      while ((streamLine = streamBuffer.readLine()) != null) {
        outputStream.println(streamLine);
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
