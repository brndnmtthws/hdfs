package org.apache.mesos.hdfs;

import java.io.*;

class StreamRedirect extends Thread {
  InputStream stream;
  PrintStream outputStream;

  StreamRedirect(InputStream stream, PrintStream outputStream) {
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
