// The contents of this file are subject to the Mozilla Public License
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License
// at https://www.mozilla.org/en-US/MPL/2.0/
//
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
// the License for the specific language governing rights and
// limitations under the License.
//
// The Original Code is RabbitMQ.
//
// The Initial Developer of the Original Code is Pivotal Software, Inc.
// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
//

package com.rabbitmq.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Host {

  private static String capture(InputStream is) throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    StringBuilder buff = new StringBuilder();
    while ((line = br.readLine()) != null) {
      buff.append(line).append("\n");
    }
    return buff.toString();
  }

  private static Process executeCommand(String command) throws IOException {
    Process pr = executeCommandProcess(command);

    int ev = waitForExitValue(pr);
    if (ev != 0) {
      String stdout = capture(pr.getInputStream());
      String stderr = capture(pr.getErrorStream());
      throw new IOException(
          "unexpected command exit value: "
              + ev
              + "\ncommand: "
              + command
              + "\n"
              + "\nstdout:\n"
              + stdout
              + "\nstderr:\n"
              + stderr
              + "\n");
    }
    return pr;
  }

  private static int waitForExitValue(Process pr) {
    while (true) {
      try {
        pr.waitFor();
        break;
      } catch (InterruptedException ignored) {
      }
    }
    return pr.exitValue();
  }

  private static Process executeCommandProcess(String command) throws IOException {
    String[] finalCommand;
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      finalCommand = new String[4];
      finalCommand[0] = "C:\\winnt\\system32\\cmd.exe";
      finalCommand[1] = "/y";
      finalCommand[2] = "/c";
      finalCommand[3] = command;
    } else {
      finalCommand = new String[3];
      finalCommand[0] = "/bin/sh";
      finalCommand[1] = "-c";
      finalCommand[2] = command;
    }
    return Runtime.getRuntime().exec(finalCommand);
  }

  public static Process rabbitmqctl(String command) throws IOException {
    return rabbitmqctl(command, node1name());
  }

  public static Process rabbitmqctl(String command, String nodename) throws IOException {
    return executeCommand(rabbitmqctlCommand() + " -n '" + nodename + "'" + " " + command);
  }

  public static String node1name() {
    return System.getProperty("node1.name", "rabbit-1@" + hostname());
  }

  public static String node2name() {
    return System.getProperty("node2.name", "rabbit-2@" + hostname());
  }

  public static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      try {
        Process process = executeCommand("hostname");
        return capture(process.getInputStream()).trim();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  static String rabbitmqctlCommand() {
    return System.getProperty("rabbitmqctl.bin");
  }
}
