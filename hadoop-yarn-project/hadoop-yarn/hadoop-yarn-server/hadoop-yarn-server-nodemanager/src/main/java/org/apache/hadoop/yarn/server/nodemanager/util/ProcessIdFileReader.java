// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * 从进程ID文件中读取进程ID的工具类
 */
public class ProcessIdFileReader {

  private static final Logger LOG =
       LoggerFactory.getLogger(ProcessIdFileReader.class);
  
  /**
   * 从指定路径的进程ID文件中读取进程ID，返回找到的第一个有效ID
   * @param path 进程ID文件路径
   * @return 读取到的进程ID，若未找到则返回null
   * @throws IOException IO异常
   */
  public static String getProcessId(Path path) throws IOException {
    if (path == null) {
      throw new IOException("Trying to access process id from a null path");
    }
    LOG.debug("Accessing pid from pid file {}", path);
    String processId = null;
    BufferedReader bufReader = null;

    try {
      File file = new File(path.toString());
      if (file.exists()) {
        // 打开PID文件并构建缓冲读取器
        FileInputStream fis = new FileInputStream(file);
        bufReader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        // 逐行查找第一个有效的进程ID
        while (true) {
          String line = bufReader.readLine();
          if (line == null) {
            break;
          }
          String temp = line.trim(); 
          if (!temp.isEmpty()) {
            if (Shell.WINDOWS) {
              // Windows平台下，进程ID存储为容器ID格式，验证并返回第一个有效容器ID
              try {
                ContainerId.fromString(temp);
                processId = temp;
                break;
              } catch (Exception e) {
                // do nothing
              }
            }
            else {
              // 非Windows平台下，验证并返回第一个正整数格式的PID
              try {
                long pid = Long.parseLong(temp);
                if (pid > 0) {
                  processId = temp;
                  break;
                }
              } catch (Exception e) {
                // do nothing
              }
            }
          }
        }
      }
    } finally {
      // 关闭文件读取流
      if (bufReader != null) {
        bufReader.close();
      }
    }
    LOG.debug("Got pid {} from path {}",
        (processId != null ? processId : "null"), path);
    return processId;
  }

}