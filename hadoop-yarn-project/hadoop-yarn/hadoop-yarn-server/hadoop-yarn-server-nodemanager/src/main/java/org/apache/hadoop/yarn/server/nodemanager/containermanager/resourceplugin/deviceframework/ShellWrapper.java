// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.hadoop.util.Shell;

import java.io.File;
import java.io.IOException;

/**
 * 系统命令shell执行封装器，对Shell操作进行抽象，便于单元测试
 * */
public class ShellWrapper {

  /**
   * 获取指定设备文件的文件类型
   * @param devName 设备文件路径
   * @return 设备文件类型字符串
   * @throws IOException 命令执行失败时抛出IO异常
   */
  public String getDeviceFileType(String devName) throws IOException {
    // 构造stat命令获取文件类型
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
        new String[]{"stat", "-c", "%F", devName});
    // 执行命令
    shexec.execute();
    // 返回命令输出结果
    return shexec.getOutput();
  }

  /**
   * 检查指定路径的文件是否存在
   * @param path 待检查的文件路径
   * @return 文件存在返回true，否则返回false
   */
  public boolean existFile(String path) {
    File searchFile =
        new File(path);
    if (searchFile.exists()) {
      return true;
    }
    return false;
  }
}