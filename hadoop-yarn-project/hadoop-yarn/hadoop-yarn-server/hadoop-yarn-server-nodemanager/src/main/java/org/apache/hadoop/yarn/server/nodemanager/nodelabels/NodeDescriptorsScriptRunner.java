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
package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TimerTask;

/**
 * 节点描述符脚本运行器，定期执行用户自定义脚本，解析脚本输出获取描述符，并提交到节点描述符提供者
 * 用于通过外部脚本动态获取节点标签等描述信息
 * @param <T> 描述符类型
 */
public abstract class NodeDescriptorsScriptRunner<T> extends TimerTask {

  private final static Logger LOG = LoggerFactory
      .getLogger(NodeDescriptorsScriptRunner.class);

  // shell命令执行器
  private final Shell.ShellCommandExecutor exec;
  // 节点描述符提供者，用于提交解析后的描述符
  private final NodeDescriptorsProvider provider;

  /**
   * 构造节点描述符脚本运行器
   * @param scriptPath 脚本路径
   * @param scriptArgs 脚本参数
   * @param scriptTimeout 脚本执行超时时间
   * @param ndProvider 节点描述符提供者
   */
  public NodeDescriptorsScriptRunner(String scriptPath,
      String[] scriptArgs, long scriptTimeout,
      NodeDescriptorsProvider ndProvider) {
    ArrayList<String> execScript = new ArrayList<>();
    execScript.add(scriptPath);
    if (scriptArgs != null) {
      execScript.addAll(Arrays.asList(scriptArgs));
    }
    this.provider = ndProvider;
    this.exec = new Shell.ShellCommandExecutor(
        execScript.toArray(new String[execScript.size()]), null, null,
        scriptTimeout);
  }

  /**
   * TimerTask定时执行入口，执行脚本并处理结果
   */
  @Override
  public void run() {
    try {
      // 执行外部脚本
      exec.execute();
      // 解析脚本输出，设置到描述符提供者
      provider.setDescriptors(parseOutput(exec.getOutput()));
    } catch (Exception e) {
      // 根据是否超时输出不同警告日志
      if (exec.isTimedOut()) {
        LOG.warn("Node Labels script timed out, Caught exception : "
            + e.getMessage(), e);
      } else {
        LOG.warn("Execution of Node Labels script failed, Caught exception : "
            + e.getMessage(), e);
      }
    }
  }

  /**
   * 清理资源，销毁残留进程
   */
  public void cleanUp() {
    if (exec != null) {
      Process p = exec.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
  }

  /**
   * 抽象方法：解析脚本输出得到描述符集合，子类实现具体解析逻辑
   * @param scriptOutput 脚本执行输出内容
   * @return 解析后的描述符集合
   * @throws IOException 解析异常
   */
  abstract Set<T> parseOutput(String scriptOutput) throws IOException;
}