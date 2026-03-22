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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：NameNode元数据恢复过程的上下文容器，保存恢复过程中的配置参数和交互状态，
 * 用于支持元数据损坏/损坏编辑日志恢复过程中的用户交互和强制策略控制。
 * <p>
 * Context data for an ongoing NameNode metadata recovery process.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class MetaRecoveryContext  {
  public static final Logger LOG =
      LoggerFactory.getLogger(MetaRecoveryContext.class.getName());
  // 不强制选择任何选项，需要用户交互确认
  public final static int FORCE_NONE = 0;
  // 强制始终选择第一个选项，不再向用户提问
  public final static int FORCE_FIRST_CHOICE = 1;
  // 强制选择所有默认选项，全程自动执行恢复
  public final static int FORCE_ALL = 2;
  private int force;
  
  /**
   * 用户请求停止恢复处理时抛出的异常，用于终止当前编辑日志加载流程。
   * Exception thrown when the user has requested processing to stop.
   */
  static public class RequestStopException extends IOException {
    private static final long serialVersionUID = 1L;
    /**
     * 构造请求停止异常，携带停止原因信息。
     * @param msg 停止原因描述
     */
    public RequestStopException(String msg) {
      super(msg);
    }
  }
  
  /**
   * 构造元数据恢复上下文，指定强制选择策略。
   * @param force 强制策略类型，取值为FORCE_NONE/FORCE_FIRST_CHOICE/FORCE_ALL
   */
  public MetaRecoveryContext(int force) {
    this.force = force;
  }

  /**
   * 向用户输出提示问题，读取并返回用户选择，根据当前强制策略自动选择或等待用户输入。
   *  
   * @param prompt      要显示的提示文字
   * @param firstChoice 第一个选项（当强制策略开启时自动选择此选项）
   * @param choices     其他可选选项
   *
   * @return            用户实际选择的选项
   * @throws IOException 读取用户输入失败时抛出
   */
  public String ask(String prompt, String firstChoice, String... choices) 
      throws IOException {
    while (true) {
      // 输出提示文字到标准错误
      System.err.print(prompt);
      // 如果开启强制选择，自动选择第一个选项返回
      if (force > FORCE_NONE) {
        System.out.println("automatically choosing " + firstChoice);
        return firstChoice;
      }
      StringBuilder responseBuilder = new StringBuilder();
      // 循环读取用户输入直到换行或流结束
      while (true) {
        int c = System.in.read();
        if (c == -1 || c == '\r' || c == '\n') {
          break;
        }
        responseBuilder.append((char)c);
      }
      // 去除换行后的用户输入
      String response = responseBuilder.toString();
      // 匹配第一个选项，返回结果
      if (response.equalsIgnoreCase(firstChoice))
        return firstChoice;
      // 遍历匹配其他选项，匹配成功则返回
      for (String c : choices) {
        if (response.equalsIgnoreCase(c)) {
          return c;
        }
      }
      // 输入不匹配，提示错误并重试
      System.err.print("I'm sorry, I cannot understand your response.\n");
    }
  }

  /**
   * 编辑日志加载出错时向用户弹出处理选项提示，根据用户选择执行对应操作。
   * @param prompt 错误提示信息
   * @param recovery 元数据恢复上下文，为null则直接抛出IO异常
   * @param contStr 继续操作的描述文字
   * @throws IOException 上下文为null时抛出原始错误
   * @throws RequestStopException 用户请求停止加载时抛出
   */
  public static void editLogLoaderPrompt(String prompt,
        MetaRecoveryContext recovery, String contStr)
        throws IOException, RequestStopException
  {
    // 无恢复上下文，直接抛出错误终止
    if (recovery == null) {
      throw new IOException(prompt);
    }
    // 记录错误日志
    LOG.error(prompt);
    // 向用户发起询问，获取选择结果
    String answer = recovery.ask("\nEnter 'c' to continue, " + contStr + "\n" +
      "Enter 's' to stop reading the edit log here, abandoning any later " +
        "edits\n" +
      "Enter 'q' to quit without saving\n" +
      "Enter 'a' to always select the first choice in the future " +
      "without prompting. " + 
      "(c/s/q/a)\n", "c", "s", "q", "a");
    // 选择继续：记录日志后返回，继续加载编辑日志
    if (answer.equals("c")) {
      LOG.info("Continuing");
      return;
    } else if (answer.equals("s")) {
      // 选择停止：抛出异常终止编辑日志加载
      throw new RequestStopException("user requested stop");
    } else if (answer.equals("q")) {
      // 选择退出：直接退出进程
      recovery.quit();
    } else {
      // 选择始终默认：设置强制选择第一个选项策略，继续加载
      recovery.setForce(FORCE_FIRST_CHOICE);
      return;
    }
  }

  /**
   * 记录退出日志并按用户请求退出进程。
   */
  public void quit() {
    LOG.error("Exiting on user request.");
    System.exit(0);
  }

  /**
   * 获取当前强制策略类型。
   * @return 强制策略类型
   */
  public int getForce() {
    return this.force;
  }

  /**
   * 设置当前强制策略类型。
   * @param force 新的强制策略类型
   */
  public void setForce(int force) {
    this.force = force;
  }
}