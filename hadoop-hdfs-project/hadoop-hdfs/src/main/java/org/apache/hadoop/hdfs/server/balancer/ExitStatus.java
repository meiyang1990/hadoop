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
package org.apache.hadoop.hdfs.server.balancer;

/**
 * HDFS数据均衡器进程退出状态枚举
 * 每个退出状态对应的数据值，会直接作为命令行进程的退出码返回
 */
public enum ExitStatus {
  /** 均衡执行成功，退出码0 */
  SUCCESS(0),
  /** 均衡正在执行中，退出码1 */
  IN_PROGRESS(1),
  /** 已有均衡实例正在运行，退出码-1 */
  ALREADY_RUNNING(-1),
  /** 没有需要移动的数据块，退出码-2 */
  NO_MOVE_BLOCK(-2),
  /** 均衡执行无进展，退出码-3 */
  NO_MOVE_PROGRESS(-3),
  /** IO异常导致退出，退出码-4 */
  IO_EXCEPTION(-4),
  /** 非法参数导致退出，退出码-5 */
  ILLEGAL_ARGUMENTS(-5),
  /** 执行被中断，退出码-6 */
  INTERRUPTED(-6),
  /** 存在未完成的升级无法执行均衡，退出码-7 */
  UNFINALIZED_UPGRADE(-7);

  private final int code;

  private ExitStatus(int code) {
    this.code = code;
  }
  
  /** 
   * 获取对应当前退出状态的命令行退出码
   * @return 命令行进程退出码
   */
  public int getExitCode() {
    return code;
  }
}