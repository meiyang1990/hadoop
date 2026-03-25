// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件输出提交器清单文件的诊断信息键常量定义。
 * 存储输出提交过程中各类诊断数据的key，用于_SUCCESS文件和故障排查。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class DiagnosticKeys {
  /**
   * 提交操作使用的Kerberos用户主体，添加到_SUCCESS文件的诊断属性中。
   */
  public static final String PRINCIPAL = "principal";
  /**
   * 当前所处的提交阶段
   */
  public static final String STAGE = "stage";
  /**
   * 异常类型名称
   */
  public static final String EXCEPTION = "exception";
  /**
   * 异常堆栈信息
   */
  public static final String STACKTRACE = "stacktrace";
  /**
   * JVM总内存大小
   */
  public static final String TOTAL_MEMORY = "total.memory";
  /**
   * JVM空闲内存大小
   */
  public static final String FREE_MEMORY = "free.memory";
  /**
   * JVM堆内存使用量
   */
  public static final String HEAP_MEMORY = "heap.memory";


  /** 重命名后的清单文件所在目录键名: {@value}. */
  public static final String MANIFESTS = "manifests";

  private DiagnosticKeys() {
  }
}