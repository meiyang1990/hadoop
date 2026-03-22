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
package org.apache.hadoop.hdfs.qjournal.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import java.io.IOException;

/**
 * QJournal日志节点未格式化异常类，用于表示对尚未格式化的JournalNode发起操作请求时抛出的异常
 * 在HDFS QJM共享编辑日志方案中，JournalNode需要先格式化才能存储编辑日志，未格式化时操作抛出此异常
 */
@InterfaceAudience.Private
public class JournalNotFormattedException extends IOException {
  private static final long serialVersionUID = 1L;
  
  /**
   * 构造指定异常信息的未格式化异常实例
   * @param msg 异常描述信息
   */
  public JournalNotFormattedException(String msg) {
    super(msg);
  }

}