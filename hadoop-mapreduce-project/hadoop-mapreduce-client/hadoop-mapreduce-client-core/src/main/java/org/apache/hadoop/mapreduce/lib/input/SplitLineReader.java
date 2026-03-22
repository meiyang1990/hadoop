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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * 输入分片行读取器，继承自通用LineReader，为MapReduce输入分片定制行读取逻辑
 * 用于在分片边界处处理换行和跨分片记录读取，是Hadoop文本输入处理的核心组件
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SplitLineReader extends org.apache.hadoop.util.LineReader {
  /**
   * 构造函数，使用输入流和自定义记录分隔符初始化分片行读取器
   * @param in 输入流，对应输入分片的数据流
   * @param recordDelimiterBytes 自定义记录分隔符字节数组
   */
  public SplitLineReader(InputStream in, byte[] recordDelimiterBytes) {
    super(in, recordDelimiterBytes);
  }

  /**
   * 构造函数，使用配置、输入流和自定义记录分隔符初始化分片行读取器
   * @param in 输入流，对应输入分片的数据流
   * @param conf Hadoop作业配置
   * @param recordDelimiterBytes 自定义记录分隔符字节数组
   * @throws IOException 初始化读取缓冲区失败时抛出
   */
  public SplitLineReader(InputStream in, Configuration conf,
      byte[] recordDelimiterBytes) throws IOException {
    super(in, conf, recordDelimiterBytes);
  }

  /**
   * 判断分片读取结束后是否需要读取下一个分片来获取完整记录
   * 该默认实现在不处理跨分片记录时返回false，表示无需额外读取
   * @return false 当前实现不需要读取额外记录，子类可覆盖修改行为
   */
  public boolean needAdditionalRecordAfterSplit() {
    return false;
  }
}