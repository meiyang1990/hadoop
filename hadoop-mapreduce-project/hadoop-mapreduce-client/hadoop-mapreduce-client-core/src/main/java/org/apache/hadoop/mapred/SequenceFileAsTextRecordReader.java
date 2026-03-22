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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 文件说明: SequenceFile输入格式的RecordReader实现，将SequenceFile中原始键值对转换为Text格式，
 * 通过调用原始类型的toString()方法完成转换。对应关系为：本类对应SequenceFileAsTextInputFormat，
 * 正如LineRecordReader对应TextInputFormat。
 * 
 * 核心职责: 为MapReduce任务提供从SequenceFile中读取Text类型键值对的能力，让原本存储任意可序列化
 * 类型的SequenceFile可以直接作为文本输入处理。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsTextRecordReader
  implements RecordReader<Text, Text> {
  
  // 持有原始SequenceFile的RecordReader，委托实际读取操作
  private final SequenceFileRecordReader<WritableComparable, Writable>
  sequenceFileRecordReader;

  // 存储原始键对象，用于读取底层SequenceFile数据
  private WritableComparable innerKey;
  // 存储原始值对象，用于读取底层SequenceFile数据
  private Writable innerValue;

  /**
   * 构造函数，初始化底层SequenceFileRecordReader，创建原始键值对象
   * @param conf 作业配置对象
   * @param split 要读取的输入分片
   * @throws IOException 初始化或读取分片失败时抛出
   */
  public SequenceFileAsTextRecordReader(Configuration conf, FileSplit split)
    throws IOException {
    sequenceFileRecordReader =
      new SequenceFileRecordReader<WritableComparable, Writable>(conf, split);
    innerKey = sequenceFileRecordReader.createKey();
    innerValue = sequenceFileRecordReader.createValue();
  }

  /**
   * 创建输出键对象，类型为Text
   * @return 新建的Text实例作为输出键
   */
  public Text createKey() {
    return new Text();
  }
  
  /**
   * 创建输出值对象，类型为Text
   * @return 新建的Text实例作为输出值
   */
  public Text createValue() {
    return new Text();
  }

  /** Read key/value pair in a line. */
  /**
   * 读取下一个键值对，将原始键值转换为文本格式
   * @param key 输出转换后的文本键
   * @param value 输出转换后的文本值
   * @return 是否成功读取到下一个键值对，到达分片末尾返回false
   * @throws IOException 读取数据失败时抛出
   */
  public synchronized boolean next(Text key, Text value) throws IOException {
    Text tKey = key;
    Text tValue = value;
    // 从底层SequenceFile读取下一个原始键值对
    if (!sequenceFileRecordReader.next(innerKey, innerValue)) {
      return false;
    }
    // 将原始键的字符串形式写入输出Text
    tKey.set(innerKey.toString());
    // 将原始值的字符串形式写入输出Text
    tValue.set(innerValue.toString());
    return true;
  }
  
  /**
   * 获取当前读取进度百分比
   * @return 0.0到1.0之间的进度值
   * @throws IOException 获取进度失败时抛出
   */
  public float getProgress() throws IOException {
    return sequenceFileRecordReader.getProgress();
  }
  
  /**
   * 获取当前读取位置偏移量
   * @return 当前位置字节偏移量
   * @throws IOException 获取位置失败时抛出
   */
  public synchronized long getPos() throws IOException {
    return sequenceFileRecordReader.getPos();
  }
  
  /**
   * 关闭底层读取器，释放资源
   * @throws IOException 关闭失败时抛出
   */
  public synchronized void close() throws IOException {
    sequenceFileRecordReader.close();
  }
  
}