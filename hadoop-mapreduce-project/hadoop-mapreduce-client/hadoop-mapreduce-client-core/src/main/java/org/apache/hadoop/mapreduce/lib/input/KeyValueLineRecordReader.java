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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件级注释：KeyValue行记录读取器，将输入文本的每一行按分隔符切分为键值对，作为MapReduce输入记录
 * 本类将输入中每一行文本，按照指定分隔符分割为键和值两个部分，分隔符可通过配置参数指定，默认分隔符为制表符'\t'
 * 用于处理每行以键值对形式存储的文本输入数据
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyValueLineRecordReader extends RecordReader<Text, Text> {
  /** 键值分隔符配置项名称 */
  public static final String KEY_VALUE_SEPARATOR =
      "mapreduce.input.keyvaluelinerecordreader.key.value.separator";
  /**
   * @deprecated Use {@link #KEY_VALUE_SEPARATOR}
   * 拼写错误的旧配置项名称，已废弃，使用新配置项KEY_VALUE_SEPARATOR
   */
  @Deprecated
  public static final String KEY_VALUE_SEPERATOR = KEY_VALUE_SEPARATOR;

  /** 封装底层行读取逻辑的LineRecordReader实例 */
  private final LineRecordReader lineRecordReader;

  /** 键值分隔符，字节形式存储 */
  private byte separator = (byte) '\t';

  /** 存储LineRecordReader读取到的当前行原始内容 */
  private Text innerValue;

  /** 当前记录的键 */
  private Text key;
  
  /** 当前记录的值 */
  private Text value;
  
  /**
   * 获取键的类型
   * @return 键的Class对象，固定为Text.class
   */
  public Class getKeyClass() { return Text.class; }
  
  /**
   * 构造函数，初始化KeyValueLineRecordReader，加载配置的分隔符
   * @param conf 作业配置对象，用于获取键值分隔符配置
   * @throws IOException 初始化失败时抛出IO异常
   */
  public KeyValueLineRecordReader(Configuration conf)
    throws IOException {
    
    lineRecordReader = new LineRecordReader();
    String sepStr = conf.get(KEY_VALUE_SEPARATOR, "\t");
    this.separator = (byte) sepStr.charAt(0);
  }

  /**
   * 初始化记录读取器，委托底层LineRecordReader完成输入分片初始化
   * @param genericSplit 待读取的输入分片
   * @param context 任务尝试上下文对象
   * @throws IOException 初始化失败时抛出IO异常
   */
  public void initialize(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    lineRecordReader.initialize(genericSplit, context);
  }
  
  /**
   * 在字节数组中查找指定分隔符的位置，从给定起始位置开始查找
   * @param utf 待查找的字节数组（UTF-8编码行内容）
   * @param start 查找起始偏移量
   * @param length 查找范围长度
   * @param sep 目标分隔符字节
   * @return 分隔符所在位置索引，未找到返回-1
   */
  public static int findSeparator(byte[] utf, int start, int length, 
      byte sep) {
    for (int i = start; i < (start + length); i++) {
      if (utf[i] == sep) {
        return i;
      }
    }
    return -1;
  }

  /**
   * 根据分隔符位置将行内容切割并设置到键和值对象中
   * @param key 存储切割后键的对象
   * @param value 存储切割后值的对象
   * @param line 行内容字节数组
   * @param lineLen 行内容有效长度
   * @param pos 分隔符位置，-1表示未找到分隔符
   */
  public static void setKeyValue(Text key, Text value, byte[] line,
      int lineLen, int pos) {
    if (pos == -1) {
      key.set(line, 0, lineLen);
      value.set("");
    } else {
      key.set(line, 0, pos);
      value.set(line, pos + 1, lineLen - pos - 1);
    }
  }
  /**
   * 读取下一个键值对记录，读取一行并切割为键值
   * @return 是否成功读取到下一条记录，没有更多记录返回false
   * @throws IOException 读取过程发生IO异常时抛出
   */
  public synchronized boolean nextKeyValue()
    throws IOException {
    byte[] line = null;
    int lineLen = -1;
    // 先通过LineRecordReader读取下一行
    if (lineRecordReader.nextKeyValue()) {
      innerValue = lineRecordReader.getCurrentValue();
      line = innerValue.getBytes();
      lineLen = innerValue.getLength();
    } else {
      return false;
    }
    if (line == null)
      return false;
    // 延迟初始化键对象
    if (key == null) {
      key = new Text();
    }
    // 延迟初始化值对象
    if (value == null) {
      value = new Text();
    }
    // 查找当前行中分隔符位置
    int pos = findSeparator(line, 0, lineLen, this.separator);
    // 将切割后的键值设置到成员变量
    setKeyValue(key, value, line, lineLen, pos);
    return true;
  }
  
  /**
   * 获取当前读取到的记录键
   * @return 当前记录的键
   */
  public Text getCurrentKey() {
    return key;
  }

  /**
   * 获取当前读取到的记录值
   * @return 当前记录的值
   */
  public Text getCurrentValue() {
    return value;
  }

  /**
   * 获取当前分片读取进度
   * @return 0.0到1.0之间的进度值
   * @throws IOException 获取进度失败时抛出IO异常
   */
  public float getProgress() throws IOException {
    return lineRecordReader.getProgress();
  }
  
  /**
   * 关闭记录读取器，关闭底层LineRecordReader释放资源
   * @throws IOException 关闭过程发生IO异常时抛出
   */
  public synchronized void close() throws IOException { 
    lineRecordReader.close();
  }
}