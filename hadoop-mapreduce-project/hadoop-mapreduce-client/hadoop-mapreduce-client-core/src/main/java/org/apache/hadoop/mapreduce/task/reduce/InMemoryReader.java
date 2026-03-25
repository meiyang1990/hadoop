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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * IFile的内存读取器，用于读取存储在内存中的Map输出数据。
 * 在Reduce阶段，当Map输出数据足够小时，会直接放在内存中合并，该类负责读取内存数据。
 * @param <K> 键类型
 * @param <V> 值类型
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryReader<K, V> extends Reader<K, V> {
  private final TaskAttemptID taskAttemptId;
  private final MergeManagerImpl<K,V> merger;
  private final DataInputBuffer memDataIn = new DataInputBuffer();
  private final int start;
  private final int length;

  /**
   * 构造内存读取器，初始化内存数据输入流。
   * @param merger 合并管理器，用于内存资源管理
   * @param taskAttemptId Map任务尝试ID，用于错误日志标识
   * @param data 存储Map输出的内存字节数组
   * @param start 数据在字节数组中的起始偏移量
   * @param length 数据总长度
   * @param conf 配置对象
   * @throws IOException 初始化失败时抛出
   */
  public InMemoryReader(MergeManagerImpl<K,V> merger, TaskAttemptID taskAttemptId,
                        byte[] data, int start, int length, Configuration conf)
  throws IOException {
    super(conf, null, length - start, null, null);
    this.merger = merger;
    this.taskAttemptId = taskAttemptId;

    buffer = data;
    bufferSize = (int)fileLength;
    memDataIn.reset(buffer, start, length - start);
    this.start = start;
    this.length = length;
  }

  @Override
  /**
   * 重置读取位置到指定偏移量
   * @param offset 新的读取偏移量
   */
  public void reset(int offset) {
    memDataIn.reset(buffer, start + offset, length - start - offset);
    bytesRead = offset;
    eof = false;
  }

  @Override
  /**
   * 获取当前已读取的字节位置
   * @return 已读取的未压缩字节数，内存数据无压缩直接返回计数
   * @throws IOException 不会抛出
   */
  public long getPosition() throws IOException {
    // InMemoryReader does not initialize streams like Reader, so in.getPos()
    // would not work. Instead, return the number of uncompressed bytes read,
    // which will be correct since in-memory data is not compressed.
    return bytesRead;
  }
  
  @Override
  /**
   * 获取数据总长度
   * @return 数据总长度
   */
  public long getLength() { 
    return fileLength;
  }
  
  /**
   * 读取错误时将损坏的内存数据导出到文件，方便问题排查
   */
  private void dumpOnError() {
    File dumpFile = new File("../output/" + taskAttemptId + ".dump");
    System.err.println("Dumping corrupt map-output of " + taskAttemptId + 
                       " to " + dumpFile.getAbsolutePath());
    try (FileOutputStream fos = new FileOutputStream(dumpFile)) {
      fos.write(buffer, 0, bufferSize);
    } catch (IOException ioe) {
      System.err.println("Failed to dump map-output of " + taskAttemptId);
    }
  }
  
  /**
   * 读取下一条记录的原始键数据
   * @param key 输出参数，存储读取到的键数据
   * @return 是否成功读取到键，false表示已到文件末尾
   * @throws IOException 读取失败时抛出，并导出损坏数据
   */
  public boolean nextRawKey(DataInputBuffer key) throws IOException {
    try {
      if (!positionToNextRecord(memDataIn)) {
        return false;
      }
      // Setup the key
      int pos = memDataIn.getPosition();
      byte[] data = memDataIn.getData();
      key.reset(data, pos, currentKeyLength);
      // Position for the next value
      long skipped = memDataIn.skip(currentKeyLength);
      if (skipped != currentKeyLength) {
        throw new IOException("Rec# " + recNo + 
            ": Failed to skip past key of length: " + 
            currentKeyLength);
      }

      // Record the byte
      bytesRead += currentKeyLength;
      return true;
    } catch (IOException ioe) {
      dumpOnError();
      throw ioe;
    }
  }
  
  /**
   * 读取当前记录的原始值数据，应在nextRawKey之后调用
   * @param value 输出参数，存储读取到的值数据
   * @throws IOException 读取失败时抛出，并导出损坏数据
   */
  public void nextRawValue(DataInputBuffer value) throws IOException {
    try {
      int pos = memDataIn.getPosition();
      byte[] data = memDataIn.getData();
      value.reset(data, pos, currentValueLength);

      // Position for the next record
      long skipped = memDataIn.skip(currentValueLength);
      if (skipped != currentValueLength) {
        throw new IOException("Rec# " + recNo + 
            ": Failed to skip past value of length: " + 
            currentValueLength);
      }
      // Record the byte
      bytesRead += currentValueLength;

      ++recNo;
    } catch (IOException ioe) {
      dumpOnError();
      throw ioe;
    }
  }
    
  /**
   * 关闭读取器，释放内存资源，通知合并管理器归还内存配额
   */
  public void close() {
    // Release
    dataIn = null;
    buffer = null;
      // Inform the MergeManager
    if (merger != null) {
      merger.unreserve(bufferSize);
    }
  }
}