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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 文件说明：SequenceFile二进制输入格式实现，以原始二进制字节形式读取SequenceFile中的键值对
 * 用于MapReduce任务处理SequenceFile时，保留原始二进制数据不进行反序列化
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsBinaryInputFormat
    extends SequenceFileInputFormat<BytesWritable,BytesWritable> {

  /**
   * 构造函数，初始化二进制SequenceFile输入格式
   */
  public SequenceFileAsBinaryInputFormat() {
    super();
  }

  /**
   * 创建二进制SequenceFile记录读取器，用于读取当前输入分片的原始二进制记录
   * @param split 输入分片
   * @param context MapReduce任务上下文
   * @return 二进制记录读取器实例
   * @throws IOException 如果创建过程中发生IO异常
   */
  public RecordReader<BytesWritable,BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new SequenceFileAsBinaryRecordReader();
  }

  /**
   * 类说明：从SequenceFile中读取原始二进制键值对的记录读取器
   * 不对键值进行反序列化，直接以字节数组形式返回原始存储内容
   */
  public static class SequenceFileAsBinaryRecordReader
      extends RecordReader<BytesWritable,BytesWritable> {
    private SequenceFile.Reader in;
    private long start;
    private long end;
    private boolean done = false;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private SequenceFile.ValueBytes vbytes;
    private BytesWritable key = null;
    private BytesWritable value = null;

    /**
     * 初始化记录读取器，打开SequenceFile读取器并定位到分片起始位置
     * @param split 输入分片
     * @param context 任务上下文
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public void initialize(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException {
      // 获取分片对应的文件路径
      Path path = ((FileSplit)split).getPath();
      Configuration conf = context.getConfiguration();
      FileSystem fs = path.getFileSystem(conf);
      // 创建SequenceFile读取器
      this.in = new SequenceFile.Reader(fs, path, conf);
      // 计算分片结束位置
      this.end = ((FileSplit)split).getStart() + split.getLength();
      // 如果当前分片起始不在块边界，同步到最近的同步标记位置
      if (((FileSplit)split).getStart() > in.getPosition()) {
        in.sync(((FileSplit)split).getStart());
      }
      // 记录实际起始位置
      this.start = in.getPosition();
      // 创建Value字节容器
      vbytes = in.createValueBytes();
      // 判断是否已经读取完成
      done = start >= end;
    }
    
    @Override
    public BytesWritable getCurrentKey() 
        throws IOException, InterruptedException {
      return key;
    }
    
    @Override
    public BytesWritable getCurrentValue() 
        throws IOException, InterruptedException {
      return value;
    }

    /**
     * 获取当前SequenceFile的键类名
     * @return 键类的全限定名
     */
    public String getKeyClassName() {
      return in.getKeyClassName();
    }

    /**
     * 获取当前SequenceFile的值类名
     * @return 值类的全限定名
     */
    public String getValueClassName() {
      return in.getValueClassName();
    }

    /**
     * 读取下一个原始二进制键值对
     * @return 是否成功读取到下一个键值对
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public synchronized boolean nextKeyValue()
        throws IOException, InterruptedException {
      if (done) {
        return false;
      }
      // 记录当前读取位置
      long pos = in.getPosition();
      // 读取原始键二进制数据
      boolean eof = -1 == in.nextRawKey(buffer);
      if (!eof) {
        // 延迟初始化键值对象
        if (key == null) {
          key = new BytesWritable();
        }
        if (value == null) {
          value = new BytesWritable();
        }
        // 将键数据写入BytesWritable
        key.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
        // 读取原始值二进制数据
        in.nextRawValue(vbytes);
        vbytes.writeUncompressedBytes(buffer);
        // 将值数据写入BytesWritable
        value.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
      }
      // 判断是否已经读取完成，更新完成状态并返回结果
      return !(done = (eof || (pos >= end && in.syncSeen())));
    }

    /**
     * 关闭读取器，释放资源
     * @throws IOException IO异常
     */
    public void close() throws IOException {
      in.close();
    }

    /**
     * 获取当前读取进度，范围从0.0到1.0
     * @return 读取进度百分比
     * @throws IOException IO异常
     * @throws InterruptedException 中断异常
     */
    public float getProgress() throws IOException, InterruptedException {
      if (end == start) {
        return 0.0f;
      } else {
        // 根据已读取字节数计算进度，限制最大值为1.0
        return Math.min(1.0f, (float)((in.getPosition() - start) /
                                      (double)(end - start)));
      }
    }
  }
}