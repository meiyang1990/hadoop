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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;

/**
 * 文件级注释：SequenceFile二进制输入格式，用于MapReduce任务读取SequenceFile中的原始二进制键值对
 * 输入格式实现类，以原始二进制格式读取SequenceFile中的键和值，不对数据进行反序列化解析
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsBinaryInputFormat
    extends SequenceFileInputFormat<BytesWritable,BytesWritable> {

  /**
   * 构造函数，初始化二进制SequenceFile输入格式对象
   */
  public SequenceFileAsBinaryInputFormat() {
    super();
  }

  /**
   * 获取指定输入分片的记录读取器，用于从分片中读取二进制键值对
   * @param split 输入分片
   * @param job 作业配置
   * @param reporter 进度汇报器
   * @return 二进制格式SequenceFile记录读取器
   * @throws IOException IO异常
   */
  public RecordReader<BytesWritable,BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return new SequenceFileAsBinaryRecordReader(job, (FileSplit)split);
  }

  /**
   * 二进制格式SequenceFile记录读取器，以原始二进制字节形式读取SequenceFile中的记录
   */
  public static class SequenceFileAsBinaryRecordReader
      implements RecordReader<BytesWritable,BytesWritable> {
    private SequenceFile.Reader in;
    private long start;
    private long end;
    private boolean done = false;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private SequenceFile.ValueBytes vbytes;

    /**
     * 构造二进制记录读取器，打开SequenceFile并定位到分片起始位置
     * @param conf 作业配置
     * @param split 文件分片
     * @throws IOException IO异常
     */
    public SequenceFileAsBinaryRecordReader(Configuration conf, FileSplit split)
        throws IOException {
      Path path = split.getPath();
      FileSystem fs = path.getFileSystem(conf);
      this.in = new SequenceFile.Reader(fs, path, conf);
      this.end = split.getStart() + split.getLength();
      // 如果当前偏移大于分片起始位置，同步对齐到分片起始边界
      if (split.getStart() > in.getPosition())
        in.sync(split.getStart());
      this.start = in.getPosition();
      vbytes = in.createValueBytes();
      done = start >= end;
    }

    /**
     * 创建键对象，用于存放读取到的二进制键数据
     * @return 新建的BytesWritable对象
     */
    public BytesWritable createKey() {
      return new BytesWritable();
    }

    /**
     * 创建值对象，用于存放读取到的二进制值数据
     * @return 新建的BytesWritable对象
     */
    public BytesWritable createValue() {
      return new BytesWritable();
    }

    /**
     * 获取当前SequenceFile中键类的全类名
     * @return 键类名
     */
    public String getKeyClassName() {
      return in.getKeyClassName();
    }

    /**
     * 获取当前SequenceFile中值类的全类名
     * @return 值类名
     */
    public String getValueClassName() {
      return in.getValueClassName();
    }

    /**
     * 读取下一条原始二进制记录，填充到提供的键值对象中
     * @param key 用于存放二进制键的对象
     * @param val 用于存放二进制值的对象
     * @return 是否成功读取到下一条记录，false表示读取完成
     * @throws IOException IO异常
     */
    public synchronized boolean next(BytesWritable key, BytesWritable val)
        throws IOException {
      if (done) return false;
      long pos = in.getPosition();
      // 读取原始键二进制数据
      boolean eof = -1 == in.nextRawKey(buffer);
      if (!eof) {
        // 将读取到的键数据设置到输出键对象中
        key.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
        // 读取原始值二进制数据
        in.nextRawValue(vbytes);
        vbytes.writeUncompressedBytes(buffer);
        val.set(buffer.getData(), 0, buffer.getLength());
        buffer.reset();
      }
      // 判断是否读取完成：到达文件尾/已超过分片结束且已完成同步对齐
      return !(done = (eof || (pos >= end && in.syncSeen())));
    }

    /**
     * 获取当前读取位置的字节偏移
     * @return 当前字节偏移
     * @throws IOException IO异常
     */
    public long getPos() throws IOException {
      return in.getPosition();
    }

    /**
     * 关闭读取器，释放打开的SequenceFile资源
     * @throws IOException IO异常
     */
    public void close() throws IOException {
      in.close();
    }

    /**
     * 获取当前分片的读取进度，范围0.0到1.0
     * @return 读取进度百分比
     * @throws IOException IO异常
     */
    public float getProgress() throws IOException {
      if (end == start) {
        return 0.0f;
      } else {
        // 计算已读取字节数占总分片字节数的比例
        return Math.min(1.0f, (float)((in.getPosition() - start) /
                                      (double)(end - start)));
      }
    }
  }
}