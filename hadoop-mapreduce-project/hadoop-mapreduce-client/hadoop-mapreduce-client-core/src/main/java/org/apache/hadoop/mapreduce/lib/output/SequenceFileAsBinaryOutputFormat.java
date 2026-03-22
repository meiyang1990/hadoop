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
package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.io.DataOutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** 
 * 输出格式实现类，将键值对以二进制原始格式写入SequenceFile文件
 * 本类继承SequenceFileOutputFormat，支持保持原始二进制数据不变写入
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsBinaryOutputFormat 
    extends SequenceFileOutputFormat <BytesWritable,BytesWritable> {
  // 配置项：输出SequenceFile的键类型
  public static String KEY_CLASS = "mapreduce.output.seqbinaryoutputformat.key.class"; 
  // 配置项：输出SequenceFile的值类型
  public static String VALUE_CLASS = "mapreduce.output.seqbinaryoutputformat.value.class"; 

  /** 
   * 实现ValueBytes接口的内部类，用于支持原始二进制值的写入
   */
  static public class WritableValueBytes implements ValueBytes {
    private BytesWritable value;

    public WritableValueBytes() {
      this.value = null;
    }
    
    public WritableValueBytes(BytesWritable value) {
      this.value = value;
    }

    /**
     * 重置内部持有的BytesWritable引用，复用对象
     * @param value 新的二进制值
     */
    public void reset(BytesWritable value) {
      this.value = value;
    }

    @Override
    public void writeUncompressedBytes(DataOutputStream outStream)
        throws IOException {
      // 将原始字节数组直接写入输出流
      outStream.write(value.getBytes(), 0, value.getLength());
    }

    @Override
    public void writeCompressedBytes(DataOutputStream outStream)
        throws IllegalArgumentException, IOException {
      // 不支持RECORD级压缩，抛出不支持操作异常
      throw new UnsupportedOperationException(
        "WritableValueBytes doesn't support RECORD compression"); 
    }
    
    @Override
    public int getSize(){
      // 返回二进制值的字节长度
      return value.getLength();
    }
  }

  /**
   * 设置输出SequenceFile的键类型，允许指定和写入时实际使用的BytesWritable不同的类型
   * @param job 当前作业对象
   * @param theClass SequenceFile输出的键类
   */
  static public void setSequenceFileOutputKeyClass(Job job, 
      Class<?> theClass) {
    job.getConfiguration().setClass(KEY_CLASS,
      theClass, Object.class);
  }

  /**
   * 设置输出SequenceFile的值类型，允许指定和写入时实际使用的BytesWritable不同的类型
   * @param job 当前作业对象
   * @param theClass SequenceFile输出的值类
   */
  static public void setSequenceFileOutputValueClass(Job job, 
      Class<?> theClass) {
    job.getConfiguration().setClass(VALUE_CLASS, 
                  theClass, Object.class);
  }

  /**
   * 从作业配置中获取输出SequenceFile的键类型
   * @param job 作业上下文对象
   * @return 输出SequenceFile的键类
   */
  static public Class<? extends WritableComparable> 
      getSequenceFileOutputKeyClass(JobContext job) { 
    return job.getConfiguration().getClass(KEY_CLASS,
      job.getOutputKeyClass().asSubclass(WritableComparable.class), 
      WritableComparable.class);
  }

  /**
   * 从作业配置中获取输出SequenceFile的值类型
   * @param job 作业上下文对象
   * @return 输出SequenceFile的值类
   */
  static public Class<? extends Writable> getSequenceFileOutputValueClass(
      JobContext job) { 
    return job.getConfiguration().getClass(VALUE_CLASS, 
      job.getOutputValueClass().asSubclass(Writable.class), Writable.class);
  }
  
  @Override 
  public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException {
    // 创建SequenceFile写入器，使用用户指定的键值类型
    final SequenceFile.Writer out = getSequenceWriter(context,
      getSequenceFileOutputKeyClass(context),
      getSequenceFileOutputValueClass(context)); 

    // 返回自定义的RecordWriter实现
    return new RecordWriter<BytesWritable, BytesWritable>() {
      // 复用WritableValueBytes对象，避免重复创建
      private WritableValueBytes wvaluebytes = new WritableValueBytes();

      @Override
      public void write(BytesWritable bkey, BytesWritable bvalue)
        throws IOException {
        // 重置值对象引用
        wvaluebytes.reset(bvalue);
        // 以原始二进制格式追加键值对到SequenceFile
        out.appendRaw(bkey.getBytes(), 0, bkey.getLength(), wvaluebytes);
        // 清空引用帮助GC
        wvaluebytes.reset(null);
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException { 
        // 关闭底层SequenceFile写入器
        out.close();
      }
    };
  }

  @Override 
  public void checkOutputSpecs(JobContext job) throws IOException {
    // 调用父类检查输出规范
    super.checkOutputSpecs(job);
    // 本格式不支持RECORD级压缩，若配置则抛出异常
    if (getCompressOutput(job) && 
        getOutputCompressionType(job) == CompressionType.RECORD ) {
      throw new InvalidJobConfException("SequenceFileAsBinaryOutputFormat "
        + "doesn't support Record Compression" );
    }
  }
}