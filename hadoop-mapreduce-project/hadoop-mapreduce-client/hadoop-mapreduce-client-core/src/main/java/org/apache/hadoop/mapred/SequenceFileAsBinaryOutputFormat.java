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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/** 
 * 文件级注释：SequenceFile二进制输出格式实现，将原始二进制格式的键值对写入SequenceFile文件
 * 兼容旧版MapRed API，用于输出二进制格式的SequenceFile结果文件
 * An {@link OutputFormat} that writes keys, values to 
 * {@link SequenceFile}s in binary(raw) format
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileAsBinaryOutputFormat 
 extends SequenceFileOutputFormat <BytesWritable,BytesWritable> {

  /** 
   * 内部工具类，用于支持原始二进制数据追加，封装值字节适配逻辑
   * Inner class used for appendRaw
   */
  static protected class WritableValueBytes extends org.apache.hadoop.mapreduce
      .lib.output.SequenceFileAsBinaryOutputFormat.WritableValueBytes {
    public WritableValueBytes() {
      super();
    }

    public WritableValueBytes(BytesWritable value) {
      super(value);
    }
  }

  /**
   * 设置输出SequenceFile的键类型，允许指定与实际写入类(BytesWritable)不同的元数据类型
   * @param conf 作业配置对象
   * @param theClass SequenceFile输出的键类
   */
  static public void setSequenceFileOutputKeyClass(JobConf conf, 
                                                   Class<?> theClass) {
    conf.setClass(org.apache.hadoop.mapreduce.lib.output.
      SequenceFileAsBinaryOutputFormat.KEY_CLASS, theClass, Object.class);
  }

  /**
   * 设置输出SequenceFile的值类型，允许指定与实际写入类(BytesWritable)不同的元数据类型
   * @param conf 作业配置对象
   * @param theClass SequenceFile输出的值类
   */
  static public void setSequenceFileOutputValueClass(JobConf conf, 
                                                     Class<?> theClass) {
    conf.setClass(org.apache.hadoop.mapreduce.lib.output.
      SequenceFileAsBinaryOutputFormat.VALUE_CLASS, theClass, Object.class);
  }

  /**
   * 获取输出SequenceFile配置的键类型，默认回退到作业输出键类
   * @param conf 作业配置对象
   * @return SequenceFile输出的键类
   */
  static public Class<? extends WritableComparable> getSequenceFileOutputKeyClass(JobConf conf) { 
    return conf.getClass(org.apache.hadoop.mapreduce.lib.output.
      SequenceFileAsBinaryOutputFormat.KEY_CLASS, 
      conf.getOutputKeyClass().asSubclass(WritableComparable.class),
      WritableComparable.class);
  }

  /**
   * 获取输出SequenceFile配置的值类型，默认回退到作业输出值类
   * @param conf 作业配置对象
   * @return SequenceFile输出的值类
   */
  static public Class<? extends Writable> getSequenceFileOutputValueClass(JobConf conf) { 
    return conf.getClass(org.apache.hadoop.mapreduce.lib.output.
      SequenceFileAsBinaryOutputFormat.VALUE_CLASS, 
      conf.getOutputValueClass().asSubclass(Writable.class), Writable.class);
  }
  
  @Override 
  public RecordWriter <BytesWritable, BytesWritable> 
             getRecordWriter(FileSystem ignored, JobConf job,
                             String name, Progressable progress)
    throws IOException {
    // 获取任务输出文件的路径
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    
    FileSystem fs = file.getFileSystem(job);
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    // 检查是否开启输出压缩
    if (getCompressOutput(job)) {
      // 获取压缩类型
      compressionType = getOutputCompressionType(job);

      // 获取压缩编码类
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,
	  DefaultCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
    }
    // 创建SequenceFile写入器
    final SequenceFile.Writer out = 
      SequenceFile.createWriter(fs, job, file,
                    getSequenceFileOutputKeyClass(job),
                    getSequenceFileOutputValueClass(job),
                    compressionType,
                    codec,
                    progress);

    // 返回RecordWriter实现，处理二进制键值对写入
    return new RecordWriter<BytesWritable, BytesWritable>() {
        
        private WritableValueBytes wvaluebytes = new WritableValueBytes();

        public void write(BytesWritable bkey, BytesWritable bvalue)
          throws IOException {
          // 重置值字节包装器
          wvaluebytes.reset(bvalue);
          // 以原始二进制格式追加键值对
          out.appendRaw(bkey.getBytes(), 0, bkey.getLength(), wvaluebytes);
          // 清空包装器引用避免内存泄漏
          wvaluebytes.reset(null);
        }

        public void close(Reporter reporter) throws IOException { 
          out.close();
        }

      };

  }

  @Override 
  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
            throws IOException {
    super.checkOutputSpecs(ignored, job);
    // 检查不支持的记录压缩类型
    if (getCompressOutput(job) && 
        getOutputCompressionType(job) == CompressionType.RECORD ){
        throw new InvalidJobConfException("SequenceFileAsBinaryOutputFormat "
                    + "doesn't support Record Compression" );
    }

  }

}