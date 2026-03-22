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
package org.apache.hadoop.mapred.pipes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Pipes非Java输入格式，供Pipes应用使用非Java RecordReader时使用
 * 
 * 该类仅作为占位实现，负责提供占位用的PipesDummyRecordReader，输入分片生成功能
 * 委托给用户通过mapreduce.pipes.inputformat配置指定的真实输入格式类完成
 */
class PipesNonJavaInputFormat 
implements InputFormat<FloatWritable, NullWritable> {

  /**
   * 获取记录读取器，返回占位用的虚拟记录读取器
   * @param genericSplit 输入分片
   * @param job 作业配置
   * @param reporter 进度报告器
   * @return 虚拟记录读取器实例
   * @throws IOException IO异常
   */
  public RecordReader<FloatWritable, NullWritable> getRecordReader(
      InputSplit genericSplit, JobConf job, Reporter reporter)
      throws IOException {
    return new PipesDummyRecordReader(job, genericSplit);
  }
  
  /**
   * 生成输入分片，委托给用户配置的真实输入格式类完成分片生成
   * @param job 作业配置
   * @param numSplits 期望分片数量
   * @return 生成的输入分片数组
   * @throws IOException IO异常
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // 委托给用户配置的原始输入格式生成分片
    return ReflectionUtils.newInstance(
        job.getClass(Submitter.INPUT_FORMAT, 
                     TextInputFormat.class, 
                     InputFormat.class), job).getSplits(job, numSplits);
  }

  /**
   * 虚拟记录读取器，用于Pipes应用使用非Java RecordReader时跟踪任务进度
   * 
   * 非Java端通过OutputHandler#progress上报进度，进度值作为key传入next方法，
   * 本类保存进度值供框架查询，从而实现非Java任务进度对Hadoop框架的可见性
   */
  static class PipesDummyRecordReader implements RecordReader<FloatWritable, NullWritable> {
    // 当前任务进度，范围[0.0, 1.0]
    float progress = 0.0f;
    
    public PipesDummyRecordReader(Configuration job, InputSplit split)
    throws IOException{
    }

    
    public FloatWritable createKey() {
      return null;
    }

    public NullWritable createValue() {
      return null;
    }

    public synchronized void close() throws IOException {}

    public synchronized long getPos() throws IOException {
      return 0;
    }

    /**
     * 获取当前任务进度
     * @return 当前进度值[0.0, 1.0]
     */
    public float getProgress() {
      return progress;
    }

    /**
     * 接收非Java端上报的进度，更新进度值
     * @param key 封装了进度值的FloatWritable对象
     * @param value 占位值，未使用
     * @return 始终返回true，表示还有进度可接收
     * @throws IOException IO异常
     */
    public synchronized boolean next(FloatWritable key, NullWritable value)
        throws IOException {
      progress = key.get();
      return true;
    }
  }
}