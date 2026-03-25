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

package org.apache.hadoop.mapreduce.lib.map;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * 文件说明: MapReduce Map阶段包装工具类，提供对已有Mapper的包装能力，支持自定义Mapper.Context实现
 * 
 * 包装类Mapper，允许用户基于已有MapContext实现自定义的Mapper.Context，方便扩展Map阶段逻辑
 * @param <KEYIN> Map输入键类型
 * @param <VALUEIN> Map输入值类型
 * @param <KEYOUT> Map输出键类型
 * @param <VALUEOUT> Map输出值类型
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
    extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  
  /**
   * 获取包装后的Mapper.Context对象，基于已有MapContext实现自定义包装
   * @param mapContext 待包装的原始MapContext对象
   * @return 包装完成的可自定义扩展的Mapper.Context对象
   */
  public Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context
  getMapContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext) {
    return new Context(mapContext);
  }
  
  /**
   * 包装后的Context实现类，代理所有MapContext接口方法，方便子类自定义扩展
   */
  @InterfaceStability.Evolving
  public class Context 
      extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

    // 被代理的原始MapContext对象
    protected MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext;

    /**
     * 构造方法，基于原始MapContext创建包装Context
     * @param mapContext 原始MapContext对象
     */
    public Context(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext) {
      this.mapContext = mapContext;
    }

    /**
     * 获取当前Map任务处理的输入分片
     * @return 当前输入分片对象
     */
    public InputSplit getInputSplit() {
      return mapContext.getInputSplit();
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return mapContext.getCurrentKey();
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return mapContext.getCurrentValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return mapContext.nextKeyValue();
    }

    @Override
    public Counter getCounter(Enum<?> counterName) {
      return mapContext.getCounter(counterName);
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
      return mapContext.getCounter(groupName, counterName);
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      return mapContext.getOutputCommitter();
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      mapContext.write(key, value);
    }

    @Override
    public String getStatus() {
      return mapContext.getStatus();
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
      return mapContext.getTaskAttemptID();
    }

    @Override
    public void setStatus(String msg) {
      mapContext.setStatus(msg);
    }

    @Override
    public Path[] getArchiveClassPaths() {
      return mapContext.getArchiveClassPaths();
    }

    @Override
    public String[] getArchiveTimestamps() {
      return mapContext.getArchiveTimestamps();
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
      return mapContext.getCacheArchives();
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
      return mapContext.getCacheFiles();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException {
      return mapContext.getCombinerClass();
    }

    @Override
    public Configuration getConfiguration() {
      return mapContext.getConfiguration();
    }

    @Override
    public Path[] getFileClassPaths() {
      return mapContext.getFileClassPaths();
    }

    @Override
    public String[] getFileTimestamps() {
      return mapContext.getFileTimestamps();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
      return mapContext.getCombinerKeyGroupingComparator();
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
      return mapContext.getGroupingComparator();
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException {
      return mapContext.getInputFormatClass();
    }

    @Override
    public String getJar() {
      return mapContext.getJar();
    }

    @Override
    public JobID getJobID() {
      return mapContext.getJobID();
    }

    @Override
    public String getJobName() {
      return mapContext.getJobName();
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
      return mapContext.getJobSetupCleanupNeeded();
    }

    @Override
    public boolean getTaskCleanupNeeded() {
      return mapContext.getTaskCleanupNeeded();
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
      return mapContext.getLocalCacheArchives();
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
      return mapContext.getLocalCacheFiles();
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
      return mapContext.getMapOutputKeyClass();
    }

    @Override
    public Class<?> getMapOutputValueClass() {
      return mapContext.getMapOutputValueClass();
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException {
      return mapContext.getMapperClass();
    }

    @Override
    public int getMaxMapAttempts() {
      return mapContext.getMaxMapAttempts();
    }

    @Override
    public int getMaxReduceAttempts() {
      return mapContext.getMaxReduceAttempts();
    }

    @Override
    public int getNumReduceTasks() {
      return mapContext.getNumReduceTasks();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException {
      return mapContext.getOutputFormatClass();
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return mapContext.getOutputKeyClass();
    }

    @Override
    public Class<?> getOutputValueClass() {
      return mapContext.getOutputValueClass();
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException {
      return mapContext.getPartitionerClass();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException {
      return mapContext.getReducerClass();
    }

    @Override
    public RawComparator<?> getSortComparator() {
      return mapContext.getSortComparator();
    }

    @Override
    public boolean getSymlink() {
      return mapContext.getSymlink();
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
      return mapContext.getWorkingDirectory();
    }

    @Override
    public void progress() {
      mapContext.progress();
    }

    @Override
    public boolean getProfileEnabled() {
      return mapContext.getProfileEnabled();
    }

    @Override
    public String getProfileParams() {
      return mapContext.getProfileParams();
    }

    @Override
    public IntegerRanges getProfileTaskRange(boolean isMap) {
      return mapContext.getProfileTaskRange(isMap);
    }

    @Override
    public String getUser() {
      return mapContext.getUser();
    }

    @Override
    public Credentials getCredentials() {
      return mapContext.getCredentials();
    }
    
    @Override
    public float getProgress() {
      return mapContext.getProgress();
    }
  }
}