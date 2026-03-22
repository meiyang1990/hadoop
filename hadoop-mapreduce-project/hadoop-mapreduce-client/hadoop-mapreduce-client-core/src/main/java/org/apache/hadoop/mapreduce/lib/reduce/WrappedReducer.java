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

package org.apache.hadoop.mapreduce.lib.reduce;

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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * WrappedReducer 是对已有 Reducer 的包装类，支持用户自定义 Reducer.Context 实现，
 * 通过代理模式将所有方法转发给原始 ReduceContext，方便扩展上下文功能而无需重写所有方法。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
    extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * 根据给定的 ReduceContext 创建并返回包装后的 Reducer.Context 实例，
   * 用于支持自定义上下文功能扩展。
   * @param reduceContext 原始的 ReduceContext 实例
   * @return 包装后的 Reducer.Context 实例
   */
  public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context 
  getReducerContext(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext) {
    return new Context(reduceContext);
  }
  
  /**
   * 包装 ReduceContext 的内部 Context 类，继承自 Reducer.Context，
   * 将所有接口方法代理转发给持有的原始 ReduceContext 实例，简化自定义扩展。
   */
  @InterfaceStability.Evolving
  public class Context 
      extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

    // 被包装的原始 ReduceContext 实例
    protected ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext;

    /**
     * 构造函数，传入需要包装的原始 ReduceContext 实例。
     * @param reduceContext 原始 ReduceContext 实例
     */
    public Context(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext)
    {
      this.reduceContext = reduceContext; 
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return reduceContext.getCurrentKey();
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return reduceContext.getCurrentValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return reduceContext.nextKeyValue();
    }

    @Override
    public Counter getCounter(Enum counterName) {
      return reduceContext.getCounter(counterName);
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
      return reduceContext.getCounter(groupName, counterName);
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      return reduceContext.getOutputCommitter();
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      reduceContext.write(key, value);
    }

    @Override
    public String getStatus() {
      return reduceContext.getStatus();
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
      return reduceContext.getTaskAttemptID();
    }

    @Override
    public void setStatus(String msg) {
      reduceContext.setStatus(msg);
    }

    @Override
    public Path[] getArchiveClassPaths() {
      return reduceContext.getArchiveClassPaths();
    }

    @Override
    public String[] getArchiveTimestamps() {
      return reduceContext.getArchiveTimestamps();
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
      return reduceContext.getCacheArchives();
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
      return reduceContext.getCacheFiles();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException {
      return reduceContext.getCombinerClass();
    }

    @Override
    public Configuration getConfiguration() {
      return reduceContext.getConfiguration();
    }

    @Override
    public Path[] getFileClassPaths() {
      return reduceContext.getFileClassPaths();
    }

    @Override
    public String[] getFileTimestamps() {
      return reduceContext.getFileTimestamps();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
      return reduceContext.getCombinerKeyGroupingComparator();
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
      return reduceContext.getGroupingComparator();
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException {
      return reduceContext.getInputFormatClass();
    }

    @Override
    public String getJar() {
      return reduceContext.getJar();
    }

    @Override
    public JobID getJobID() {
      return reduceContext.getJobID();
    }

    @Override
    public String getJobName() {
      return reduceContext.getJobName();
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
      return reduceContext.getJobSetupCleanupNeeded();
    }

    @Override
    public boolean getTaskCleanupNeeded() {
      return reduceContext.getTaskCleanupNeeded();
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
      return reduceContext.getLocalCacheArchives();
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
      return reduceContext.getLocalCacheFiles();
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
      return reduceContext.getMapOutputKeyClass();
    }

    @Override
    public Class<?> getMapOutputValueClass() {
      return reduceContext.getMapOutputValueClass();
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException {
      return reduceContext.getMapperClass();
    }

    @Override
    public int getMaxMapAttempts() {
      return reduceContext.getMaxMapAttempts();
    }

    @Override
    public int getMaxReduceAttempts() {
      return reduceContext.getMaxReduceAttempts();
    }

    @Override
    public int getNumReduceTasks() {
      return reduceContext.getNumReduceTasks();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException {
      return reduceContext.getOutputFormatClass();
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return reduceContext.getOutputKeyClass();
    }

    @Override
    public Class<?> getOutputValueClass() {
      return reduceContext.getOutputValueClass();
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException {
      return reduceContext.getPartitionerClass();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException {
      return reduceContext.getReducerClass();
    }

    @Override
    public RawComparator<?> getSortComparator() {
      return reduceContext.getSortComparator();
    }

    @Override
    public boolean getSymlink() {
      return reduceContext.getSymlink();
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
      return reduceContext.getWorkingDirectory();
    }

    @Override
    public void progress() {
      reduceContext.progress();
    }

    @Override
    public Iterable<VALUEIN> getValues() throws IOException,
        InterruptedException {
      return reduceContext.getValues();
    }

    @Override
    public boolean nextKey() throws IOException, InterruptedException {
      return reduceContext.nextKey();
    }
    
    @Override
    public boolean getProfileEnabled() {
      return reduceContext.getProfileEnabled();
    }

    @Override
    public String getProfileParams() {
      return reduceContext.getProfileParams();
    }

    @Override
    public IntegerRanges getProfileTaskRange(boolean isMap) {
      return reduceContext.getProfileTaskRange(isMap);
    }

    @Override
    public String getUser() {
      return reduceContext.getUser();
    }

    @Override
    public Credentials getCredentials() {
      return reduceContext.getCredentials();
    }
    
    @Override
    public float getProgress() {
      return reduceContext.getProgress();
    }
  }
}