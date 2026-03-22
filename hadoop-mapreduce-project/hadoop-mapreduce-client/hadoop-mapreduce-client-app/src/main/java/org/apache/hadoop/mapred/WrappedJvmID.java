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

/**
 * WrappedJvmID 类是 JVMId 的包装类，用于提升 JVMId 的访问可见性。
 * 在 MapReduce 任务执行过程中，用于跨包访问 JVMId 标识信息，
 * 标识一个MapReduce任务的JVM进程ID。
 */
public class WrappedJvmID extends JVMId {

  /**
   * 构造 WrappedJvmID 实例，将参数透传给父类 JVMId 构造函数
   * @param jobID 所属作业ID
   * @param mapTask 是否是Map任务
   * @param nextLong JVM的唯一编号
   */
  public WrappedJvmID(JobID jobID, boolean mapTask, long nextLong) {
    super(jobID, mapTask, nextLong);
  }

}