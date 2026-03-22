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

package org.apache.hadoop.mapreduce.v2.util;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobStateProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.PhaseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventStatusProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptStateProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskStateProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskTypeProto;

/**
 * MapReduce Protobuf序列化工具类，负责API层枚举类型与ProtoBuf定义的枚举类型之间的互相转换
 * 为RPC通信提供统一的类型转换能力，隔离API层与Protobuf层的枚举命名空间
 */
public class MRProtoUtils {

  /*
   * JobState
   */
  private static String JOB_STATE_PREFIX = "J_";
  
  /**
   * 将API层的作业状态枚举转换为Protobuf格式枚举
   * @param e API层作业状态枚举
   * @return Protobuf格式作业状态枚举
   */
  public static JobStateProto convertToProtoFormat(JobState e) {
    return JobStateProto.valueOf(JOB_STATE_PREFIX + e.name());
  }
  
  /**
   * 将Protobuf格式作业状态枚举转换为API层格式枚举
   * @param e Protobuf格式作业状态枚举
   * @return API层作业状态枚举
   */
  public static JobState convertFromProtoFormat(JobStateProto e) {
    return JobState.valueOf(e.name().replace(JOB_STATE_PREFIX, ""));
  }
  
  /*
   * Phase
   */
  private static String PHASE_PREFIX = "P_";
  
  /**
   * 将API层的作业阶段枚举转换为Protobuf格式枚举
   * @param e API层作业阶段枚举
   * @return Protobuf格式作业阶段枚举
   */
  public static PhaseProto convertToProtoFormat(Phase e) {
    return PhaseProto.valueOf(PHASE_PREFIX + e.name());
  }
  
  /**
   * 将Protobuf格式作业阶段枚举转换为API层格式枚举
   * @param e Protobuf格式作业阶段枚举
   * @return API层作业阶段枚举
   */
  public static Phase convertFromProtoFormat(PhaseProto e) {
    return Phase.valueOf(e.name().replace(PHASE_PREFIX, ""));
  }
  
  /*
   * TaskAttemptCompletionEventStatus
   */
  private static String TACE_PREFIX = "TACE_";
  
  /**
   * 将API层的任务尝试完成事件状态枚举转换为Protobuf格式枚举
   * @param e API层任务尝试完成事件状态枚举
   * @return Protobuf格式任务尝试完成事件状态枚举
   */
  public static TaskAttemptCompletionEventStatusProto convertToProtoFormat(TaskAttemptCompletionEventStatus e) {
    return TaskAttemptCompletionEventStatusProto.valueOf(TACE_PREFIX + e.name());
  }
  
  /**
   * 将Protobuf格式任务尝试完成事件状态枚举转换为API层格式枚举
   * @param e Protobuf格式任务尝试完成事件状态枚举
   * @return API层任务尝试完成事件状态枚举
   */
  public static TaskAttemptCompletionEventStatus convertFromProtoFormat(TaskAttemptCompletionEventStatusProto e) {
    return TaskAttemptCompletionEventStatus.valueOf(e.name().replace(TACE_PREFIX, ""));
  }
  
  /*
   * TaskAttemptState
   */
  private static String TASK_ATTEMPT_STATE_PREFIX = "TA_";
  
  /**
   * 将API层的任务尝试状态枚举转换为Protobuf格式枚举
   * @param e API层任务尝试状态枚举
   * @return Protobuf格式任务尝试状态枚举
   */
  public static TaskAttemptStateProto convertToProtoFormat(TaskAttemptState e) {
    return TaskAttemptStateProto.valueOf(TASK_ATTEMPT_STATE_PREFIX + e.name());
  }
  
  /**
   * 将Protobuf格式任务尝试状态枚举转换为API层格式枚举
   * @param e Protobuf格式任务尝试状态枚举
   * @return API层任务尝试状态枚举
   */
  public static TaskAttemptState convertFromProtoFormat(TaskAttemptStateProto e) {
    return TaskAttemptState.valueOf(e.name().replace(TASK_ATTEMPT_STATE_PREFIX, ""));
  }
  
  /*
   * TaskState
   */
  private static String TASK_STATE_PREFIX = "TS_";
  
  /**
   * 将API层的任务状态枚举转换为Protobuf格式枚举
   * @param e API层任务状态枚举
   * @return Protobuf格式任务状态枚举
   */
  public static TaskStateProto convertToProtoFormat(TaskState e) {
    return TaskStateProto.valueOf(TASK_STATE_PREFIX + e.name());
  }
  
  /**
   * 将Protobuf格式任务状态枚举转换为API层格式枚举
   * @param e Protobuf格式任务状态枚举
   * @return API层任务状态枚举
   */
  public static TaskState convertFromProtoFormat(TaskStateProto e) {
    return TaskState.valueOf(e.name().replace(TASK_STATE_PREFIX, ""));
  }
  
  /*
   * TaskType
   */
  
  /**
   * 将API层的任务类型枚举转换为Protobuf格式枚举
   * @param e API层任务类型枚举
   * @return Protobuf格式任务类型枚举
   */
  public static TaskTypeProto convertToProtoFormat(TaskType e) {
    return TaskTypeProto.valueOf(e.name());
  }
  
  /**
   * 将Protobuf格式任务类型枚举转换为API层格式枚举
   * @param e Protobuf格式任务类型枚举
   * @return API层任务类型枚举
   */
  public static TaskType convertFromProtoFormat(TaskTypeProto e) {
    return TaskType.valueOf(e.name());
  }
}