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

package org.apache.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobReport;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

/**
 * 不同版本MapReduce/YARN数据类型转换器工具类
 * 负责在旧版mapred API对象和新版mapreduce.v2 API对象之间进行类型转换
 * 支撑新旧API兼容以及客户端和YARN服务端之间的数据交互
 */
public class TypeConverter {

  private static RecordFactory recordFactory;

  static {
    recordFactory = RecordFactoryProvider.getRecordFactory(null);
  }

  /**
   * 将YARN版JobId转换为旧版mapred JobID
   * @param id YARN API的JobId对象
   * @return 旧版mapred API的JobID对象
   */
  public static org.apache.hadoop.mapred.JobID fromYarn(JobId id) {
    String identifier = fromClusterTimeStamp(id.getAppId().getClusterTimestamp());
    return new org.apache.hadoop.mapred.JobID(identifier, id.getId());
  }

  //currently there is 1-1 mapping between appid and jobid
  /**
   * 将YARN ApplicationId转换为新版mapreduce JobID
   * @param appID YARN API的ApplicationId对象
   * @return 新版mapreduce API的JobID对象
   */
  public static org.apache.hadoop.mapreduce.JobID fromYarn(ApplicationId appID) {
    String identifier = fromClusterTimeStamp(appID.getClusterTimestamp());
    return new org.apache.hadoop.mapred.JobID(identifier, appID.getId());
  }

  /**
   * 将新版mapreduce JobID转换为YARN版JobId
   * @param id 新版mapreduce API的JobID对象
   * @return YARN API的JobId对象
   */
  public static JobId toYarn(org.apache.hadoop.mapreduce.JobID id) {
    JobId jobId = recordFactory.newRecordInstance(JobId.class);
    jobId.setId(id.getId()); //currently there is 1-1 mapping between appid and jobid

    ApplicationId appId = ApplicationId.newInstance(
        toClusterTimeStamp(id.getJtIdentifier()), id.getId());
    jobId.setAppId(appId);
    return jobId;
  }

  /**
   * 将作业优先级字符串转换为YARN应用优先级整数值
   * @param priority 旧版作业优先级字符串
   * @return 对应的YARN优先级整数值
   */
  public static int toYarnApplicationPriority(String priority) {
    JobPriority jobPriority = JobPriority.valueOf(priority);
    switch (jobPriority) {
    case VERY_HIGH :
      return 5;
    case HIGH :
      return 4;
    case NORMAL :
      return 3;
    case LOW :
      return 2;
    case VERY_LOW :
      return 1;
    case DEFAULT :
      return 0;
    }
    throw new IllegalArgumentException("Unrecognized priority: " + priority);
  }

  /**
   * 将集群时间戳转换为Job标识字符串
   * @param clusterTimeStamp 集群时间戳
   * @return 转换后的字符串标识
   */
  private static String fromClusterTimeStamp(long clusterTimeStamp) {
    return Long.toString(clusterTimeStamp);
  }

  /**
   * 将Job标识字符串转换回集群时间戳
   * @param identifier Job标识字符串
   * @return 转换后的集群时间戳
   */
  private static long toClusterTimeStamp(String identifier) {
    return Long.parseLong(identifier);
  }

  /**
   * 将YARN版任务类型转换为新版mapreduce任务类型
   * @param taskType YARN API的TaskType对象
   * @return 新版mapreduce API的TaskType对象
   */
  public static org.apache.hadoop.mapreduce.TaskType fromYarn(
      TaskType taskType) {
    switch (taskType) {
    case MAP:
      return org.apache.hadoop.mapreduce.TaskType.MAP;
    case REDUCE:
      return org.apache.hadoop.mapreduce.TaskType.REDUCE;
    default:
      throw new YarnRuntimeException("Unrecognized task type: " + taskType);
    }
  }

  /**
   * 将新版mapreduce任务类型转换为YARN版任务类型
   * @param taskType 新版mapreduce API的TaskType对象
   * @return YARN API的TaskType对象
   */
  public static TaskType
      toYarn(org.apache.hadoop.mapreduce.TaskType taskType) {
    switch (taskType) {
    case MAP:
      return TaskType.MAP;
    case REDUCE:
      return TaskType.REDUCE;
    default:
      throw new YarnRuntimeException("Unrecognized task type: " + taskType);
    }
  }

  /**
   * 将YARN版TaskId转换为旧版mapred TaskID
   * @param id YARN API的TaskId对象
   * @return 旧版mapred API的TaskID对象
   */
  public static org.apache.hadoop.mapred.TaskID fromYarn(TaskId id) {
    return new org.apache.hadoop.mapred.TaskID(fromYarn(id.getJobId()),
      fromYarn(id.getTaskType()), id.getId());
  }

  /**
   * 将新版mapreduce TaskID转换为YARN版TaskId
   * @param id 新版mapreduce API的TaskID对象
   * @return YARN API的TaskId对象
   */
  public static TaskId toYarn(org.apache.hadoop.mapreduce.TaskID id) {
    TaskId taskId = recordFactory.newRecordInstance(TaskId.class);
    taskId.setId(id.getId());
    taskId.setTaskType(toYarn(id.getTaskType()));
    taskId.setJobId(toYarn(id.getJobID()));
    return taskId;
  }

  /**
   * 将旧版mapred任务状态转换为YARN版任务尝试状态
   * @param state 旧版mapred API的TaskStatus.State对象
   * @return YARN API的TaskAttemptState对象
   */
  public static TaskAttemptState toYarn(
      org.apache.hadoop.mapred.TaskStatus.State state) {
    switch (state) {
    case COMMIT_PENDING:
      return TaskAttemptState.COMMIT_PENDING;
    case FAILED:
    case FAILED_UNCLEAN:
      return TaskAttemptState.FAILED;
    case KILLED:
    case KILLED_UNCLEAN:
      return TaskAttemptState.KILLED;
    case RUNNING:
      return TaskAttemptState.RUNNING;
    case SUCCEEDED:
      return TaskAttemptState.SUCCEEDED;
    case UNASSIGNED:
      return TaskAttemptState.STARTING;
    default:
      throw new YarnRuntimeException("Unrecognized State: " + state);
    }
  }

  /**
   * 将旧版mapred任务阶段转换为YARN版阶段
   * @param phase 旧版mapred API的TaskStatus.Phase对象
   * @return YARN API的Phase对象
   */
  public static Phase toYarn(org.apache.hadoop.mapred.TaskStatus.Phase phase) {
    switch (phase) {
    case STARTING:
      return Phase.STARTING;
    case MAP:
      return Phase.MAP;
    case SHUFFLE:
      return Phase.SHUFFLE;
    case SORT:
      return Phase.SORT;
    case REDUCE:
      return Phase.REDUCE;
    case CLEANUP:
      return Phase.CLEANUP;
    default:
      break;
    }
    throw new YarnRuntimeException("Unrecognized Phase: " + phase);
  }

  /**
   * 将YARN版任务尝试完成事件数组批量转换为旧版mapred任务完成事件数组
   * @param newEvents YARN API的TaskAttemptCompletionEvent数组
   * @return 旧版mapred API的TaskCompletionEvent数组
   */
  public static TaskCompletionEvent[] fromYarn(
      TaskAttemptCompletionEvent[] newEvents) {
    TaskCompletionEvent[] oldEvents =
        new TaskCompletionEvent[newEvents.length];
    int i = 0;
    for (TaskAttemptCompletionEvent newEvent
        : newEvents) {
      oldEvents[i++] = fromYarn(newEvent);
    }
    return oldEvents;
  }

  /**
   * 将单个YARN版任务尝试完成事件转换为旧版mapred任务完成事件
   * @param newEvent YARN API的TaskAttemptCompletionEvent对象
   * @return 旧版mapred API的TaskCompletionEvent对象
   */
  public static TaskCompletionEvent fromYarn(
      TaskAttemptCompletionEvent newEvent) {
    return new TaskCompletionEvent(newEvent.getEventId(),
              fromYarn(newEvent.getAttemptId()), newEvent.getAttemptId().getId(),
              newEvent.getAttemptId().getTaskId().getTaskType().equals(TaskType.MAP),
              fromYarn(newEvent.getStatus()),
              newEvent.getMapOutputServerAddress());
  }

  /**
   * 将YARN版任务尝试完成状态转换为旧版mapred任务完成状态
   * @param newStatus YARN API的TaskAttemptCompletionEventStatus对象
   * @return 旧版mapred API的TaskCompletionEvent.Status对象
   */
  public static TaskCompletionEvent.Status fromYarn(
      TaskAttemptCompletionEventStatus newStatus) {
    switch (newStatus) {
    case FAILED:
      return TaskCompletionEvent.Status.FAILED;
    case KILLED:
      return TaskCompletionEvent.Status.KILLED;
    case OBSOLETE:
      return TaskCompletionEvent.Status.OBSOLETE;
    case SUCCEEDED:
      return TaskCompletionEvent.Status.SUCCEEDED;
    case TIPFAILED:
      return TaskCompletionEvent.Status.TIPFAILED;
    }
    throw new YarnRuntimeException("Unrecognized status: " + newStatus);
  }

  /**
   * 将YARN版TaskAttemptId转换为旧版mapred TaskAttemptID
   * @param id YARN API的TaskAttemptId对象
   * @return 旧版mapred API的TaskAttemptID对象
   */
  public static org.apache.hadoop.mapred.TaskAttemptID fromYarn(
      TaskAttemptId id) {
    return new org.apache.hadoop.mapred.TaskAttemptID(fromYarn(id.getTaskId()),
        id.getId());
  }

  /**
   * 将旧版mapred TaskAttemptID转换为YARN版TaskAttemptId
   * @param id 旧版mapred API的TaskAttemptID对象
   * @return YARN API的TaskAttemptId对象
   */
  public static TaskAttemptId toYarn(
      org.apache.hadoop.mapred.TaskAttemptID id) {
    TaskAttemptId taskAttemptId = recordFactory.newRecordInstance(TaskAttemptId.class);
    taskAttemptId.setTaskId(toYarn(id.getTaskID()));
    taskAttemptId.setId(id.getId());
    return taskAttemptId;
  }

  /**
   * 将新版mapreduce TaskAttemptID转换为YARN版TaskAttemptId
   * @param id 新版mapreduce API的TaskAttemptID对象
   * @return YARN API的TaskAttemptId对象
   */
  public static TaskAttemptId toYarn(
      org.apache.hadoop.mapreduce.TaskAttemptID id) {
    TaskAttemptId taskAttemptId = recordFactory.newRecordInstance(TaskAttemptId.class);
    taskAttemptId.setTaskId(toYarn(id.getTaskID()));
    taskAttemptId.setId(id.getId());
    return taskAttemptId;
  }

  /**
   * 将YARN版计数器集合转换为新版mapreduce计数器集合
   * @param yCntrs YARN API的Counters对象
   * @return 新版mapreduce API的Counters对象
   */
  public static org.apache.hadoop.mapreduce.Counters fromYarn(
      Counters yCntrs) {
    if (yCntrs == null) {
      return null;
    }
    org.apache.hadoop.mapreduce.Counters counters =
      new org.apache.hadoop.mapreduce.Counters();
    for (CounterGroup yGrp : yCntrs.getAllCounterGroups().values()) {
      counters.addGroup(yGrp.getName(), yGrp.getDisplayName());
      for (Counter yCntr : yGrp.getAllCounters().values()) {
        org.apache.hadoop.mapreduce.Counter c =
          counters.findCounter(yGrp.getName(),
              yCntr.getName());
        // if c can be found, or it will be skipped.
        if (c != null) {
          c.setValue(yCntr.getValue());
        }
      }
    }
    return counters;
  }

  /**
   * 将旧版mapred计数器集合转换为YARN版计数器集合
   * @param counters 旧版mapred API的Counters对象
   * @return YARN API的Counters对象
   */
  public static Counters toYarn(org.apache.hadoop.mapred.Counters counters) {
    if (counters == null) {
      return null;
    }
    Counters yCntrs = recordFactory.newRecordInstance(Counters.class);
    yCntrs.addAllCounterGroups(new HashMap<String, CounterGroup>());
    for (org.apache.hadoop.mapred.Counters.Group grp : counters) {
      CounterGroup yGrp = recordFactory.newRecordInstance(CounterGroup.class);
      yGrp.setName(grp.getName());
      yGrp.setDisplayName(grp.getDisplayName());
      yGrp.addAllCounters(new HashMap<String, Counter>());
      for (org.apache.hadoop.mapred.Counters.Counter cntr : grp) {
        Counter yCntr = recordFactory.newRecordInstance(Counter.class);
        yCntr.setName(cntr.getName());
        yCntr.setDisplayName(cntr.getDisplayName());
        yCntr.setValue(cntr.getValue());
        yGrp.setCounter(yCntr.getName(), yCntr);
      }
      yCntrs.setCounterGroup(yGrp.getName(), yGrp);
    }
    return yCntrs;
  }

  /**
   * 将新版mapreduce计数器集合转换为YARN版计数器集合
   * @param counters 新版mapreduce API的Counters对象
   * @return YARN API的Counters对象
   */
  public static Counters toYarn(org.apache.hadoop.mapreduce.Counters counters) {
    if (counters == null) {
      return null;
    }
    Counters yCntrs = recordFactory.newRecordInstance(Counters.class);
    yCntrs.addAllCounterGroups(new HashMap<String, CounterGroup>());
    for (org.apache.hadoop.mapreduce.CounterGroup grp : counters) {
      CounterGroup yGrp = recordFactory.newRecordInstance(CounterGroup.class);
      yGrp.setName(grp.getName());
      yGrp.setDisplayName(grp.getDisplayName());
      yGrp.addAllCounters(new HashMap<String, Counter>());
      for (org.apache.hadoop.mapreduce.Counter cntr : grp) {
        Counter yCntr = recordFactory.newRecordInstance(Counter.class);