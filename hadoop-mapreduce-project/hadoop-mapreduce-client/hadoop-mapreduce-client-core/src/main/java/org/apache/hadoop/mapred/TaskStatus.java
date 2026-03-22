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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述MapReduce任务的当前运行状态，是MapTaskStatus和ReduceTaskStatus的抽象基类
 * 用于在TaskTracker、JobTracker之间传递任务执行状态信息
 */
/**************************************************
 * Describes the current status of a task.  This is
 * not intended to be a comprehensive piece of data.
 *
 **************************************************/
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class TaskStatus implements Writable, Cloneable {
  static final Logger LOG =
      LoggerFactory.getLogger(TaskStatus.class.getName());
  
  /** 任务执行阶段枚举 */
  //enumeration for reporting current phase of a task.
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public enum Phase{STARTING, MAP, SHUFFLE, SORT, REDUCE, CLEANUP}

  /** 任务运行状态枚举 */
  // what state is the task in?
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public enum State {RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED,
                            COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN, PREEMPTED}
    
  private final TaskAttemptID taskid;
  private float progress;
  private volatile State runState;
  private String diagnosticInfo;
  private String stateString;
  private String taskTracker;
  private int numSlots;
    
  private long startTime; //in ms
  private long finishTime; 
  private long outputSize = -1L;
    
  private volatile Phase phase = Phase.STARTING; 
  private Counters counters;
  private boolean includeAllCounters;
  private SortedRanges.Range nextRecordRange = new SortedRanges.Range();
  
  // 任务状态信息字符串最大长度
  static final int MAX_STRING_SIZE = 1024;

  /**
   * 测试用方法，用于覆盖获取最大字符串长度的逻辑，控制TaskStatus中字符串的最大长度
   * 仅用于单元测试，生产环境不会被修改
   * @return 允许的最大字符串长度
   */
  protected int getMaxStringSize() {
    return MAX_STRING_SIZE;
  }
  
  /**
   * 空构造函数，用于反序列化
   */
  public TaskStatus() {
    taskid = new TaskAttemptID();
    numSlots = 0;
  }

  /**
   * 构造TaskStatus对象，初始化任务状态信息
   * @param taskid 任务尝试ID
   * @param progress 任务进度（0-1）
   * @param numSlots 任务占用的槽位数
   * @param runState 任务运行状态
   * @param diagnosticInfo 诊断信息
   * @param stateString 状态描述字符串
   * @param taskTracker 运行任务的TaskTracker名称
   * @param phase 任务执行阶段
   * @param counters 任务计数器
   */
  public TaskStatus(TaskAttemptID taskid, float progress, int numSlots,
                    State runState, String diagnosticInfo,
                    String stateString, String taskTracker,
                    Phase phase, Counters counters) {
    this.taskid = taskid;
    this.progress = progress;
    this.numSlots = numSlots;
    this.runState = runState;
    setDiagnosticInfo(diagnosticInfo);
    setStateString(stateString);
    this.taskTracker = taskTracker;
    this.phase = phase;
    this.counters = counters;
    this.includeAllCounters = true;
  }
  
  public TaskAttemptID getTaskID() { return taskid; }
  /**
   * 判断任务是否为Map任务，由子类实现
   * @return true表示Map任务，false表示Reduce任务
   */
  public abstract boolean getIsMap();
  public int getNumSlots() {
    return numSlots;
  }

  public float getProgress() { return progress; }
  public void setProgress(float progress) {
    this.progress = progress;
  } 
  public State getRunState() { return runState; }
  public String getTaskTracker() {return taskTracker;}
  public void setTaskTracker(String tracker) { this.taskTracker = tracker;}
  public void setRunState(State runState) { this.runState = runState; }
  public String getDiagnosticInfo() { return diagnosticInfo; }
  
  /**
   * 设置任务诊断信息，追加新信息并限制总长度不超过最大值
   * @param info 要添加的诊断信息
   */
  public void setDiagnosticInfo(String info) {
    // 如果诊断信息已经达到最大长度，直接记录日志并返回
    if (diagnosticInfo != null 
        && diagnosticInfo.length() == getMaxStringSize()) {
      LOG.info("task-diagnostic-info for task " + taskid + " : " + info);
      return;
    }
    // 拼接新诊断信息
    diagnosticInfo = 
      ((diagnosticInfo == null) ? info : diagnosticInfo.concat(info)); 
    // 如果超过最大长度，截断并记录完整日志
    if (diagnosticInfo != null 
        && diagnosticInfo.length() > getMaxStringSize()) {
      LOG.info("task-diagnostic-info for task " + taskid + " : " 
               + diagnosticInfo);
      diagnosticInfo = diagnosticInfo.substring(0, getMaxStringSize());
    }
  }
  public String getStateString() { return stateString; }
  /**
   * Set the state of the {@link TaskStatus}.
   */
  /**
   * 设置任务状态描述字符串，超过最大长度则截断并记录日志
   * @param stateString 状态描述字符串
   */
  public void setStateString(String stateString) {
    if (stateString != null) {
      if (stateString.length() <= getMaxStringSize()) {
        this.stateString = stateString;
      } else {
        // 记录完整字符串日志
        LOG.info("state-string for task " + taskid + " : " + stateString);
        // 截断超长字符串
        this.stateString = stateString.substring(0, getMaxStringSize());
      }
    }
  }
  
  /**
   * Get the next record range which is going to be processed by Task.
   * @return nextRecordRange
   */
  /**
   * 获取任务接下来将要处理的记录范围
   * @return 下一个待处理记录范围
   */
  public SortedRanges.Range getNextRecordRange() {
    return nextRecordRange;
  }

  /**
   * Set the next record range which is going to be processed by Task.
   * @param nextRecordRange
   */
  /**
   * 设置任务接下来将要处理的记录范围
   * @param nextRecordRange 下一个待处理记录范围
   */
  public void setNextRecordRange(SortedRanges.Range nextRecordRange) {
    this.nextRecordRange = nextRecordRange;
  }
  
  /**
   * Get task finish time. if shuffleFinishTime and sortFinishTime 
   * are not set before, these are set to finishTime. It takes care of 
   * the case when shuffle, sort and finish are completed with in the 
   * heartbeat interval and are not reported separately. if task state is 
   * TaskStatus.FAILED then finish time represents when the task failed.
   * @return finish time of the task. 
   */
  /**
   * 获取任务结束时间，如果任务失败则表示失败发生的时间
   * 如果shuffle和sort结束时间未单独设置，会默认使用任务结束时间
   * @return 任务结束时间（毫秒）
   */
  public long getFinishTime() {
    return finishTime;
  }

  /**
   * Sets finishTime for the task status if and only if the
   * start time is set and passed finish time is greater than
   * zero.
   * 
   * @param finishTime finish time of task.
   */
  /**
   * 设置任务结束时间，仅在开始时间已设置且结束时间合法时生效
   * @param finishTime 任务结束时间（毫秒）
   */
  void setFinishTime(long finishTime) {
    if(this.getStartTime() > 0 && finishTime > 0) {
      this.finishTime = finishTime;
    } else {
      // 记录错误堆栈日志
      LOG.error("Trying to set finish time for task " + taskid + 
          " when no start time is set, stackTrace is : " + 
      		StringUtils.stringifyException(new Exception()));
    }
  }
  /**
   * Get shuffle finish time for the task. If shuffle finish time was 
   * not set due to shuffle/sort/finish phases ending within same
   * heartbeat interval, it is set to finish time of next phase i.e. sort 
   * or task finish when these are set.  
   * @return 0 if shuffleFinishTime, sortFinishTime and finish time are not set. else 
   * it returns approximate shuffle finish time.  
   */
  /**
   * 获取Shuffle阶段结束时间，如果未单独设置则返回0，由子类实现
   * @return Shuffle阶段结束时间
   */
  public long getShuffleFinishTime() {
    return 0;
  }

  /**
   * Set shuffle finish time. 
   * @param shuffleFinishTime 
   */
  /**
   * 设置Shuffle阶段结束时间，由子类实现
   * @param shuffleFinishTime Shuffle阶段结束时间
   */
  void setShuffleFinishTime(long shuffleFinishTime) {}

  /**
   * Get map phase finish time for the task. If map finsh time was
   * not set due to sort phase ending within same heartbeat interval,
   * it is set to finish time of next phase i.e. sort phase
   * when it is set.
   * @return 0 if mapFinishTime, sortFinishTime are not set. else 
   * it returns approximate map finish time.
   */
  /**
   * 获取Map阶段结束时间，如果未单独设置则返回0，由子类实现
   * @return Map阶段结束时间
   */
  public long getMapFinishTime() {
    return 0;
  }
  
  /**
   * Set map phase finish time. 
   * @param mapFinishTime 
   */
  /**
   * 设置Map阶段结束时间，由子类实现
   * @param mapFinishTime Map阶段结束时间
   */
  void setMapFinishTime(long mapFinishTime) {}

  /**
   * Get sort finish time for the task,. If sort finish time was not set 
   * due to sort and reduce phase finishing in same heartebat interval, it is 
   * set to finish time, when finish time is set. 
   * @return 0 if sort finish time and finish time are not set, else returns sort
   * finish time if that is set, else it returns finish time. 
   */
  /**
   * 获取Sort阶段结束时间，如果未单独设置则返回任务结束时间，由子类实现
   * @return Sort阶段结束时间
   */
  public long getSortFinishTime() {
    return 0;
  }

  /**
   * Sets sortFinishTime, if shuffleFinishTime is not set before 
   * then its set to sortFinishTime.  
   * @param sortFinishTime
   */
  /**
   * 设置Sort阶段结束时间，由子类实现
   * @param sortFinishTime Sort阶段结束时间
   */
  void setSortFinishTime(long sortFinishTime) {}

  /**
   * Get start time of the task. 
   * @return 0 is start time is not set, else returns start time. 
   */
  /**
   * 获取任务开始时间
   * @return 任务开始时间（毫秒），未设置则返回0
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set startTime of the task if start time is greater than zero.
   * @param startTime start time
   */
  /**
   * 设置任务开始时间，仅在时间为正值时生效
   * @param startTime 任务开始时间（毫秒）
   */
  void setStartTime(long startTime) {
    // 仅接受正的时间戳
    if (startTime > 0) {
      this.startTime = startTime;
    } else {
      // 记录非法参数错误堆栈
      LOG.error("Trying to set illegal startTime for task : " + taskid +
          ".Stack trace is : " +
          StringUtils.stringifyException(new Exception()));
    }
  }
  /**
   * Get current phase of this task. Phase.Map in case of map tasks, 
   * for reduce one of Phase.SHUFFLE, Phase.SORT or Phase.REDUCE. 
   * @return . 
   */
  /**
   * 获取任务当前执行阶段
   * @return 任务执行阶段
   */
  public Phase getPhase(){
    return this.phase; 
  }
  /**
   * Set current phase of this task.  
   * @param phase phase of this task
   */
  /**
   * 设置任务当前执行阶段，切换阶段时自动记录前一阶段的结束时间
   * @param phase 任务执行阶段
   */
  public void setPhase(Phase phase){
    TaskStatus.Phase oldPhase = getPhase();
    if (oldPhase != phase){
      // 进入排序阶段，自动记录上一阶段结束时间
      if (phase == TaskStatus.Phase.SORT){
        if (oldPhase == TaskStatus.Phase.MAP) {
          setMapFinishTime(System.currentTimeMillis());
        }
        else {
          setShuffleFinishTime(System.currentTimeMillis());
        }
      }else if (phase == TaskStatus.Phase.REDUCE){
        // 进入Reduce阶段，自动记录排序结束时间
        setSortFinishTime(System.currentTimeMillis());
      }
      this.phase = phase;
    }
  }

  /**
   * 判断任务是否处于清理阶段（任务失败/被杀死后的清理）
   * @return true表示处于清理阶段，false否则
   */
  boolean inTaskCleanupPhase() {
    return (this.phase == TaskStatus.Phase.CLEANUP && 
      (this.runState == TaskStatus.State.FAILED_UNCLEAN || 
      this.runState == TaskStatus.State.KILLED_UNCLEAN));
  }
  
  public boolean getIncludeAllCounters() {
    return includeAllCounters;
  }
  
  /**
   * 设置是否需要发送全部计数器，并更新计数器的写入配置
   * @param send true表示发送全部计数器，false仅发送变更过的计数器
   */
  public void setIncludeAllCounters(boolean send) {
    includeAllCounters = send;
    counters.setWriteAllCounters(send);
  }
  
  /**
   * Get task's counters.
   */
  /**
   * 获取任务计数器
   * @return 任务计数器对象
   */
  public Counters getCounters() {
    return counters;
  }
  /**
   * Set the task's counters.
   * @param counters
   */
  /**
   * 设置任务计数器
   * @param counters 任务计数器对象
   */
  public void setCounters(Counters counters) {
    this.counters = counters;
  }
  
  /**
   * Returns the number of bytes of output from this map.
   */
  /**
   * 获取任务输出字节数
   * @return 任务输出字节数
   */
  public long getOutputSize() {
    return outputSize;
  }
  
  /**
   * Set the size on disk of this task's output.
   * @param l the number of map output bytes
   */
  /**
   * 设置任务输出字节数
   * @param l 输出字节数
   */
  void setOutputSize(long l)  {
    outputSize = l;
  }
  
  /**
   * Get the list of maps from which output-fetches failed.
   * 
   * @return the list of maps from which output-fetches failed.
   */
  /**
   * 获取Fetch失败的Map任务列表，仅Reduce任务有意义
   * @return 拉取输出失败的Map任务尝试ID列表，默认返回null
   */
  public List<TaskAttemptID> getFetchFailedMaps() {
    return null;
  }

  /**
   * Add to the list of maps from which output-fetches failed.
   *  
   * @param mapTaskId map from which fetch failed
   */
  /**
   * 添加拉取输出失败的Map任务到失败列表，