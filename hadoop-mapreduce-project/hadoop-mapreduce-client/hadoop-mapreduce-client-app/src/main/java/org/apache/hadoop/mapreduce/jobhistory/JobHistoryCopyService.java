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

package org.apache.hadoop.mapreduce.jobhistory;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.util.functional.FutureIO.awaitFuture;

/**
 * 文件：作业历史复制服务
 * 功能：从已有的作业历史文件中读取历史事件，重新转发给事件处理器进行记录，
 *       用于应用尝试失败重启后恢复之前已生成的作业历史数据。
 * 实现：作为复合服务组件，在服务启动时触发解析历史文件流程，
 *       将解析得到的事件转发给目标事件处理器。
 */
public class JobHistoryCopyService extends CompositeService implements HistoryEventHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobHistoryCopyService.class);

  private final ApplicationAttemptId applicationAttemptId;
  private final EventHandler<Event> handler;
  private final JobId jobId;

  /**
   * 构造作业历史复制服务实例。
   * @param applicationAttemptId 当前YARN应用尝试ID
   * @param handler 目标事件处理器，接收转发的历史事件
   */
  public JobHistoryCopyService(ApplicationAttemptId applicationAttemptId, 
      EventHandler<Event> handler) {
    super("JobHistoryCopyService");
    this.applicationAttemptId = applicationAttemptId;
    this.jobId =  TypeConverter.toYarn(
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId()));
    this.handler = handler;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }
  
  @Override
  public void handleEvent(HistoryEvent event) throws IOException {
    // 跳过AM启动事件，该事件由其他模块处理
    if (!(event instanceof AMStartedEvent)) {
      handler.handle(new JobHistoryEvent(jobId, event));
    }
  }
  
  @Override
  protected void serviceStart() throws Exception {
    try {
      //TODO should we parse on a background thread???
      // 解析前一次应用尝试的作业历史文件
      parse();
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStart();
  }
  
  /**
   * 解析前一次应用尝试生成的作业历史文件，将事件转发给当前处理器。
   * @throws IOException 读取文件或解析失败时抛出IO异常
   */
  private void parse() throws IOException {
    FSDataInputStream in = null;
    try {
      // 获取前一次尝试的作业历史文件输入流
      in =  getPreviousJobHistoryFileStream(getConfig(), applicationAttemptId);
    } catch (IOException e) {
      LOG.warn("error trying to open previous history file. No history data " +
      		"will be copied over.", e);
      return;
    }
    // 创建作业历史解析器
    JobHistoryParser parser = new JobHistoryParser(in);
    // 开始解析，将事件转发到当前服务处理
    parser.parse(this);
    // 获取解析过程中产生的异常，记录日志忽略不完整事件
    Exception parseException = parser.getParseException();
    if (parseException != null) {
      LOG.info("Got an error parsing job-history file" + 
          ", ignoring incomplete events.", parseException);
    }
  }

  /**
   * 获取前一次应用尝试生成的作业历史文件输入流。
   * @param conf Hadoop配置对象
   * @param applicationAttemptId 当前应用尝试ID
   * @return 前一次作业历史文件的输入流
   * @throws IOException 定位或打开文件失败时抛出IO异常
   */
  public static FSDataInputStream getPreviousJobHistoryFileStream(
      Configuration conf, ApplicationAttemptId applicationAttemptId)
      throws IOException {
    FSDataInputStream in = null;
    Path historyFile = null;
    String jobId =
        TypeConverter.fromYarn(applicationAttemptId.getApplicationId())
          .toString();
    // 从配置获取作业历史临时目录前缀
    String jobhistoryDir =
        JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(conf, jobId);
    // 构建并规范化历史目录路径
    Path histDirPath =
        FileContext.getFileContext(conf).makeQualified(new Path(jobhistoryDir));
    // 获取对应文件系统上下文
    FileContext fc = FileContext.getFileContext(histDirPath.toUri(), conf);
    // 获取前一次尝试的作业历史文件路径（尝试ID减1对应上一次尝试）
    historyFile =
        fc.makeQualified(JobHistoryUtils.getStagingJobHistoryFile(histDirPath,
          jobId, (applicationAttemptId.getAttemptId() - 1)));
    LOG.info("History file is at " + historyFile);
    // 使用整文件读取策略打开文件，等待异步打开完成并获取输入流
    in = awaitFuture(
        fc.openFile(historyFile)
            .opt(FS_OPTION_OPENFILE_READ_POLICY,
                FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE)
            .build());
    return in;
  }
  
  
}