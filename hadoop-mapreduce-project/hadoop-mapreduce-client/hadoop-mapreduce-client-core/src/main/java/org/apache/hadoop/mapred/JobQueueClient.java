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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * <code>JobQueueClient</code> is interface provided to the user in order to get
 * JobQueue related information from the {@link JobTracker}
 * 
 * It provides the facility to list the JobQueues present and ability to view
 * the list of jobs within a specific JobQueue
 * 
 **/

/**
 * 作业队列客户端，提供从JobTracker获取作业队列相关信息的客户端工具
 * 支持列出所有队列、查看指定队列信息、查看当前用户队列ACL权限等功能
 */
class JobQueueClient extends Configured implements Tool {

  JobClient jc;

  public JobQueueClient() {
  }

  public JobQueueClient(JobConf conf) throws IOException {
    setConf(conf);
  }

  /**
   * 初始化JobClient客户端实例
   * @param conf 作业配置对象
   * @throws IOException 初始化失败抛出IO异常
   */
  private void init(JobConf conf) throws IOException {
    setConf(conf);
    jc = new JobClient(conf);
  }

  @Override
  /**
   * 执行命令行工具入口方法，解析参数并调用对应功能
   * @param argv 命令行参数数组
   * @return 执行结果退出码，0表示成功，-1表示失败
   * @throws Exception 执行过程中抛出异常
   */
  public int run(String[] argv) throws Exception {
    int exitcode = -1;

    if (argv.length < 1) {
      // 参数不足，显示帮助信息
      displayUsage("");
      return exitcode;
    }
    String cmd = argv[0];
    boolean displayQueueList = false;
    boolean displayQueueInfoWithJobs = false;
    boolean displayQueueInfoWithoutJobs = false;
    boolean displayQueueAclsInfoForCurrentUser = false;

    // 解析命令行参数
    if ("-list".equals(cmd)) {
      displayQueueList = true;
    } else if ("-showacls".equals(cmd)) {
      displayQueueAclsInfoForCurrentUser = true;
    } else if ("-info".equals(cmd)) {
      if (argv.length == 2 && !(argv[1].equals("-showJobs"))) {
        displayQueueInfoWithoutJobs = true;
      } else if (argv.length == 3) {
        if (argv[2].equals("-showJobs")) {
          displayQueueInfoWithJobs = true;
        } else {
          displayUsage(cmd);
          return exitcode;
        }
      } else {
        displayUsage(cmd);
        return exitcode;
      }
    } else {
      displayUsage(cmd);
      return exitcode;
    }
    
    // 初始化客户端
    JobConf conf = new JobConf(getConf());
    init(conf);
    // 根据参数执行对应功能
    if (displayQueueList) {
      displayQueueList();
      exitcode = 0;
    } else if (displayQueueInfoWithoutJobs) {
      displayQueueInfo(argv[1], false);
      exitcode = 0;
    } else if (displayQueueInfoWithJobs) {
      displayQueueInfo(argv[1], true);
      exitcode = 0;
    } else if (displayQueueAclsInfoForCurrentUser) {
      this.displayQueueAclsInfoForCurrentUser();
      exitcode = 0;
    }
    return exitcode;
  }

// format and print information about the passed in job queue.
  /**
   * 格式化并打印指定作业队列的信息，输出到指定Writer
   * @param jobQueueInfo 作业队列信息对象
   * @param writer 输出目标Writer
   * @throws IOException 写入异常
   */
  void printJobQueueInfo(JobQueueInfo jobQueueInfo, Writer writer)
    throws IOException {
    printJobQueueInfo(jobQueueInfo, writer, "");
  }

  // format and print information about the passed in job queue.
  /**
   * 格式化并打印指定作业队列的信息（支持层级缩进输出子队列）
   * @param jobQueueInfo 作业队列信息对象
   * @param writer 输出目标Writer
   * @param prefix 行前缀缩进，用于输出层级结构
   * @throws IOException 写入异常
   */
  @SuppressWarnings("deprecation")
  void printJobQueueInfo(JobQueueInfo jobQueueInfo, Writer writer,
    String prefix) throws IOException {
    if (jobQueueInfo == null) {
      writer.write("No queue found.\n");
      writer.flush();
      return;
    }
    // 输出队列基本信息
    writer.write(String.format(prefix + "======================\n"));
    writer.write(String.format(prefix + "Queue Name : %s \n",
        jobQueueInfo.getQueueName()));
    writer.write(String.format(prefix + "Queue State : %s \n",
        jobQueueInfo.getQueueState()));
    writer.write(String.format(prefix + "Scheduling Info : %s \n",
        jobQueueInfo.getSchedulingInfo()));
    // 递归输出子队列
    List<JobQueueInfo> childQueues = jobQueueInfo.getChildren();
    if (childQueues != null && childQueues.size() > 0) {
      for (int i = 0; i < childQueues.size(); i++) {
	  printJobQueueInfo(childQueues.get(i), writer, "    " + prefix);
      }
    }
    writer.flush();
  }
  
  /**
   * 显示所有根队列的基本信息，输出到标准输出
   * @throws IOException 获取队列信息失败抛出IO异常
   */
  private void displayQueueList() throws IOException {
    JobQueueInfo[] rootQueues = jc.getRootQueues();
    for (JobQueueInfo queue : rootQueues) {
      printJobQueueInfo(queue, new PrintWriter(new OutputStreamWriter(
          System.out, StandardCharsets.UTF_8)));
    }
  }
  
  /**
   * 深度优先展开队列层级结构，返回所有队列的扁平列表
   * @param rootQueues 顶级根队列数组
   * @return 深度优先顺序排列的所有队列列表
   */
  List<JobQueueInfo> expandQueueList(JobQueueInfo[] rootQueues) {
    List<JobQueueInfo> allQueues = new ArrayList<JobQueueInfo>();
    for (JobQueueInfo queue : rootQueues) {
      allQueues.add(queue);
      if (queue.getChildren() != null) {
        JobQueueInfo[] childQueues 
          = queue.getChildren().toArray(new JobQueueInfo[0]);
        allQueues.addAll(expandQueueList(childQueues));
      }
    }
    return allQueues;
  }
 
  /**
   * 显示指定队列的详细信息，可选择是否同时显示队列中的作业列表
   * @param queue 目标队列名称
   * @param showJobs 是否显示队列中的作业列表
   * @throws IOException 获取信息失败抛出IO异常
   * @throws InterruptedException 中断异常
   */
  private void displayQueueInfo(String queue, boolean showJobs)
      throws IOException, InterruptedException {
    JobQueueInfo jobQueueInfo = jc.getQueueInfo(queue);
    
    if (jobQueueInfo == null) {
      System.out.println("Queue \"" + queue + "\" does not exist.");
      return;
    }
    // 打印队列基本信息
    printJobQueueInfo(jobQueueInfo, new PrintWriter(new OutputStreamWriter(
        System.out, StandardCharsets.UTF_8)));
    // 如果需要显示作业且当前队列是叶子队列，打印作业列表
    if (showJobs && (jobQueueInfo.getChildren() == null ||
        jobQueueInfo.getChildren().size() == 0)) {
      JobStatus[] jobs = jobQueueInfo.getJobStatuses();
      if (jobs == null)
        jobs = new JobStatus[0];
      jc.displayJobList(jobs);
    }
  }
   
  /**
   * 显示当前登录用户对各队列拥有的ACL操作权限
   * @throws IOException 获取权限信息失败抛出IO异常
   */
  private void displayQueueAclsInfoForCurrentUser() throws IOException {
    QueueAclsInfo[] queueAclsInfoList = jc.getQueueAclsForCurrentUser();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    if (queueAclsInfoList.length > 0) {
      System.out.println("Queue acls for user :  " + ugi.getShortUserName());
      System.out.println("\nQueue  Operations");
      System.out.println("=====================");
      // 遍历每个队列，输出权限操作
      for (QueueAclsInfo queueInfo : queueAclsInfoList) {
        System.out.print(queueInfo.getQueueName() + "  ");
        String[] ops = queueInfo.getOperations();
        Arrays.sort(ops);
        int max = ops.length - 1;
        for (int j = 0; j < ops.length; j++) {
          // 移除acl-前缀，简化输出
          System.out.print(ops[j].replaceFirst("acl-", ""));
          if (j < max) {
            System.out.print(",");
          }
        }
        System.out.println();
      }
    } else {
      System.out.println("User " + ugi.getShortUserName()
          + " does not have access to any queue. \n");
    }
  }

  /**
   * 显示工具命令行使用帮助信息
   * @param cmd 输入的命令名称
   */
  private void displayUsage(String cmd) {
    String prefix = "Usage: queue ";
    if ("-queueinfo".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + "<job-queue-name> [-showJobs]]");
    } else {
      System.err.printf(prefix + "<command> <args>%n");
      System.err.printf("\t[-list]%n");
      System.err.printf("\t[-info <job-queue-name> [-showJobs]]%n");
      System.err.printf("\t[-showacls] %n%n");
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  /**
   * 工具主入口方法
   * @param argv 命令行参数
   * @throws Exception 执行异常
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new JobQueueClient(), argv);
    System.exit(res);
  }

}