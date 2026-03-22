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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * MapReduce任务子JVM工具类，负责构建子JVM启动命令和配置子JVM运行环境
 * 用于NodeManager上启动Map/Reduce任务的独立JVM进程
 */
@SuppressWarnings("deprecation")
public class MapReduceChildJVM {

  /**
   * 获取任务日志文件的相对路径，基于YARN日志目录变量展开
   * @param filter 日志类型（STDOUT/STDERR/PROFILE等）
   * @return 日志文件相对路径
   */
  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + 
        filter.toString();
  }

  /**
   * 根据任务类型获取对应的自定义环境变量配置项名称
   * @param jobConf 作业配置
   * @param isMap 是否是Map任务
   * @return 环境变量配置项名称
   */
  private static String getChildEnvProp(JobConf jobConf, boolean isMap) {
    if (isMap) {
      return JobConf.MAPRED_MAP_TASK_ENV;
    }
    return JobConf.MAPRED_REDUCE_TASK_ENV;
  }

  /**
   * 获取任务环境变量的默认值，回退到全局任务环境配置
   * @param jobConf 作业配置
   * @return 默认环境变量字符串
   */
  private static String getChildEnvDefaultValue(JobConf jobConf) {
    // There is no default value for these - use the fallback value instead.
    return jobConf.get(JobConf.MAPRED_TASK_ENV);
  }

  /**
   * 配置子JVM进程的环境变量，包含用户自定义变量、日志配置和日志路径
   * @param environment 环境变量Map
   * @param task 当前任务对象
   */
  public static void setVMEnv(Map<String, String> environment,
      Task task) {

    JobConf conf = task.conf;
    boolean isMap = task.isMapTask();

    // 先移除已有配置，避免用户配置追加时重复拼接
    String hadoopRootLoggerKey = "HADOOP_ROOT_LOGGER";
    String hadoopClientOptsKey = "HADOOP_CLIENT_OPTS";
    environment.remove(hadoopRootLoggerKey);
    environment.remove(hadoopClientOptsKey);

    // 添加用户自定义的任务级环境变量
    MRApps.setEnvFromInputProperty(environment, getChildEnvProp(conf, isMap),
        getChildEnvDefaultValue(conf), conf);

    // 如果用户未配置日志级别和客户端参数，使用默认值
    if (!environment.containsKey(hadoopRootLoggerKey)) {
      // 设置日志级别，确保子进程派生的hadoop命令也使用正确日志级别
      environment.put(hadoopRootLoggerKey,
          MRApps.getChildLogLevel(conf, task.isMapTask()) + ",console");
    }
    if (!environment.containsKey(hadoopClientOptsKey)) {
      // 继承父进程的HADOOP_CLIENT_OPTS，支持流式任务等场景
      String hadoopClientOptsValue = System.getenv(hadoopClientOptsKey);
      if (hadoopClientOptsValue == null) {
        hadoopClientOptsValue = "";
      } else {
        hadoopClientOptsValue = hadoopClientOptsValue + " ";
      }
      environment.put(hadoopClientOptsKey, hadoopClientOptsValue);
    }

    // 设置标准输出/标准错误日志文件路径环境变量
    environment.put(
        MRJobConfig.STDOUT_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDOUT)
        );
    environment.put(
        MRJobConfig.STDERR_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDERR)
        );
  }

  /**
   * 根据任务类型获取子JVM的Java启动参数配置
   * @param jobConf 作业配置
   * @param isMapTask 是否是Map任务
   * @return JavaOpts字符串
   */
  private static String getChildJavaOpts(JobConf jobConf, boolean isMapTask) {
    return jobConf.getTaskJavaOpts(isMapTask ? TaskType.MAP : TaskType.REDUCE);
  }

  /**
   * 构建完整的子JVM启动命令，包含Java路径、堆配置、日志配置和启动参数
   * @param taskAttemptListenerAddr TaskAttemptListener服务地址，用于子任务心跳汇报
   * @param task 当前任务对象
   * @param jvmID JVM标识ID
   * @return 构建完成的启动命令列表
   */
  public static List<String> getVMCommand(
      InetSocketAddress taskAttemptListenerAddr, Task task, 
      JVMId jvmID) {

    TaskAttemptID attemptID = task.getTaskID();
    JobConf conf = task.conf;

    Vector<String> vargs = new Vector<String>(8);

    // 添加Java可执行文件路径，从环境变量获取JAVA_HOME
    vargs.add(MRApps.crossPlatformifyMREnv(task.conf, Environment.JAVA_HOME)
        + "/bin/java");

    // 解析并添加任务自定义JavaOpts，替换@taskid@占位符为当前任务ID
    String javaOpts = getChildJavaOpts(conf, task.isMapTask());
    javaOpts = javaOpts.replace("@taskid@", attemptID.toString());
    String [] javaOptsSplit = javaOpts.split(" ");
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }

    // 设置临时目录为容器内临时目录
    Path childTmpDir = new Path(MRApps.crossPlatformifyMREnv(conf, Environment.PWD),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);
    // 添加log4j系统属性配置
    MRApps.addLog4jSystemProperties(task, vargs, conf);

    // 如果开启了任务 profiling，添加profiling参数
    if (conf.getProfileEnabled()) {
      if (conf.getProfileTaskRange(task.isMapTask()
                                   ).isIncluded(task.getPartition())) {
        final String profileParams = conf.get(task.isMapTask()
            ? MRJobConfig.TASK_MAP_PROFILE_PARAMS
            : MRJobConfig.TASK_REDUCE_PROFILE_PARAMS, conf.getProfileParams());
        vargs.add(String.format(profileParams,
            getTaskLogFile(TaskLog.LogName.PROFILE)));
      }
    }

    // 添加主类和启动参数
    vargs.add(YarnChild.class.getName());  // 子JVM主类
    // 添加TaskAttemptListener地址参数，用于子任务注册汇报
    vargs.add(taskAttemptListenerAddr.getAddress().getHostAddress()); 
    vargs.add(Integer.toString(taskAttemptListenerAddr.getPort())); 
    vargs.add(attemptID.toString());                      // 当前任务尝试ID

    // 添加JVM ID参数
    vargs.add(String.valueOf(jvmID.getId()));
    // 重定向标准输出和标准错误到日志文件
    vargs.add("1>" + getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + getTaskLogFile(TaskLog.LogName.STDERR));

    // 将所有参数拼接为单个命令字符串返回，适配YARN容器启动要求
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<String>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }
}