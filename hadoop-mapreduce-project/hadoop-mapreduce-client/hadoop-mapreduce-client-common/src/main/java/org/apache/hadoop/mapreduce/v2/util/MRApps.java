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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.util.ApplicationClassLoader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.ContainerLogAppender;
import org.apache.hadoop.yarn.ContainerRollingLogAppender;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.Apps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce应用工具类，提供ID转换、类路径设置、分布式缓存配置、日志配置等通用辅助功能
 * 供MR ApplicationMaster和任务容器初始化使用
 */
@Private
@Unstable
public class MRApps extends Apps {
  public static final Logger LOG = LoggerFactory.getLogger(MRApps.class);

  /**
   * 将Yarn JobId转换为字符串
   * @param jid Yarn格式JobId
   * @return 字符串形式JobId
   */
  public static String toString(JobId jid) {
    return jid.toString();
  }

  /**
   * 将字符串JobId转换为Yarn格式JobId
   * @param jid 字符串形式JobId
   * @return Yarn格式JobId
   */
  public static JobId toJobID(String jid) {
    return TypeConverter.toYarn(JobID.forName(jid));
  }

  /**
   * 将Yarn TaskId转换为字符串
   * @param tid Yarn格式TaskId
   * @return 字符串形式TaskId
   */
  public static String toString(TaskId tid) {
    return tid.toString();
  }

  /**
   * 将字符串TaskId转换为Yarn格式TaskId
   * @param tid 字符串形式TaskId
   * @return Yarn格式TaskId
   */
  public static TaskId toTaskID(String tid) {
    return TypeConverter.toYarn(TaskID.forName(tid));
  }

  /**
   * 将Yarn TaskAttemptId转换为字符串
   * @param taid Yarn格式TaskAttemptId
   * @return 字符串形式TaskAttemptId
   */
  public static String toString(TaskAttemptId taid) {
    return taid.toString(); 
  }

  /**
   * 将字符串TaskAttemptId转换为Yarn格式TaskAttemptId
   * @param taid 字符串形式TaskAttemptId
   * @return Yarn格式TaskAttemptId
   */
  public static TaskAttemptId toTaskAttemptID(String taid) {
    return TypeConverter.toYarn(TaskAttemptID.forName(taid));
  }

  /**
   * 根据任务类型获取单字符标识
   * @param type 任务类型（MAP/REDUCE）
   * @return 单字符标识 m/r
   */
  public static String taskSymbol(TaskType type) {
    switch (type) {
      case MAP:           return "m";
      case REDUCE:        return "r";
    }
    throw new YarnRuntimeException("Unknown task type: "+ type.toString());
  }

  /**
   * Web UI端任务尝试状态枚举，将多个底层状态聚合为UI展示用状态
   */
  public enum TaskAttemptStateUI {
    NEW(
        new TaskAttemptState[] { TaskAttemptState.NEW,
            TaskAttemptState.STARTING }),
    RUNNING(
        new TaskAttemptState[] { TaskAttemptState.RUNNING,
            TaskAttemptState.COMMIT_PENDING }),
    SUCCESSFUL(new TaskAttemptState[] { TaskAttemptState.SUCCEEDED}),
    FAILED(new TaskAttemptState[] { TaskAttemptState.FAILED}),
    KILLED(new TaskAttemptState[] { TaskAttemptState.KILLED});

    private final List<TaskAttemptState> correspondingStates;

    private TaskAttemptStateUI(TaskAttemptState[] correspondingStates) {
      this.correspondingStates = Arrays.asList(correspondingStates);
    }

    /**
     * 判断底层状态是否对应当前UI状态
     * @param state 底层任务尝试状态
     * @return 是否匹配
     */
    public boolean correspondsTo(TaskAttemptState state) {
      return this.correspondingStates.contains(state);
    }
  }

  /**
   * Web UI端任务状态枚举，将多个底层状态聚合为UI展示用状态
   */
  public enum TaskStateUI {
    RUNNING(
        new TaskState[]{TaskState.RUNNING}),
    PENDING(new TaskState[]{TaskState.SCHEDULED}),
    COMPLETED(new TaskState[]{TaskState.SUCCEEDED, TaskState.FAILED, TaskState.KILLED});

    private final List<TaskState> correspondingStates;

    private TaskStateUI(TaskState[] correspondingStates) {
      this.correspondingStates = Arrays.asList(correspondingStates);
    }

    /**
     * 判断底层状态是否对应当前UI状态
     * @param state 底层任务状态
     * @return 是否匹配
     */
    public boolean correspondsTo(TaskState state) {
      return this.correspondingStates.contains(state);
    }
  }

  /**
   * 根据单字符标识转换为任务类型
   * @param symbol 单字符标识 m/r
   * @return 任务类型
   */
  public static TaskType taskType(String symbol) {
    // JDK 7 supports switch on strings
    if (symbol.equals("m")) return TaskType.MAP;
    if (symbol.equals("r")) return TaskType.REDUCE;
    throw new YarnRuntimeException("Unknown task symbol: "+ symbol);
  }

  /**
   * 根据状态字符串获取UI任务尝试状态
   * @param attemptStateStr 状态字符串
   * @return UI任务尝试状态枚举
   */
  public static TaskAttemptStateUI taskAttemptState(String attemptStateStr) {
    return TaskAttemptStateUI.valueOf(attemptStateStr);
  }

  /**
   * 根据状态字符串获取UI任务状态
   * @param taskStateStr 状态字符串
   * @return UI任务状态枚举
   */
  public static TaskStateUI taskState(String taskStateStr) {
    return TaskStateUI.valueOf(taskStateStr);
  }

  /**
   * 从配置中获取MapReduce框架的jar包名称
   * @param conf 配置对象
   * @return 框架名称，未配置则返回null
   */
  // gets the base name of the MapReduce framework or null if no
  // framework was configured
  private static String getMRFrameworkName(Configuration conf) {
    String frameworkName = null;
    String framework =
        conf.get(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, "");
    if (!framework.isEmpty()) {
      URI uri;
      try {
        uri = new URI(framework);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Unable to parse '" + framework
            + "' as a URI, check the setting for "
            + MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, e);
      }

      frameworkName = uri.getFragment();
      if (frameworkName == null) {
        frameworkName = new Path(uri).getName();
      }
    }
    return frameworkName;
  }

  /**
   * 将MapReduce框架类路径添加到容器环境变量中
   * @param environment 容器环境变量
   * @param conf 配置对象
   * @throws IOException 设置类路径失败时抛出
   */
  private static void setMRFrameworkClasspath(
      Map<String, String> environment, Configuration conf) throws IOException {
    // 迷你YARN集群场景传播系统类路径
    // Propagate the system classpath when using the mini cluster
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      MRApps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          System.getProperty("java.class.path"), conf);
    }
    boolean crossPlatform =
        conf.getBoolean(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM,
          MRConfig.DEFAULT_MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM);

    // 如果配置了自定义MR框架，只添加MR类路径
    // if the framework is specified then only use the MR classpath
    String frameworkName = getMRFrameworkName(conf);
    if (frameworkName == null) {
      // 添加标准Hadoop类路径
      // Add standard Hadoop classes
      for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          crossPlatform
              ? YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH
              : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        MRApps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          c.trim(), conf);
      }
    }

    boolean foundFrameworkInClasspath = (frameworkName == null);
    for (String c : conf.getStrings(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
        crossPlatform ?
            StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_CROSS_PLATFORM_APPLICATION_CLASSPATH)
            : StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH))) {
      MRApps.addToEnvironment(environment, Environment.CLASSPATH.name(),
        c.trim(), conf);
      if (!foundFrameworkInClasspath) {
        foundFrameworkInClasspath = c.contains(frameworkName);
      }
    }

    if (!foundFrameworkInClasspath) {
      throw new IllegalArgumentException(
          "Could not locate MapReduce framework name '" + frameworkName
          + "' in " + MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH);
    }
    // TODO: Remove duplicates.
  }
  
  /**
   * 设置MapReduce应用容器的类路径环境变量，支持用户类优先配置
   * @param environment 容器环境变量
   * @param conf 配置对象
   * @throws IOException 设置类路径失败时抛出
   */
  @SuppressWarnings("deprecation")
  public static void setClasspath(Map<String, String> environment,
      Configuration conf) throws IOException {
    boolean userClassesTakesPrecedence = 
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);

    String classpathEnvVar =
      conf.getBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false)
        ? Environment.APP_CLASSPATH.name() : Environment.CLASSPATH.name();

    MRApps.addToEnvironment(environment,
      classpathEnvVar, crossPlatformifyMREnv(conf, Environment.PWD), conf);
    if (!userClassesTakesPrecedence) {
      MRApps.setMRFrameworkClasspath(environment, conf);
    }
    /*
     * We use "*" for the name of the JOB_JAR instead of MRJobConfig.JOB_JAR for
     * the case where the job jar is not necessarily named "job.jar". This can
     * happen, for example, when the job is leveraging a resource from the YARN
     * shared cache.
     */
    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + "*", conf);
    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + "classes" + Path.SEPARATOR, conf);
    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        MRJobConfig.JOB_JAR + Path.SEPARATOR + "lib" + Path.SEPARATOR + "*", conf);
    MRApps.addToEnvironment(
        environment,
        classpathEnvVar,
        crossPlatformifyMREnv(conf, Environment.PWD) + Path.SEPARATOR + "*", conf);
    // 通配符只匹配jar文件，因此需要单独添加非jar资源到类路径
    // a * in the classpath will only find a .jar, so we need to filter out
    // all .jars and add everything else
    addToClasspathIfNotJar(JobContextImpl.getFileClassPaths(conf),
        JobContextImpl.getCacheFiles(conf),
        conf,
        environment, classpathEnvVar);
    addToClasspathIfNotJar(JobContextImpl.getArchiveClassPaths(conf),
        JobContextImpl.getCacheArchives(conf),
        conf,
        environment, classpathEnvVar);
    if (userClassesTakesPrecedence) {
      MRApps.setMRFrameworkClasspath(environment, conf);
    }
  }
  
  /**
   * 将非Jar类型的分布式缓存资源添加到类路径中
   * @param paths 需要检查的路径数组
   * @param withLinks 对应资源的URI，包含链接名称信息
   * @param conf 配置对象，用于文件系统解析
   * @param environment 环境变量，用于修改类路径
   * @param classpathEnvVar 类路径对应的环境变量名称
   * @throws IOException 解析路径失败时抛出
   */
  private static void addToClasspathIfNotJar(Path[] paths,
      URI[] withLinks, Configuration conf,
      Map<String, String> environment,
      String classpathEnvVar) throws IOException {
    if (paths != null) {
      HashMap<Path, String> linkLookup = new HashMap<Path, String>();
      if (withLinks != null) {
        for (URI u: withLinks) {
          Path p = new Path(u);
          FileSystem remoteFS = p.getFileSystem(conf);
          String name = p.getName();
          String wildcard = null;

          // 如果路径是通配符，解析父目录并保留通配符标记
          // If the path is wildcarded, resolve its parent directory instead
          if (name.equals(DistributedCache.WILDCARD)) {
            wildcard = name;
            p = p.getParent();
          }

          p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
              remoteFS.getWorkingDirectory()));

          if ((wildcard != null) && (u.getFragment() != null)) {
            throw new IOException("Invalid path URI: " + p + " - cannot "
                + "contain both a URI fragment and a wildcard");
          } else if (wildcard != null) {
            name = p.getName() + Path.SEPARATOR + wildcard;
          } else if (u.getFragment() != null) {
            name = u.getFragment();
          }

          // 如果不是Jar包，添加到查找表中
          // If it's not a JAR, add it to the link lookup.
          if (!StringUtils.toLowerCase(name).endsWith(".jar")) {
            String old = linkLookup.put(p, name);

            if ((old != null) && !name.equals(old)) {
              LOG.warn("The same path is included more than once "
                  + "with different links or wildcards: " + p + "