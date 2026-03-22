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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.util.ProcessTree;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 文件功能：MapReduce任务日志管理工具类，负责任务日志的本地存储、索引、同步和读取，
 * 支持YARN MRv2和旧版MRv1两种日志目录结构，提供日志截断、尾截取功能
 * A simple logger to handle the task-specific user logs.
 * This class uses the system property <code>hadoop.log.dir</code>.
 * 
 */
@InterfaceAudience.Private
public class TaskLog {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TaskLog.class);

  static final String USERLOGS_DIR_NAME = "userlogs";

  private static final File LOG_DIR = 
    new File(getBaseLogDir(), USERLOGS_DIR_NAME).getAbsoluteFile();
  
  // localFS is set in (and used by) writeToIndexFile()
  static LocalFileSystem localFS = null;
  
  /**
   * 获取YARN MRv2容器日志目录路径，从系统属性读取
   * @return YARN容器日志目录绝对路径
   */
  public static String getMRv2LogDir() {
    return System.getProperty(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR);
  }
  
  /**
   * 根据任务尝试ID和日志类型，获取日志文件路径，兼容MRv1和MRv2两种格式
   * @param taskid 任务尝试ID
   * @param isCleanup 是否是清理尝试
   * @param filter 日志类型
   * @return 日志文件对象
   */
  public static File getTaskLogFile(TaskAttemptID taskid, boolean isCleanup,
      LogName filter) {
    if (getMRv2LogDir() != null) {
      return new File(getMRv2LogDir(), filter.toString());
    } else {
      return new File(getAttemptDir(taskid, isCleanup), filter.toString());
    }
  }

  /**
   * 获取日志文件实际存储位置，通过索引文件解析偏移位置
   * @param taskid 任务尝试ID
   * @param isCleanup 是否是清理尝试
   * @param filter 日志类型
   * @return 实际日志文件对象，解析失败返回null
   */
  static File getRealTaskLogFileLocation(TaskAttemptID taskid,
      boolean isCleanup, LogName filter) {
    LogFileDetail l;
    try {
      l = getLogFileDetail(taskid, filter, isCleanup);
    } catch (IOException ie) {
      LOG.error("getTaskLogFileDetail threw an exception " + ie);
      return null;
    }
    return new File(l.location, filter.toString());
  }

  /**
   * 存储日志文件元信息，包含日志实际存储目录、起始偏移和长度
   */
  private static class LogFileDetail {
    final static String LOCATION = "LOG_DIR:";
    /**日志实际存储目录*/
    String location;
    /**日志起始偏移*/
    long start;
    /**日志长度*/
    long length;
  }
  
  /**
   * 从日志索引文件解析指定日志的元信息（位置、起始偏移、长度）
   * @param taskid 任务尝试ID
   * @param filter 日志类型
   * @param isCleanup 是否是清理尝试
   * @return 解析后的日志元信息
   * @throws IOException 读取索引文件失败抛出异常
   */
  private static LogFileDetail getLogFileDetail(TaskAttemptID taskid, 
                                                LogName filter,
                                                boolean isCleanup) 
  throws IOException {
    File indexFile = getIndexFile(taskid, isCleanup);
    BufferedReader fis = new BufferedReader(new InputStreamReader(
      SecureIOUtils.openForRead(indexFile, obtainLogDirOwner(taskid), null),
      StandardCharsets.UTF_8));
    //索引文件格式如下：
    //LOG_DIR: <日志实际存储目录路径>
    //stdout:<stdout起始偏移> <stdout长度>
    //stderr:<stderr起始偏移> <stderr长度>
    //syslog:<syslog起始偏移> <syslog长度>
    LogFileDetail l = new LogFileDetail();
    String str = null;
    try {
      str = fis.readLine();
      if (str == null) { // 索引文件为空
        throw new IOException("Index file for the log of " + taskid
            + " doesn't exist.");
      }
      l.location = str.substring(str.indexOf(LogFileDetail.LOCATION)
          + LogFileDetail.LOCATION.length());
      // debugout和profile.out特殊处理：JVM重用禁用时每个任务独占，直接读取整个文件
      if (filter.equals(LogName.DEBUGOUT) || filter.equals(LogName.PROFILE)) {
        l.length = new File(l.location, filter.toString()).length();
        l.start = 0;
        fis.close();
        return l;
      }
      str = fis.readLine();
      while (str != null) {
        // 查找匹配当前日志类型的行
        if (str.contains(filter.toString())) {
          str = str.substring(filter.toString().length() + 1);
          String[] startAndLen = str.split(" ");
          l.start = Long.parseLong(startAndLen[0]);
          l.length = Long.parseLong(startAndLen[1]);
          break;
        }
        str = fis.readLine();
      }
      fis.close();
      fis = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, fis);
    }
    return l;
  }
  
  /**
   * 获取临时索引文件路径，用于保证索引更新原子性
   * @param taskid 任务尝试ID
   * @param isCleanup 是否是清理尝试
   * @return 临时索引文件对象
   */
  private static File getTmpIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.tmp");
  }

  /**
   * 获取日志索引文件路径
   * @param taskid 任务尝试ID
   * @param isCleanup 是否是清理尝试
   * @return 索引文件对象
   */
  static File getIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.index");
  }

  /**
   * Obtain the owner of the log dir. This is 
   * determined by checking the job's log directory.
   * 获取日志目录所有者用户名，通过作业日志目录的权限信息获取
   * @param taskid 任务尝试ID
   * @return 日志目录所有者用户名
   * @throws IOException 获取文件状态失败抛出异常
   */
  static String obtainLogDirOwner(TaskAttemptID taskid) throws IOException {
    Configuration conf = new Configuration();
    FileSystem raw = FileSystem.getLocal(conf).getRaw();
    Path jobLogDir = new Path(getJobDir(taskid.getJobID()).getAbsolutePath());
    FileStatus jobStat = raw.getFileStatus(jobLogDir);
    return jobStat.getOwner();
  }

  /**
   * 获取基础日志目录，从系统属性hadoop.log.dir读取
   * @return 基础日志目录路径
   */
  static String getBaseLogDir() {
    return System.getProperty("hadoop.log.dir");
  }

  /**
   * 获取单个任务尝试的日志目录路径
   * @param taskid 任务尝试ID
   * @param isCleanup 是否是清理尝试
   * @return 任务尝试日志目录对象
   */
  static File getAttemptDir(TaskAttemptID taskid, boolean isCleanup) {
    String cleanupSuffix = isCleanup ? ".cleanup" : "";
    return new File(getJobDir(taskid.getJobID()), taskid + cleanupSuffix);
  }
  /**上一次写入索引时stdout文件长度*/
  private static long prevOutLength;
  /**上一次写入索引时stderr文件长度*/
  private static long prevErrLength;
  /**上一次写入索引时syslog文件长度*/
  private static long prevLogLength;
  
  /**
   * 原子写入日志索引文件，先写入临时文件再重命名保证原子性
   * @param logLocation 日志实际存储目录
   * @param isCleanup 是否是清理尝试
   * @throws IOException 写入文件失败抛出异常
   */
  private static synchronized 
  void writeToIndexFile(String logLocation,
                        boolean isCleanup) throws IOException {
    // To ensure atomicity of updates to index file, write to temporary index
    // file first and then rename.
    File tmpIndexFile = getTmpIndexFile(currentTaskid, isCleanup);

    BufferedOutputStream bos = null;
    DataOutputStream dos = null;
    try{
      bos = new BufferedOutputStream(
          SecureIOUtils.createForWrite(tmpIndexFile, 0644));
      dos = new DataOutputStream(bos);
      //索引文件格式如下：
      //LOG_DIR: <日志实际存储目录路径>
      //STDOUT: <stdout起始偏移> <stdout增量长度>
      //STDERR: <stderr起始偏移> <stderr增量长度>
      //SYSLOG: <syslog起始偏移> <syslog增量长度>   

      dos.writeBytes(LogFileDetail.LOCATION + logLocation + "\n"
          + LogName.STDOUT.toString() + ":");
      dos.writeBytes(Long.toString(prevOutLength) + " ");
      dos.writeBytes(Long.toString(new File(logLocation, LogName.STDOUT
          .toString()).length() - prevOutLength)
          + "\n" + LogName.STDERR + ":");
      dos.writeBytes(Long.toString(prevErrLength) + " ");
      dos.writeBytes(Long.toString(new File(logLocation, LogName.STDERR
          .toString()).length() - prevErrLength)
          + "\n" + LogName.SYSLOG.toString() + ":");
      dos.writeBytes(Long.toString(prevLogLength) + " ");
      dos.writeBytes(Long.toString(new File(logLocation, LogName.SYSLOG
          .toString()).length() - prevLogLength)
          + "\n");
      dos.close();
      dos = null;
      bos.close();
      bos = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dos, bos);
    }

    File indexFile = getIndexFile(currentTaskid, isCleanup);
    Path indexFilePath = new Path(indexFile.getAbsolutePath());
    Path tmpIndexFilePath = new Path(tmpIndexFile.getAbsolutePath());

    if (localFS == null) {// 延迟初始化本地文件系统，只初始化一次
      localFS = FileSystem.getLocal(new Configuration());
    }
    // 重命名原子替换索引文件
    localFS.rename (tmpIndexFilePath, indexFilePath);
  }

  /**
   * 重置三个日志文件当前长度，用于新任务切换时初始化索引
   * @param logLocation 日志存储目录
   */
  private static void resetPrevLengths(String logLocation) {
    prevOutLength = new File(logLocation, LogName.STDOUT.toString()).length();
    prevErrLength = new File(logLocation, LogName.STDERR.toString()).length();
    prevLogLength = new File(logLocation, LogName.SYSLOG.toString()).length();
  }
  /**当前正在执行的任务尝试ID*/
  private volatile static TaskAttemptID currentTaskid = null;

  @SuppressWarnings("unchecked")
  /**
   * 同步当前任务日志到索引文件，刷新标准输出和错误输出流，更新索引信息
   * @param logLocation 日志存储目录
   * @param taskid 当前任务尝试ID
   * @param isCleanup 是否是清理尝试
   * @throws IOException 写入索引失败抛出异常
   */
  public synchronized static void syncLogs(String logLocation, 
                                           TaskAttemptID taskid,
                                           boolean isCleanup) 
  throws IOException {
    System.out.flush();
    System.err.flush();
    if (currentTaskid != taskid) {
      currentTaskid = taskid;
      resetPrevLengths(logLocation);
    }
    writeToIndexFile(logLocation, isCleanup);
  }

  /**
   * 日志同步关闭钩子，关闭同步调度器，刷新流和日志追加器，保证日志全部写出
   * @param scheduler 日志同步调度器
   */
  public static synchronized void syncLogsShutdown(
    ScheduledExecutorService scheduler) 
  {
    // 刷新标准输出错误流
    System.out.flush();
    System.err.flush();

    if (scheduler != null) {
      scheduler.shutdownNow();
    }

    // 关闭所有日志追加器，刷新缓冲区
    LogManager.shutdown(); 
  }

  @SuppressWarnings("unchecked")
  /**
   * 手动同步所有日志，刷新标准流和所有日志追加器的缓冲区
   */
  public static synchronized void syncLogs() {
    // 刷新标准输出错误流
    System.out.flush();
    System.err.flush();

    // 刷新所有可刷新的日志追加器
    final Logger rootLogger = Logger.getRootLogger();
    flushAppenders(rootLogger);
    final Enumeration<Logger> allLoggers = rootLogger.getLoggerRepository().
      getCurrentLoggers();
    while (allLoggers.hasMoreElements()) {
      final Logger l = allLoggers.nextElement();
      flushAppenders(l);
    }
  }

  @SuppressWarnings("unchecked")
  /**
   * 刷新指定日志记录器下所有可刷新的追加器缓冲区
   * @param l 日志记录器
   */
  private static void flushAppenders(Logger l) {
    final Enumeration<Appender> allAppenders = l.getAllAppenders();
    while (allAppenders.hasMoreElements()) {
      final Appender a = allAppenders.nextElement();
      if (a instanceof Flushable) {
        try {
          ((Flushable) a).flush();
        } catch (IOException ioe) {
          System.err.println(a + ": Failed to flush!"
            + StringUtils.stringifyException(ioe));
        }
      }
    }
  }

  /**
   * 创建定时日志同步器，定期刷新日志缓冲区保证日志实时性，注册JVM关闭钩子
   * @return 定时日志同步调度器
   */
  public static ScheduledExecutorService createLogSyncer() {
    final ScheduledExecutorService scheduler =
        HadoopExecutors.newSingleThreadScheduledExecutor(
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName("Thread for syncLogs");
                return t;
              }
            });
    // 注册JVM关闭钩子，关闭时刷新日志
    ShutdownHookManager.get().addShutdownHook(new Runnable() {
      @Override
      public void run() {
        TaskLog.syncLogsShutdown(scheduler);
      }
    }, 50);
    // 每5秒执行一次日志同步，初始延迟0秒
    scheduler.scheduleWithFixedDelay(
        new Runnable() {
          @Override
          public void run() {
            TaskLog.syncLogs();
          }
        }, 0L, 5L, TimeUnit.SECONDS);
    return scheduler;
  }

  /**
   * 任务日志类型枚举，定义MapReduce任务生成的各类日志
   */
  @InterfaceAudience.Private
  public enum LogName {
    /** 任务标准输出日志 */
    STDOUT ("stdout"),

    /** 任务标准错误