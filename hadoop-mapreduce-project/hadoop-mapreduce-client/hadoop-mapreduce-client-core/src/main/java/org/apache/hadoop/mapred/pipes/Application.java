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

package org.apache.hadoop.mapred.pipes;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.crypto.SecretKey;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipes框架中负责启动用户可执行任务进程、并与该进程通信的核心管理类
 * 为C/C++等非Java语言编写的MapReduce任务提供进程间通信支撑
 * @param <K1> Map输入键类型
 * @param <V1> Map输入值类型
 * @param <K2> Reduce输出键类型
 * @param <V2> Reduce输出值类型
 */
class Application<K1 extends WritableComparable, V1 extends Writable,
                  K2 extends WritableComparable, V2 extends Writable> {
  private static final Logger LOG =
      LoggerFactory.getLogger(Application.class.getName());
  /** 服务端Socket，用于接收用户任务进程的连接 */
  private ServerSocket serverSocket;
  /** 空闲Ping连接清理线程 */
  private PingSocketCleaner socketCleaner;
  /** 启动的用户任务子进程 */
  private Process process;
  /** 与用户任务进程通信的客户端Socket */
  private Socket clientSocket;
  /** 输出处理器，处理用户任务进程返回的输出数据 */
  private OutputHandler<K2, V2> handler;
  /** 下行协议对象，用于向用户任务进程发送命令 */
  private DownwardProtocol<K1, V1> downlink;
  /** 标识当前系统是否为Windows */
  static final boolean WINDOWS
  = System.getProperty("os.name").startsWith("Windows");

  /**
   * 构造并启动用户任务子进程，完成与子进程的连接建立和身份认证
   * @param conf 任务配置对象
   * @param recordReader 记录读取器，用于更新任务进度
   * @param output 输出收集器，用于收集用户任务输出并写入Hadoop
   * @param reporter 任务Reporter，用于上报进度和状态
   * @param outputKeyClass 输出键的类型
   * @param outputValueClass 输出值的类型
   * @throws IOException
   * @throws InterruptedException
   */
  Application(JobConf conf, 
              RecordReader<FloatWritable, NullWritable> recordReader, 
              OutputCollector<K2,V2> output, Reporter reporter,
              Class<? extends K2> outputKeyClass,
              Class<? extends V2> outputValueClass
              ) throws IOException, InterruptedException {
    // 绑定随机端口，供子进程连接
    serverSocket = new ServerSocket(0);
    Map<String, String> env = new HashMap<String,String>();
    // 设置临时目录环境变量，使用Java的临时目录
    env.put("TMPDIR", System.getProperty("java.io.tmpdir"));
    // 将监听端口写入环境变量，供子进程获取连接
    env.put(Submitter.PORT, 
            Integer.toString(serverSocket.getLocalPort()));
    
    // 安全启用时，将作业令牌密码写入本地文件供子进程使用
    Token<JobTokenIdentifier> jobToken = TokenCache.getJobToken(conf
        .getCredentials());
    // 该密码作为Java进程和C++子进程的共享密钥
    byte[]  password = jobToken.getPassword();
    String localPasswordFile = new File(".") + Path.SEPARATOR
        + "jobTokenPassword";
    writePasswordToLocalFile(localPasswordFile, password, conf);
    // FIXME This doesn't seem to be read anywhere
    env.put("hadoop_pipes_shared_secret_location", localPasswordFile);
 
    List<String> cmd = new ArrayList<String>();
    String interpretor = conf.get(Submitter.INTERPRETOR);
    // 如果配置了解释器（如Python），添加到命令行
    if (interpretor != null) {
      cmd.add(interpretor);
    }
    // 获取分布式缓存中第一个文件，即为用户可执行程序
    String executable = JobContextImpl.getLocalCacheFiles(conf)[0].toString();
    // 如果可执行文件没有执行权限，添加执行权限
    if (!FileUtil.canExecute(new File(executable))) {
      // LinuxTaskController已经默认给了执行权限，这里主要处理DefaultTaskController的情况
      FileUtil.chmod(executable, "u+x");
    }
    cmd.add(executable);
    // 包装命令，捕获子进程的标准输出和错误输出到任务日志
    TaskAttemptID taskid = 
      TaskAttemptID.forName(conf.get(MRJobConfig.TASK_ATTEMPT_ID));
    File stdout = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDERR);
    long logLength = TaskLog.getTaskLogLength(conf);
    cmd = TaskLog.captureOutAndError(null, cmd, stdout, stderr, logLength,
                                     false);
    
    // 启动子进程
    process = runClient(cmd, env);
    // 接受子进程连接
    clientSocket = serverSocket.accept();
    // 启动空闲Ping连接清理线程，处理闲置连接
    int soTimeout = conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
    socketCleaner = new PingSocketCleaner("ping-socket-cleaner", serverSocket,
                                          soTimeout);
    socketCleaner.setDaemon(true);
    socketCleaner.start();
    
    // 生成安全挑战，完成双向认证
    String challenge = getSecurityChallenge();
    String digestToSend = createDigest(password, challenge);
    String digestExpected = createDigest(password, digestToSend);
    
    // 初始化输出处理器
    handler = new OutputHandler<K2, V2>(output, reporter, recordReader, 
        digestExpected);
    K2 outputKey = (K2)
      ReflectionUtils.newInstance(outputKeyClass, conf);
    V2 outputValue = (V2) 
      ReflectionUtils.newInstance(outputValueClass, conf);
    // 初始化二进制协议处理器
    downlink = new BinaryProtocol<K1, V1, K2, V2>(clientSocket, handler, 
                                  outputKey, outputValue, conf);
    
    // 发送认证信息
    downlink.authenticate(digestToSend, challenge);
    // 等待认证完成
    waitForAuthentication();
    LOG.debug("Authentication succeeded");
    // 启动协议处理线程
    downlink.start();
    // 发送作业配置给子进程
    downlink.setJobConf(conf);
  }

  /**
   * 生成随机安全挑战字符串，用于身份认证
   * @return 随机生成的挑战字符串
   */
  private String getSecurityChallenge() {
    Random rand = new Random(System.currentTimeMillis());
    // 使用4个随机整数生成16字节随机数据
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(rand.nextInt(0x7fffffff));
    strBuilder.append(rand.nextInt(0x7fffffff));
    strBuilder.append(rand.nextInt(0x7fffffff));
    strBuilder.append(rand.nextInt(0x7fffffff));
    return strBuilder.toString();
  }

  /**
   * 将作业令牌密码写入本地权限受限文件，供子进程读取用于认证
   * @param localPasswordFile 本地文件路径
   * @param password 令牌密码字节数组
   * @param conf 作业配置对象
   * @throws IOException
   */
  private void writePasswordToLocalFile(String localPasswordFile,
      byte[] password, JobConf conf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Path localPath = new Path(localPasswordFile);
    FSDataOutputStream out = FileSystem.create(localFs, localPath,
        new FsPermission("400"));
    out.write(password);
    out.close();
  }

  /**
   * 获取向用户任务进程发送命令的下行协议对象
   * @return 下行协议代理对象
   */
  DownwardProtocol<K1, V1> getDownlink() {
    return downlink;
  }
  
  /**
   * 等待子进程完成身份认证响应
   * @throws IOException
   * @throws InterruptedException
   */
  void waitForAuthentication() throws IOException,
      InterruptedException {
    downlink.flush();
    LOG.debug("Waiting for authentication response");
    handler.waitForAuthentication();
  }
  
  /**
   * 等待用户任务进程执行完成
   * @return 任务是否正常完成
   * @throws Throwable
   */
  boolean waitForFinish() throws Throwable {
    downlink.flush();
    return handler.waitForFinish();
  }

  /**
   * 中止用户任务进程，并清理资源
   * @param t 导致中止的异常
   * @throws IOException 包装后的异常抛出
   */
  void abort(Throwable t) throws IOException {
    LOG.info("Aborting because of " + StringUtils.stringifyException(t));
    try {
      downlink.abort();
      downlink.flush();
    } catch (IOException e) {
      // 清理阶段忽略IO异常
    }
    try {
      handler.waitForFinish();
    } catch (Throwable ignored) {
      // 等待失败则直接销毁进程
      process.destroy();
    }
    IOException wrapper = new IOException("pipe child exception");
    wrapper.initCause(t);
    throw wrapper;      
  }
  
  /**
   * 清理子进程和Socket资源
   * @throws IOException
   */
  void cleanup() throws IOException {
    serverSocket.close();
    try {
      downlink.close();
      socketCleaner.interrupt();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }      
  }

  /**
   * 在子进程中执行指定命令，并转发输出到当前进程
   * @param command 命令及参数列表
   * @param env 子进程环境变量
   * @return 启动后的进程句柄
   * @throws IOException
   */
  static Process runClient(List<String> command, 
                           Map<String, String> env) throws IOException {
    ProcessBuilder builder = new ProcessBuilder(command);
    if (env != null) {
      builder.environment().putAll(env);
    }
    Process result = builder.start();
    return result;
  }
  
  /**
   * 使用共享密钥对输入数据计算消息摘要，用于身份认证
   * @param password 共享密钥字节数组
   * @param data 待计算摘要的输入数据
   * @return 计算得到的摘要字符串
   * @throws IOException
   */
  public static String createDigest(byte[] password, String data)
      throws IOException {
    SecretKey key = JobTokenSecretManager.createSecretKey(password);
    return SecureShuffleUtils.hashFromString(data, key);
  }

  /**
   * 空闲Ping连接清理线程，负责清理ServerSocket上接受的闲置连接
   * 处理额外的连接请求，避免连接泄漏
   */
  @VisibleForTesting
  public static class PingSocketCleaner extends SubjectInheritingThread {
    private final ServerSocket serverSocket;
    private final int soTimeout;

    /**
     * 构造Ping连接清理线程
     * @param name 线程名称
     * @param serverSocket 待监听的服务端Socket
     * @param soTimeout Socket读取超时时间
     */
    PingSocketCleaner(String name, ServerSocket serverSocket, int soTimeout) {
      super(name);
      this.serverSocket = serverSocket;
      this.soTimeout = soTimeout;
    }

    @Override
    public void work() {
      LOG.info("PingSocketCleaner started...");
      while (!Thread.currentThread().isInterrupted()) {
        Socket clientSocket = null;
        try {
          // 接受新连接
          clientSocket = serverSocket.accept();
          // 设置读取超时
          clientSocket.setSoTimeout(soTimeout);
          LOG.debug("Connection received from {}",
                    clientSocket.getInetAddress());
          int readData = 0;
          // 读取到流结束
          while (readData != -1) {
            readData = clientSocket.getInputStream().read();
          }
          LOG.debug("close socket cause client has closed.");
          closeSocketInternal(clientSocket);
        } catch (IOException exception) {
          LOG.error("PingSocketCleaner exception", exception);
        } finally {
          // 确保连接被关闭
          closeSocketInternal(clientSocket);
        }
      }
    }

    /**
     * 关闭指定Socket连接
     * @param clientSocket 待关闭的Socket
     */
    @VisibleForTesting
    protected void closeSocketInternal(Socket clientSocket) {
      IOUtils.closeSocket(clientSocket);
    }
  }
}