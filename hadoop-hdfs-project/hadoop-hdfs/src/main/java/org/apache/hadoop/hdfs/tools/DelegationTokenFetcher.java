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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;

import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * DelegationTokenFetcher是HDFS提供的命令行工具，用于从NameNode获取委托令牌，
 * 并支持对令牌进行获取、取消、续签、打印等管理操作，是HDFS安全认证的运维工具
 */
@InterfaceAudience.Private
public class DelegationTokenFetcher {
  private static final String WEBSERVICE = "webservice";
  private static final String CANCEL = "cancel";
  private static final String HELP = "help";
  private static final String HELP_SHORT = "h";
  private static final Logger LOG = LoggerFactory
      .getLogger(DelegationTokenFetcher.class);
  private static final String PRINT = "print";
  private static final String RENEW = "renew";
  private static final String RENEWER = "renewer";
  private static final String VERBOSE = "verbose";

  /**
   * 工具主入口，使用默认HDFS配置处理命令行参数
   * @param args 命令行参数列表
   * @throws Exception 执行失败时抛出异常
   */
  public static void main(final String[] args) throws Exception {
    main(new HdfsConfiguration(), args);
  }

  /**
   * 支持传入自定义配置的主入口，处理命令行解析和业务分发
   * @param conf 用于创建文件系统的配置对象
   * @param args 命令行参数列表
   * @throws Exception 执行失败时抛出异常
   */
  @VisibleForTesting
  public static void main(Configuration conf, final String[] args)
      throws Exception {
    // 初始化命令行选项
    Options fetcherOptions = new Options();
    fetcherOptions
      .addOption(WEBSERVICE, true, "HTTP url to reach the NameNode at")
      .addOption(RENEWER, true, "Name of the delegation token renewer")
      .addOption(CANCEL, false, "cancel the token")
      .addOption(RENEW, false, "renew the token")
      .addOption(PRINT, false, "print the token")
      .addOption(VERBOSE, false, "print verbose output")
      .addOption(HELP_SHORT, HELP, false, "print out help information");

    // 使用Hadoop通用选项解析器解析参数
    GenericOptionsParser parser = new GenericOptionsParser(conf,
            fetcherOptions, args);
    CommandLine cmd = parser.getCommandLine();

    // 解析各个选项参数
    final String webUrl = cmd.hasOption(WEBSERVICE) ? cmd
            .getOptionValue(WEBSERVICE) : null;
    final String renewer = cmd.hasOption(RENEWER) ? cmd.getOptionValue
            (RENEWER) : null;
    final boolean cancel = cmd.hasOption(CANCEL);
    final boolean renew = cmd.hasOption(RENEW);
    final boolean print = cmd.hasOption(PRINT);
    final boolean verbose = cmd.hasOption(VERBOSE);
    final boolean help = cmd.hasOption(HELP);
    String[] remaining = parser.getRemainingArgs();

    // 处理帮助请求
    if (help) {
      printUsage(System.out);
      return;
    }

    // 检查命令参数合法性：只能指定一个操作（取消/续签/打印）
    int commandCount = (cancel ? 1 : 0) + (renew ? 1 : 0) + (print ? 1 : 0);
    if (commandCount > 1) {
      System.err.println("ERROR: Only specify cancel, renew or print.");
      printUsage(System.err);
      return;
    }
    // 检查必须指定一个令牌文件路径
    if (remaining.length != 1 || remaining[0].charAt(0) == '-') {
      System.err.println("ERROR: Must specify exactly one token file");
      printUsage(System.err);
      return;
    }
    // 获取本地文件系统，解析令牌文件路径
    FileSystem local = FileSystem.getLocal(conf);
    final Path tokenFile = new Path(local.getWorkingDirectory(), remaining[0]);

    // 以当前用户身份执行对应操作
    UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        if (print) {
          // 打印令牌信息
          printTokens(conf, tokenFile, verbose);
        } else if (cancel) {
          // 取消令牌
          cancelTokens(conf, tokenFile);
        } else if (renew) {
          // 续签令牌
          renewTokens(conf, tokenFile);
        } else {
          // 默认操作：获取新的委托令牌并保存
          FileSystem fs = getFileSystem(conf, webUrl);
          saveDelegationToken(conf, fs, renewer, tokenFile);
        }
        return null;
      }
    });
  }

  /**
   * 根据配置和URL获取文件系统对象，支持WebHDFS兼容地址转换
   * @param conf 配置对象
   * @param url NameNode地址，可为空使用默认文件系统
   * @return 对应文件系统实例
   * @throws IOException 获取文件系统失败时抛出
   */
  private static FileSystem getFileSystem(Configuration conf, String url)
          throws IOException {
    if (url == null) {
      return FileSystem.get(conf);
    }

    // 向后兼容：将http/https地址转换为WebHDFS模式URI
    URI fsUri = URI.create(
            url.replaceFirst("^http://", WebHdfsConstants.WEBHDFS_SCHEME + "://")
               .replaceFirst("^https://", WebHdfsConstants.SWEBHDFS_SCHEME + "://"));

    return FileSystem.get(fsUri, conf);
  }

  /**
   * 取消令牌文件中所有可管理的委托令牌
   * @param conf 配置对象
   * @param tokenFile 存储令牌的文件路径
   * @throws IOException 读取文件或取消令牌失败时抛出
   * @throws InterruptedException 中断异常
   */
  @VisibleForTesting
  static void cancelTokens(final Configuration conf, final Path tokenFile)
          throws IOException, InterruptedException {
    for (Token<?> token : readTokens(tokenFile, conf)) {
      if (token.isManaged()) {
        token.cancel(conf);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cancelled token for " + token.getService());
        }
      }
    }
  }

  /**
   * 续签令牌文件中所有可管理的委托令牌
   * @param conf 配置对象
   * @param tokenFile 存储令牌的文件路径
   * @throws IOException 读取文件或续签令牌失败时抛出
   * @throws InterruptedException 中断异常
   */
  @VisibleForTesting
  static void renewTokens(final Configuration conf, final Path tokenFile)
          throws IOException, InterruptedException {
    for (Token<?> token : readTokens(tokenFile, conf)) {
      if (token.isManaged()) {
        long result = token.renew(conf);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Renewed token for " + token.getService() + " until: " +
                  new Date(result));
        }
      }
    }
  }

  /**
   * 从指定文件系统获取委托令牌，并保存到本地令牌文件
   * @param conf 配置对象
   * @param fs 目标文件系统（HDFS）
   * @param renewer 令牌续签者用户名
   * @param tokenFile 保存令牌的本地文件路径
   * @throws IOException 获取或保存令牌失败时抛出
   */
  @VisibleForTesting
  static void saveDelegationToken(Configuration conf, FileSystem fs,
                                  final String renewer, final Path tokenFile)
          throws IOException {
    Token<?> token = fs.getDelegationToken(renewer);
    if (null != token) {
      Credentials cred = new Credentials();
      cred.addToken(token.getService(), token);
      // 保持向后兼容性，使用旧的Writable格式存储令牌
      cred.writeTokenStorageFile(tokenFile, conf,
          Credentials.SerializedFormat.WRITABLE);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetched token " + fs.getUri() + " for " +
            token.getService() + " into " + tokenFile);
      }
    } else {
      System.err.println("ERROR: Failed to fetch token from " + fs.getUri());
    }
  }

  /**
   * 将令牌文件中的所有令牌信息格式化为字符串，用于输出
   * @param conf 配置对象
   * @param tokenFile 存储令牌的文件路径
   * @param verbose 是否输出详细信息
   * @return 格式化后的令牌信息字符串
   * @throws IOException 读取令牌文件失败时抛出
   */
  @VisibleForTesting
  public static String printTokensToString(
      final Configuration conf,
      final Path tokenFile,
      final boolean verbose) throws IOException {
    StringBuilder sbld = new StringBuilder();
    final String nl = System.getProperty("line.separator");
    for (Token<?> token : readTokens(tokenFile, conf)) {
      TokenIdentifier tokenId = token.decodeIdentifier();

      String idStr;
      // 针对HDFS委托令牌提供差异化输出
      if (tokenId instanceof DelegationTokenIdentifier) {
        DelegationTokenIdentifier id = (DelegationTokenIdentifier) tokenId;
        idStr = (verbose? id.toString() : id.toStringStable());
      } else {
        idStr = tokenId.toString();
      }
      sbld
          .append("Token (").append(idStr)
          .append(") for ").append(token.getService()).append(nl);
    }
    return sbld.toString();
  }

  /**
   * 将令牌文件中的所有令牌信息打印到标准输出
   * @param conf 配置对象
   * @param tokenFile 存储令牌的文件路径
   * @param verbose 是否输出详细信息
   * @throws IOException 读取令牌文件失败时抛出
   */
  static void printTokens(final Configuration conf,
      final Path tokenFile,
      final boolean verbose) throws IOException {
    System.out.print(printTokensToString(conf, tokenFile, verbose));
  }

  /**
   * 打印工具使用帮助信息，并退出进程
   * @param err 输出帮助信息的流
   */
  private static void printUsage(PrintStream err) {
    err.println("fetchdt retrieves delegation tokens from the NameNode");
    err.println();
    err.println("fetchdt <opts> <token file>");
    err.println("Options:");
    err.println("  --webservice <url>  URL to contact NN on (starts with " +
            "http:// or https://), or other filesystem URL");
    err.println("  --renewer <name>    Name of the delegation token renewer");
    err.println("  --cancel            Cancel the delegation token");
    err.println("  --renew             Renew the delegation token.  " +
            "Delegation " + "token must have been fetched using the --renewer" +
            " <name> option.");
    err.println("  --print [--verbose] Print the delegation token, when " +
            "--verbose is passed, print more information about the token");
    err.println();
    GenericOptionsParser.printGenericCommandUsage(err);
    ExitUtil.terminate(1);
  }

  /**
   * 从本地文件读取所有令牌
   * @param file 存储令牌的凭据文件
   * @param conf 配置对象
   * @return 读取到的所有令牌集合
   * @throws IOException 读取文件失败时抛出
   */
  private static Collection<Token<?>> readTokens(Path file, Configuration conf)
          throws IOException {
    Credentials creds = Credentials.readTokenStorageFile(file, conf);
    return creds.getAllTokens();
  }
}