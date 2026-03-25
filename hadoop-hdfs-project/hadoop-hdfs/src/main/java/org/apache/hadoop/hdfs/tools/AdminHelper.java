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

import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.tools.TableListing;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * @file AdminHelper.java
 * HDFS管理工具公共帮助类，为CacheAdmin、CryptoAdmin、StoragePolicyAdmin等管理工具提供通用辅助方法
 */
public class AdminHelper {
  /**
   * 输出文本最大行宽
   */
  static final int MAX_LINE_WIDTH = 80;
  static final String HELP_COMMAND_NAME = "-help";

  /**
   * 从默认配置获取HDFS分布式文件系统实例
   * @param conf Hadoop配置对象
   * @return HDFS分布式文件系统实例
   * @throws IOException 获取文件系统失败时抛出异常
   */
  public static DistributedFileSystem getDFS(Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    return checkAndGetDFS(fs, conf);
  }

  /**
   * 根据指定URI获取HDFS分布式文件系统实例
   * @param uri 文件系统URI
   * @param conf Hadoop配置对象
   * @return HDFS分布式文件系统实例
   * @throws IOException 获取文件系统失败时抛出异常
   */
  static DistributedFileSystem getDFS(URI uri, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    return checkAndGetDFS(fs, conf);
  }

  /**
   * 检查文件系统类型并返回HDFS实例，支持处理ViewFileSystemOverloadScheme场景
   * @param fs 待检查的文件系统对象
   * @param conf Hadoop配置对象
   * @return 类型检查通过后的HDFS分布式文件系统实例
   * @throws IOException 类型不匹配或获取原始文件系统失败时抛出异常
   */
  static DistributedFileSystem checkAndGetDFS(FileSystem fs, Configuration conf)
      throws IOException {
    if ((fs instanceof ViewFileSystemOverloadScheme)) {
      // ViewFSOverloadScheme场景下，从默认URI获取实际挂载的原始HDFS文件系统
      fs = ((ViewFileSystemOverloadScheme) fs)
          .getRawFileSystem(new Path(FileSystem.getDefaultUri(conf)), conf);
    }
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs.getUri()
          + " is not an HDFS file system. The fs class is: "
          + fs.getClass().getName());
    }
    return (DistributedFileSystem) fs;
  }

  /**
   * 美化NameNode异常信息，提取第一行错误信息，去除冗长栈追踪
   * @param e 原始异常对象
   * @return 美化后的异常字符串
   */
  static String prettifyException(Exception e) {
    if (e.getLocalizedMessage() != null) {
      return e.getClass().getSimpleName() + ": "
          + e.getLocalizedMessage().split("\n")[0];
    } else if (e.getStackTrace() != null && e.getStackTrace().length > 0) {
      return e.getClass().getSimpleName() + " at " + e.getStackTrace()[0];
    } else {
      return e.getClass().getSimpleName();
    }
  }

  /**
   * 创建用于展示命令选项说明的表格对象
   * @return 配置好的选项说明表格
   */
  static TableListing getOptionDescriptionListing() {
    return new TableListing.Builder()
        .addField("").addField("", true)
        .wrapWidth(MAX_LINE_WIDTH).hideHeaders().build();
  }

  /**
   * 解析缓存池TTL（生存时间）字符串转换为毫秒值
   * @param maxTtlString 输入的TTL字符串
   * @return 解析后的TTL毫秒值，never对应永不过期
   * @throws IOException 解析失败时抛出异常
   */
  static Long parseTtlString(String maxTtlString) throws IOException {
    Long maxTtl = null;
    if (maxTtlString != null) {
      if (maxTtlString.equalsIgnoreCase("never")) {
        maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
      } else {
        maxTtl = DFSUtil.parseRelativeTime(maxTtlString);
      }
    }
    return maxTtl;
  }

  /**
   * 解析限制值字符串，支持unlimited表示无限制
   * @param limitString 输入的限制字符串
   * @return 解析后的限制长整型值，unlimited对应无限制常量
   */
  static Long parseLimitString(String limitString) {
    Long limit = null;
    if (limitString != null) {
      if (limitString.equalsIgnoreCase("unlimited")) {
        limit = CachePoolInfo.LIMIT_UNLIMITED;
      } else {
        limit = Long.parseLong(limitString);
      }
    }
    return limit;
  }

  /**
   * 根据命令名称从命令数组匹配对应命令实例，自动处理help命令
   * @param commandName 待匹配的命令名称
   * @param commands 可用命令数组
   * @return 匹配到的命令实例，未匹配返回null
   */
  static Command determineCommand(String commandName, Command[] commands) {
    Preconditions.checkNotNull(commands);
    if (HELP_COMMAND_NAME.equals(commandName)) {
      return new HelpCommand(commands);
    }
    for (Command command : commands) {
      if (command.getName().equals(commandName)) {
        return command;
      }
    }
    return null;
  }

  /**
   * 打印管理工具的使用帮助信息
   * @param longUsage 是否打印详细帮助
   * @param toolName 当前工具名称
   * @param commands 可用命令数组
   */
  static void printUsage(boolean longUsage, String toolName,
      Command[] commands) {
    Preconditions.checkNotNull(commands);
    System.err.println("Usage: bin/hdfs " + toolName + " [COMMAND]");
    final HelpCommand helpCommand = new HelpCommand(commands);
    for (AdminHelper.Command command : commands) {
      if (longUsage) {
        System.err.print(command.getLongUsage());
      } else {
        System.err.print("          " + command.getShortUsage());
      }
    }
    System.err.print(longUsage ? helpCommand.getLongUsage() :
        ("          " + helpCommand.getShortUsage()));
    System.err.println();
  }

  /**
   * 管理工具命令接口，定义所有管理命令需要实现的统一接口
   */
  interface Command {
    /** 获取命令名称 */
    String getName();
    /** 获取命令简短使用说明 */
    String getShortUsage();
    /** 获取命令详细使用说明 */
    String getLongUsage();
    /**
     * 执行命令业务逻辑
     * @param conf Hadoop配置对象
     * @param args 命令参数列表
     * @return 执行结果状态码，0成功，非0失败
     * @throws IOException 执行过程中IO异常抛出
     */
    int run(Configuration conf, List<String> args) throws IOException;
  }

  /**
   * Help命令实现类，负责打印管理工具的帮助信息
   */
  static class HelpCommand implements Command {
    private final Command[] commands;

    /**
     * 构造Help命令，绑定所有可用命令
     * @param commands 所有可用命令数组
     */
    public HelpCommand(Command[] commands) {
      Preconditions.checkNotNull(commands, "commands cannot be null.");
      this.commands = commands;
    }

    @Override
    public String getName() {
      return HELP_COMMAND_NAME;
    }

    @Override
    public String getShortUsage() {
      return "[-help <command-name>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<command-name>", "The command for which to get " +
          "detailed help. If no command is specified, print detailed help for " +
          "all commands");
      return getShortUsage() + "\n" +
          "Get detailed help about a command.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (args.size() == 0) {
        // 无参数时打印所有命令详细帮助
        for (AdminHelper.Command command : commands) {
          System.err.println(command.getLongUsage());
        }
        return 1;
      }
      if (args.size() != 1) {
        System.err.println("You must give exactly one argument to -help.");
        return 1;
      }
      final String commandName = args.get(0);
      // 添加横线前缀匹配命令名称
      final AdminHelper.Command command = AdminHelper
          .determineCommand("-" + commandName, commands);
      if (command == null) {
        // 命令不存在，打印所有可用命令名称
        System.err.print("Unknown command '" + commandName + "'.\n");
        System.err.print("Valid help command names are:\n");
        String separator = "";
        for (AdminHelper.Command c : commands) {
          System.err.print(separator + c.getName().substring(1));
          separator = ", ";
        }
        System.err.print("\n");
        return 1;
      }
      // 打印指定命令的详细帮助
      System.err.print(command.getLongUsage());
      return 0;
    }
  }
}