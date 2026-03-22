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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 文件级存储策略管理命令行工具，提供列出、获取、设置、清除存储策略等操作，供管理员通过命令行管理HDFS存储策略。
 * 存储策略用于控制数据块在不同存储介质（如RAM、SSD、DISK、ARCHIVE）上的存放策略。
 */
public class StoragePolicyAdmin extends Configured implements Tool {

  /**
   * 存储策略管理工具主入口方法，启动工具并执行用户命令。
   * @param argsArray 命令行参数数组
   * @throws Exception 执行过程中抛出的任何异常
   */
  public static void main(String[] argsArray) throws Exception {
    final StoragePolicyAdmin admin = new StoragePolicyAdmin(new
        Configuration());
    int res = ToolRunner.run(admin, argsArray);
    System.exit(res);
  }

  /**
   * 构造存储策略管理工具实例，传入Hadoop配置。
   * @param conf Hadoop配置对象
   */
  public StoragePolicyAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      AdminHelper.printUsage(false, "storagepolicies", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    // 解析用户输入的命令
    final AdminHelper.Command command = AdminHelper.determineCommand(args[0],
        COMMANDS);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      AdminHelper.printUsage(false, "storagepolicies", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    // 提取命令剩余参数
    final List<String> argsList = new LinkedList<>();
    argsList.addAll(Arrays.asList(args).subList(1, args.length));
    try {
      // 执行对应命令
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      System.err.println(AdminHelper.prettifyException(e));
      return -1;
    }
  }

  /**
   * 列出集群所有已存在的块存储策略命令实现。
   */
  private static class ListStoragePoliciesCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-listPolicies";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + "]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() + "\n" +
          "List all the existing block storage policies.\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final FileSystem fs = FileSystem.get(conf);
      try {
        // 获取文件系统所有存储策略
        Collection<? extends BlockStoragePolicySpi> policies =
            fs.getAllStoragePolicies();
        System.out.println("Block Storage Policies:");
        // 遍历输出所有存储策略信息
        for (BlockStoragePolicySpi policy : policies) {
          if (policy != null) {
            System.out.println("\t" + policy);
          }
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /**
   * 获取指定文件/目录当前存储策略命令实现。
   */
  private static class GetStoragePolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-getStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>",
          "The path of the file/directory for getting the storage policy");
      return getShortUsage() + "\n" +
          "Get the storage policy of a file/directory.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      // 从参数中提取目标路径
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path with -path.\nUsage: " +
            getLongUsage());
        return 1;
      }

      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        FileStatus status;
        try {
          // 获取目标路径的文件状态
          status = fs.getFileStatus(p);
        } catch (FileNotFoundException e) {
          System.err.println("File/Directory does not exist: " + path);
          return 2;
        }

        // 仅HDFS文件系统支持存储策略查询
        if (status instanceof HdfsFileStatus) {
          // 从文件状态中获取存储策略ID
          byte storagePolicyId = ((HdfsFileStatus)status).getStoragePolicy();
          if (storagePolicyId ==
              HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
            System.out.println("The storage policy of " + path
                + " is unspecified");
            return 0;
          }
          // 匹配存储策略ID找到对应的策略对象
          Collection<? extends BlockStoragePolicySpi> policies =
              fs.getAllStoragePolicies();
          for (BlockStoragePolicySpi policy : policies) {
            if (policy instanceof BlockStoragePolicy) {
              if (((BlockStoragePolicy)policy).getId() == storagePolicyId) {
                System.out.println("The storage policy of " + path
                    + ":\n" + policy);
                return 0;
              }
            }
          }
        }
        // 非HDFS文件系统不支持该操作
        System.err.println(getName() + " is not supported for filesystem "
            + fs.getScheme() + " on path " + path);
        return 2;
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
    }
  }

  /**
   * 为指定文件/目录设置存储策略命令实现。
   */
  private static class SetStoragePolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-setStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path> -policy <policy>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory to set storage" +
          " policy");
      listing.addRow("<policy>", "The name of the block storage policy");
      return getShortUsage() + "\n" +
          "Set the storage policy to a file/directory.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      // 提取目标路径参数
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path for setting the storage " +
            "policy.\nUsage: " + getLongUsage());
        return 1;
      }

      // 提取策略名称参数
      final String policyName = StringUtils.popOptionWithArgument("-policy",
          args);
      if (policyName == null) {
        System.err.println("Please specify the policy name.\nUsage: " +
            getLongUsage());
        return 1;
      }
      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        // 调用文件系统接口设置存储策略
        fs.setStoragePolicy(p, policyName);
        System.out.println("Set storage policy " + policyName + " on " + path);
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /**
   * 触发根据当前存储策略调度块迁移命令实现，让现有数据块符合已设置的存储策略。
   */
  private static class SatisfyStoragePolicyCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-satisfyStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory to satisfy"
          + " storage policy");
      return getShortUsage() + "\n" +
          "Schedule blocks to move based on file/directory policy.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      // 提取目标路径参数
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path for setting the storage " +
            "policy.\nUsage: " + getLongUsage());
        return 1;
      }
      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        // 调用文件系统接口触发块迁移调度
        fs.satisfyStoragePolicy(p);
        System.out.println("Scheduled blocks to move based on the current"
            + " storage policy on " + path);
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /**
   * 清除指定文件/目录已设置存储策略命令实现，清除后继承父目录策略。
   */
  private static class UnsetStoragePolicyCommand
      implements AdminHelper.Command {

    @Override
    public String getName() {
      return "-unsetStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory "
          + "from which the storage policy will be unset.");
      return getShortUsage() + "\n"
          + "Unset the storage policy set for a file/directory.\n\n"
          + listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      // 提取目标路径参数
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path from which "
            + "the storage policy will be unset.\nUsage: " + getLongUsage());
        return 1;
      }

      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        // 调用文件系统接口清除存储策略
        fs.unsetStoragePolicy(p);
        System.out.println("Unset storage policy from " + path);
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  // 所有支持的存储策略管理命令数组
  private static final AdminHelper.Command[] COMMANDS = {
      new ListStoragePoliciesCommand(),
      new SetStoragePolicyCommand(),
      new GetStoragePolicyCommand(),
      new UnsetStoragePolicyCommand(),
      new SatisfyStoragePolicyCommand()
  };
}