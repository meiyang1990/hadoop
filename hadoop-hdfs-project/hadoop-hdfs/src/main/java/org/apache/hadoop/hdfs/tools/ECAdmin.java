// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ECTopologyVerifierResult;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.NoECPolicySetException;
import org.apache.hadoop.hdfs.util.ECPolicyLoader;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * HDFS EC（纠删码）管理命令行工具，提供纠删码策略的增删改查、启用禁用和集群拓扑验证功能。
 * 属于HDFS客户端工具，供管理员管理集群纠缩码配置。
 */
@InterfaceAudience.Private
public class ECAdmin extends Configured implements Tool {

  public static final String NAME = "ec";

  /**
   * ECAdmin工具主入口方法，启动命令行工具执行用户命令。
   * @param args 命令行输入参数
   * @throws Exception 执行过程中所有异常都会抛出，由JVM处理
   */
  public static void main(String[] args) throws Exception {
    final ECAdmin admin = new ECAdmin(new Configuration());
    int res = ToolRunner.run(admin, args);
    System.exit(res);
  }

  /**
   * 构造ECAdmin实例，使用给定配置初始化。
   * @param conf Hadoop配置对象
   */
  public ECAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      // 无参数，打印帮助信息
      AdminHelper.printUsage(false, NAME, COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    // 根据第一个参数解析匹配对应的命令对象
    final AdminHelper.Command command = AdminHelper.determineCommand(args[0],
        COMMANDS);
    if (command == null) {
      // 未找到匹配命令，输出错误和帮助信息
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      AdminHelper.printUsage(false, NAME, COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    // 提取命令参数部分，去除命令名
    final List<String> argsList = new LinkedList<>();
    argsList.addAll(Arrays.asList(args).subList(1, args.length));
    try {
      // 执行对应命令
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      // 参数解析错误，输出格式化后的异常信息
      System.err.println(AdminHelper.prettifyException(e));
      return -1;
    }
  }

  /** 列出集群中所有已启用的纠删码策略 */
  private static class ListECPoliciesCommand
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
          "Get the list of all erasure coding policies.\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        final Collection<ErasureCodingPolicyInfo> policies =
            dfs.getAllErasureCodingPolicies();
        if (policies.isEmpty()) {
          System.out.println("There is no erasure coding policies in the " +
              "cluster.");
        } else {
          System.out.println("Erasure Coding Policies:");
          for (ErasureCodingPolicyInfo policy : policies) {
            if (policy != null) {
              System.out.println(policy);
            }
          }
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** 从用户指定的XML文件添加自定义纠删码策略到集群 */
  private static class AddECPoliciesCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-addPolicies";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -policyFile <file>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<file>",
          "The path of the xml file which defines the EC policies to add");
      return getShortUsage() + "\n" +
          "Add a list of user defined erasure coding policies.\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String filePath =
          StringUtils.popOptionWithArgument("-policyFile", args);
      if (filePath == null) {
        System.err.println("Please specify the path with -policyFile.\nUsage: "
            + getLongUsage());
        return 1;
      }

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        List<ErasureCodingPolicy> policies =
            new ECPolicyLoader().loadPolicy(filePath);
        if (policies.size() > 0) {
          AddErasureCodingPolicyResponse[] responses =
              dfs.addErasureCodingPolicies(
            policies.toArray(new ErasureCodingPolicy[policies.size()]));
          for (AddErasureCodingPolicyResponse response : responses) {
            System.out.println(response);
          }
        } else {
          System.out.println("No EC policy parsed out from " + filePath);
        }

      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** 查询指定文件或目录当前生效的纠删码策略 */
  private static class GetECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-getPolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>",
          "The path of the file/directory for getting the erasure coding " +
              "policy");
      return getShortUsage() + "\n" +
          "Get the erasure coding policy of a file/directory.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path with -path.\nUsage: " +
            getLongUsage());
        return 1;
      }

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final Path p = new Path(path);
      final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), conf);
      try {
        ErasureCodingPolicy ecPolicy = dfs.getErasureCodingPolicy(p);
        if (ecPolicy != null) {
          System.out.println(ecPolicy.getName());
        } else {
          System.out.println("The erasure coding policy of " + path + " is " +
              "unspecified");
        }
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** 从集群中删除指定的用户自定义纠删码策略 */
  private static class RemoveECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-removePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -policy <policy>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<policy>", "The name of the erasure coding policy");
      return getShortUsage() + "\n" +
          "Remove an user defined erasure coding policy.\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String ecPolicyName = StringUtils.popOptionWithArgument(
          "-policy", args);
      if (ecPolicyName == null) {
        System.err.println("Please specify the policy name.\nUsage: " +
            getLongUsage());
        return 1;
      }
      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }
      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.removeErasureCodingPolicy(ecPolicyName);
        System.out.println("Erasure coding policy " + ecPolicyName +
            "is removed");
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** 为指定文件/目录设置纠删码策略 */
  private static class SetECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-setPolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() +
          " -path <path> [-policy <policy>] [-replicate]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory to set " +
          "the erasure coding policy");
      listing.addRow("<policy>", "The name of the erasure coding policy");
      listing.addRow("-replicate",
          "force 3x replication scheme on the directory");
      return getShortUsage() + "\n" +
          "Set the erasure coding policy for a file/directory.\n\n" +
          listing.toString() + "\n" +
          "-replicate and -policy are optional arguments. They cannot been " +
          "used at the same time.\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path for setting the EC " +
            "policy.\nUsage: " + getLongUsage());
        return 1;
      }

      String ecPolicyName = StringUtils.popOptionWithArgument("-policy",
          args);
      final boolean replicate = StringUtils.popOption("-replicate", args);

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      if (replicate) {
        if (ecPolicyName != null) {
          System.err.println(getName() +
              ": -replicate and -policy cannot been used at the same time");
          return 2;
        }
        ecPolicyName = ErasureCodeConstants.REPLICATION_POLICY_NAME;
      }

      final Path p = new Path(path);
      final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), conf);
      try {
        dfs.setErasureCodingPolicy(p, ecPolicyName);
        if (ecPolicyName == null){
          ecPolicyName = "default";
        }
        System.out.println("Set " + ecPolicyName + " erasure coding policy on" +
            " " + path);
        RemoteIterator<FileStatus> dirIt = dfs.listStatusIterator(p);
        if (dirIt.hasNext()) {
          // 提示用户：非空目录设置策略不会自动转换已有文件
          System.out.println("Warning: setting erasure coding policy on a " +
              "non-empty directory will not automatically convert existing " +
              "files to " + ecPolicyName + " erasure coding policy");
        }
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 3;
      }
      return 0;
    }
  }

  /** 取消指定目录已设置的纠删码策略，继承父目录策略 */
  private static class UnsetECPolicyCommand
      implements AdminHelper.Command {

    @Override
    public String getName() {
      return "-unsetPolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the directory "
          + "from which the erasure coding policy will be unset.");
      return getShortUsage() + "\n"
          + "Unset the erasure coding policy for a directory.\n\n"
          + listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify a path.\nUsage: " + getLongUsage());
        return 1;
      }

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final Path p = new Path(path);
      final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), conf);
      try {
        dfs.unsetErasureCodingPolicy(p);
        System.out.println("Unset erasure coding policy from " + path);
        RemoteIterator<FileStatus> dirIt = dfs.listStatusIterator(p);
        if (dirIt.hasNext()) {
          // 提示用户：非空目录取消策略不会自动转换已有文件
          System.out.println("Warning: unsetting erasure coding policy on a " +
              "non-empty directory will not automatically convert existing" +
              " files to replicated data.");
        }
      } catch (NoECPolicySetException e) {
        System.err.println(AdminHelper.prettifyException(e));
        System.err.println("Use '-setPolicy -path <PATH> -replicate' to enforce"
            + " default replication policy irrespective of EC policy"
            + " defined on parent.");
        return 2;
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** 列出集群支持的所有纠删码编解码器及其实现 */
  private static class ListECCodecsCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-listCodecs";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + "]\n";