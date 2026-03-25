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
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.text.WordUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.tools.TableListing.Justification;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.ToolRunner;

/**
 * HDFS缓存管理命令行工具，提供对HDFS集中式缓存的缓存指令和缓存池的增删改查操作
 * 
 * 该工具属于HDFS命令行工具，供管理员管理HDFS缓存，支持添加/删除/修改缓存指令和缓存池、
 * 列出缓存指令和缓存池信息与统计数据等功能。
 */
@InterfaceAudience.Private
public class CacheAdmin extends Configured implements Tool {

  public CacheAdmin() {
    this(null);
  }

  public CacheAdmin(Configuration conf) {
    super(conf);
  }

  /**
   * 工具主入口方法，解析命令行参数并分发对应命令执行
   * @param args 命令行参数
   * @return 执行结果状态码，0成功，非0失败
   * @throws IOException 访问HDFS时可能抛出IO异常
   */
  @Override
  public int run(String[] args) throws IOException {
    if (args.length == 0) {
      AdminHelper.printUsage(false, "cacheadmin", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    AdminHelper.Command command = AdminHelper.determineCommand(args[0],
        COMMANDS);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      AdminHelper.printUsage(false, "cacheadmin", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    List<String> argsList = new LinkedList<String>();
    for (int j = 1; j < args.length; j++) {
      argsList.add(args[j]);
    }
    try {
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      System.err.println(AdminHelper.prettifyException(e));
      return -1;
    }
  }

  /**
   *  CacheAdmin命令行工具主方法，启动工具并执行命令
   * @param argsArray 命令行参数
   * @throws Exception 执行过程中可能抛出各种异常
   */
  public static void main(String[] argsArray) throws Exception {
    CacheAdmin cacheAdmin = new CacheAdmin(new Configuration());
    int res = ToolRunner.run(cacheAdmin, argsArray);
    System.exit(res);
  }

  /**
   * 解析缓存指令过期时间字符串，转换为Expiration对象
   * @param ttlString 输入的过期时间字符串，支持never或带单位的时长格式
   * @return 解析后的Expiration对象
   * @throws IOException 解析时长格式错误时抛出IO异常
   */
  private static CacheDirectiveInfo.Expiration parseExpirationString(String ttlString)
      throws IOException {
    CacheDirectiveInfo.Expiration ex = null;
    if (ttlString != null) {
      if (ttlString.equalsIgnoreCase("never")) {
        ex = CacheDirectiveInfo.Expiration.NEVER;
      } else {
        long ttl = DFSUtil.parseRelativeTime(ttlString);
        ex = CacheDirectiveInfo.Expiration.newRelative(ttl);
      }
    }
    return ex;
  }

  /**
   * 添加缓存指令命令实现类，负责处理-addDirective命令，向HDFS缓存添加指定路径
   */
  private static class AddCacheDirectiveInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-addDirective";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() +
          " -path <path> -pool <pool-name> " +
          "[-force] " +
          "[-replication <replication>] [-ttl <time-to-live>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "A path to cache. The path can be " +
          "a directory or a file.");
      listing.addRow("<pool-name>", "The pool to which the directive will be " +
          "added. You must have write permission on the cache pool "
          + "in order to add new directives.");
      listing.addRow("-force",
          "Skips checking of cache pool resource limits.");
      listing.addRow("<replication>", "The cache replication factor to use. " +
          "Defaults to 1.");
      listing.addRow("<time-to-live>", "How long the directive is " +
          "valid. Can be specified in minutes, hours, and days, e.g. " +
          "30m, 4h, 2d. Valid units are [smhd]." +
          " \"never\" indicates a directive that never expires." +
          " If unspecified, the directive never expires.");
      return getShortUsage() + "\n" +
        "Add a new cache directive.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();

      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }
      builder.setPath(new Path(path));

      String poolName = StringUtils.popOptionWithArgument("-pool", args);
      if (poolName == null) {
        System.err.println("You must specify a pool name with -pool.");
        return 1;
      }
      builder.setPool(poolName);
      boolean force = StringUtils.popOption("-force", args);
      String replicationString =
          StringUtils.popOptionWithArgument("-replication", args);
      if (replicationString != null) {
        Short replication = Short.parseShort(replicationString);
        builder.setReplication(replication);
      }

      String ttlString = StringUtils.popOptionWithArgument("-ttl", args);
      try {
        Expiration ex = parseExpirationString(ttlString);
        if (ex != null) {
          builder.setExpiration(ex);
        }
      } catch (IOException e) {
        System.err.println(
            "Error while parsing ttl value: " + e.getMessage());
        return 1;
      }

      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }

      DistributedFileSystem dfs =
          AdminHelper.getDFS(new Path(path).toUri(), conf);
      CacheDirectiveInfo directive = builder.build();
      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      if (force) {
        flags.add(CacheFlag.FORCE);
      }
      try {
        long id = dfs.addCacheDirective(directive, flags);
        System.out.println("Added cache directive " + id);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }

      return 0;
    }
  }

  /**
   * 删除缓存指令命令实现类，负责处理-removeDirective命令，根据ID删除指定缓存指令
   */
  private static class RemoveCacheDirectiveInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-removeDirective";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " <id>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<id>", "The id of the cache directive to remove.  " + 
        "You must have write permission on the pool of the " +
        "directive in order to remove it.  To see a list " +
        "of cache directive IDs, use the -listDirectives command.");
      return getShortUsage() + "\n" +
        "Remove a cache directive.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String idString= StringUtils.popFirstNonOption(args);
      if (idString == null) {
        System.err.println("You must specify a directive ID to remove.");
        return 1;
      }
      long id;
      try {
        id = Long.parseLong(idString);
      } catch (NumberFormatException e) {
        System.err.println("Invalid directive ID " + idString + ": expected " +
            "a numeric value.");
        return 1;
      }
      if (id <= 0) {
        System.err.println("Invalid directive ID " + id + ": ids must " +
            "be greater than 0.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.getClient().removeCacheDirective(id);
        System.out.println("Removed cached directive " + id);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /**
   * 修改缓存指令命令实现类，负责处理-modifyDirective命令，修改已有缓存指令的属性
   */
  private static class ModifyCacheDirectiveInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-modifyDirective";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() +
          " -id <id> [-path <path>] [-force] [-replication <replication>] " +
          "[-pool <pool-name>] [-ttl <time-to-live>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<id>", "The ID of the directive to modify (required)");
      listing.addRow("<path>", "A path to cache. The path can be " +
          "a directory or a file. (optional)");
      listing.addRow("-force",
          "Skips checking of cache pool resource limits.");
      listing.addRow("<replication>", "The cache replication factor to use. " +
          "(optional)");
      listing.addRow("<pool-name>", "The pool to which the directive will be " +
          "added. You must have write permission on the cache pool "
          + "in order to move a directive into it. (optional)");
      listing.addRow("<time-to-live>", "How long the directive is " +
          "valid. Can be specified in minutes, hours, and days, e.g. " +
          "30m, 4h, 2d. Valid units are [smhd]." +
          " \"never\" indicates a directive that never expires.");
      return getShortUsage() + "\n" +
        "Modify a cache directive.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      CacheDirectiveInfo.Builder builder =
        new CacheDirectiveInfo.Builder();
      boolean modified = false;
      String idString = StringUtils.popOptionWithArgument("-id", args);
      if (idString == null) {
        System.err.println("You must specify a directive ID with -id.");
        return 1;
      }
      builder.setId(Long.parseLong(idString));
      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path != null) {
        builder.setPath(new Path(path));
        modified = true;
      }
      boolean force = StringUtils.popOption("-force", args);
      String replicationString =
        StringUtils.popOptionWithArgument("-replication", args);
      if (replicationString != null) {
        builder.setReplication(Short.parseShort(replicationString));
        modified = true;
      }
      String poolName =
        StringUtils.popOptionWithArgument("-pool", args);
      if (poolName != null) {
        builder.setPool(poolName);
        modified = true;
      }
      String ttlString = StringUtils.popOptionWithArgument("-ttl", args);
      try {
        Expiration ex = parseExpirationString(ttlString);
        if (ex != null) {
          builder.setExpiration(ex);
          modified = true;
        }
      } catch (IOException e) {
        System.err.println(
            "Error while parsing ttl value: " + e.getMessage());
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      if (!modified) {
        System.err.println("No modifications were specified.");
        return 1;
      }
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      if (force) {
        flags.add(CacheFlag.FORCE);
      }
      try {
        dfs.modifyCacheDirective(builder.build(), flags);
        System.out.println("Modified cache directive " + idString);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /**
   * 批量删除缓存指令命令实现类，负责处理-removeDirectives命令，删除指定路径下所有缓存指令
   */
  private static class RemoveCacheDirectiveInfosCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-removeDirectives";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("-path <path>", "The path of the cache directives to remove.  " +
        "You must have write permission on the pool of the directive in order " +
        "to remove it.  To see a list of cache directives, use the " +
        "-listDirectives command.");
      return getShortUsage() + "\n" +
        "Remove every cache directive with the specified path.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        System.err.println("Usage is " + getShortUsage());
        return