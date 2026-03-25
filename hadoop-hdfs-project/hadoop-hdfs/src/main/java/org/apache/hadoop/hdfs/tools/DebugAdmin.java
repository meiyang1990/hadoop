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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.impl.BlockReaderRemote;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HDFS命令行调试工具，提供多种底层调试操作。
 * 所有操作仅用于调试目的，API和功能可能在不同HDFS版本间变更或移除。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DebugAdmin extends Configured implements Tool {
  /**
   * 所有支持的调试命令列表
   */
  private final DebugCommand[] DEBUG_COMMANDS = {
      new VerifyMetaCommand(),
      new ComputeMetaCommand(),
      new RecoverLeaseCommand(),
      new VerifyECCommand(),
      new HelpCommand()
  };

  /**
   * 所有调试命令的抽象基类，定义命令通用结构
   */
  private abstract static class DebugCommand {
    final String name;
    final String usageText;
    final String helpText;

    DebugCommand(String name, String usageText, String helpText) {
      this.name = name;
      this.usageText = usageText;
      this.helpText = helpText;
    }

    /**
     * 执行调试命令
     * @param args 命令参数列表
     * @return 执行结果，0表示成功，非0表示失败
     * @throws IOException 执行过程中的IO异常
     */
    abstract int run(List<String> args) throws IOException;
  }

  private static int HEADER_LEN = 7;

  /**
   * 验证块元数据文件和数据块文件的校验和命令
   */
  private static class VerifyMetaCommand extends DebugCommand {
    VerifyMetaCommand() {
      super("verifyMeta",
          "verifyMeta -meta <metadata-file> [-block <block-file>]",
          "  Verify HDFS metadata and block files.  If a block file is specified, we" +
              System.lineSeparator() +
              "  will verify that the checksums in the metadata file match the block" +
              System.lineSeparator() +
              "  file.");
    }

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      // 解析命令行参数
      String blockFile = StringUtils.popOptionWithArgument("-block", args);
      String metaFile = StringUtils.popOptionWithArgument("-meta", args);
      if (metaFile == null) {
        System.err.println("You must specify a meta file with -meta");
        return 1;
      }

      FileInputStream metaStream = null, dataStream = null;
      FileChannel metaChannel = null, dataChannel = null;
      DataInputStream checksumStream = null;
      try {
        BlockMetadataHeader header;
        try {
          // 打开元数据文件并读取头部
          metaStream = new FileInputStream(metaFile);
          checksumStream = new DataInputStream(metaStream);
          header = BlockMetadataHeader.readHeader(checksumStream);
          metaChannel = metaStream.getChannel();
          // 跳过头部长度，定位到校验和数据
          metaChannel.position(HEADER_LEN);
        } catch (RuntimeException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        } catch (IOException e) {
          System.err.println("Failed to read HDFS metadata file header for " +
              metaFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        DataChecksum checksum = header.getChecksum();
        System.out.println("Checksum type: " + checksum.toString());
        // 如果没有指定块文件，只验证元数据头部即可
        if (blockFile == null) {
          return 0;
        }
        ByteBuffer metaBuf, dataBuf;
        try {
          // 打开数据块文件，分配读写缓冲区
          dataStream = new FileInputStream(blockFile);
          dataChannel = dataStream.getChannel();
          final int CHECKSUMS_PER_BUF = 1024 * 32;
          metaBuf = ByteBuffer.allocate(checksum.
              getChecksumSize() * CHECKSUMS_PER_BUF);
          dataBuf = ByteBuffer.allocate(checksum.
              getBytesPerChecksum() * CHECKSUMS_PER_BUF);
        } catch (IOException e) {
          System.err.println("Failed to open HDFS block file for " +
              blockFile + ": " + StringUtils.stringifyException(e));
          return 1;
        }
        long offset = 0;
        // 分块读取并验证校验和
        while (true) {
          dataBuf.clear();
          int dataRead = -1;
          try {
            dataRead = dataChannel.read(dataBuf);
            // 读取到文件末尾，结束验证
            if (dataRead < 0) {
              break;
            }
          } catch (IOException e) {
            System.err.println("Got I/O error reading block file " +
                blockFile + "from disk at offset " + dataChannel.position() +
                ": " + StringUtils.stringifyException(e));
            return 1;
          }
          try {
            // 计算当前批次需要读取的校验和大小
            int csumToRead =
                (((checksum.getBytesPerChecksum() - 1) + dataRead) /
                  checksum.getBytesPerChecksum()) *
                      checksum.getChecksumSize();
            metaBuf.clear();
            metaBuf.limit(csumToRead);
            metaChannel.read(metaBuf);
            // 翻转缓冲区准备读取
            dataBuf.flip();
            metaBuf.flip();
          } catch (IOException e) {
            System.err.println("Got I/O error reading metadata file " +
                metaFile + "from disk at offset " + metaChannel.position() +
                ": " +  StringUtils.stringifyException(e));
            return 1;
          }
          try {
            // 验证数据块与元数据中的校验和是否匹配
            checksum.verifyChunkedSums(dataBuf, metaBuf,
                blockFile, offset);
          } catch (IOException e) {
            System.out.println("verifyChunkedSums error: " +
                StringUtils.stringifyException(e));
            return 1;
          }
          offset += dataRead;
        }
        System.out.println("Checksum verification succeeded on block file " +
            blockFile);
        return 0;
      } finally {
        // 清理所有打开的流
        IOUtils.cleanupWithLogger(null, metaStream, dataStream, checksumStream);
      }
    }
  }

  /**
   * 根据数据块文件重新计算并生成元数据校验文件的命令
   */
  private static class ComputeMetaCommand extends DebugCommand {
    ComputeMetaCommand() {
      super("computeMeta",
          "computeMeta -block <block-file> -out <output-metadata-file>",
          "  Compute HDFS metadata from the specified block file, and save it"
              + " to" + System.lineSeparator()
              + "  the specified output metadata file."
              + System.lineSeparator() + System.lineSeparator()
              + "**NOTE: Use at your own risk!" + System.lineSeparator()
              + " If the block file is corrupt"
              + " and you overwrite it's meta file, " + System.lineSeparator()
              + " it will show up"
              + " as good in HDFS, but you can't read the data."
              + System.lineSeparator()
              + " Only use as a last measure, and when you are 100% certain"
              + " the block file is good.");
    }

    /**
     * 根据校验选项创建DataChecksum实例
     * @param opt 校验选项，包含类型和每个校验和覆盖的字节数
     * @return 新建的DataChecksum实例
     */
    private DataChecksum createChecksum(Options.ChecksumOpt opt) {
      DataChecksum dataChecksum = DataChecksum
          .newDataChecksum(opt.getChecksumType(), opt.getBytesPerChecksum());
      if (dataChecksum == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid checksum type: userOpt=" + opt + ", default=" + opt
                + ", effective=null");
      }
      return dataChecksum;
    }

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      // 解析命令行参数
      final String name = StringUtils.popOptionWithArgument("-block", args);
      if (name == null) {
        System.err.println("You must specify a block file with -block");
        return 2;
      }
      final File blockFile = new File(name);
      if (!blockFile.exists() || !blockFile.isFile()) {
        System.err.println("Block file <" + name + "> does not exist "
            + "or is not a file");
        return 3;
      }
      final String outFile = StringUtils.popOptionWithArgument("-out", args);
      if (outFile == null) {
        System.err.println("You must specify a output file with -out");
        return 4;
      }
      final File srcMeta = new File(outFile);
      if (srcMeta.exists()) {
        System.err.println("output file already exists!");
        return 5;
      }

      DataOutputStream metaOut = null;
      try {
        // 初始化校验对象
        final Configuration conf = new Configuration();
        final Options.ChecksumOpt checksumOpt =
            DfsClientConf.getChecksumOptFromConf(conf);
        final DataChecksum checksum = createChecksum(checksumOpt);

        final int smallBufferSize = DFSUtilClient.getSmallBufferSize(conf);
        // 创建输出元数据文件，写入头部
        metaOut = new DataOutputStream(
            new BufferedOutputStream(Files.newOutputStream(srcMeta.toPath()),
                smallBufferSize));
        BlockMetadataHeader.writeHeader(metaOut, checksum);
        metaOut.close();
        metaOut = null;
        // 计算校验和并写入元数据文件
        FsDatasetUtil.computeChecksum(
            srcMeta, srcMeta, blockFile, smallBufferSize, conf);
        System.out.println(
            "Checksum calculation succeeded on block file " + name
                + " saved metadata to meta file " + outFile);
        return 0;
      } finally {
        IOUtils.cleanupWithLogger(null, metaOut);
      }
    }
  }

  /**
   * 恢复指定文件租约的调试命令，用于处理文件被客户端锁定但客户端已故障的场景
   */
  private class RecoverLeaseCommand extends DebugCommand {
    RecoverLeaseCommand() {
      super("recoverLease",
"recoverLease -path <path> [-retries <num-retries>]",
"  Recover the lease on the specified path.  The path must reside on an" +
    System.lineSeparator() +
"  HDFS filesystem.  The default number of retries is 1.");
    }

    private static final int TIMEOUT_MS = 5000;

    int run(List<String> args) throws IOException {
      if (args.size() == 0) {
        System.out.println(usageText);
        System.out.println(helpText + System.lineSeparator());
        return 1;
      }
      // 解析命令行参数
      String pathStr = StringUtils.popOptionWithArgument("-path", args);
      String retriesStr = StringUtils.popOptionWithArgument("-retries", args);
      if (pathStr == null) {
        System.err.println("You must supply a -path argument to " +
            "recoverLease.");
        return 1;
      }
      int maxRetries = 1;
      if (retriesStr != null) {
        try {
          maxRetries = Integer.parseInt(retriesStr);
        } catch (NumberFormatException e) {
          System.err.println("Failed to parse the argument to -retries: " +
              StringUtils.stringifyException(e));
          return 1;
        }
      }
      FileSystem fs;
      try {
        // 获取文件系统实例
        fs = FileSystem.newInstance(new URI(pathStr), getConf(), null);
      } catch (URISyntaxException e) {
        System.err.println("URISyntaxException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      } catch (InterruptedException e) {
        System.err.println("InterruptedException for " + pathStr + ":" +
            StringUtils.stringifyException(e));
        return 1;
      }
      DistributedFileSystem dfs = null;
      try {
        dfs = (DistributedFileSystem) fs;
      } catch (ClassCastException e) {
        System.err.println("Invalid filesystem for path " + pathStr + ": " +
            "needed scheme hdfs, but got: " + fs.getScheme());
        return 1;
      }
      // 循环重试恢复租约
      for (int retry = 0; true; ) {
        boolean recovered = false;
        IOException ioe = null;
        try {
          recovered = dfs.recoverLease(new Path(pathStr));
        } catch (FileNotFoundException e) {
          System.err.println("recoverLease got exception: " + e.getMessage());
          System.err.println("Giving up on recoverLease for " + pathStr +
              " after 1 try");
          return 1;
        } catch (IOException e) {
          ioe = e;
        }
        if (recovered) {
          System.out.println("recoverLease SUCCEEDED on " + pathStr); 
          return 0;
        }
        if (ioe != null) {
          System.err.println("recoverLease got exception: " +
              ioe.getMessage());
        } else {
          System.err.println("recoverLease returned false.");
        }
        retry++;
        // 达到最大重试次数