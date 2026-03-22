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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoExpirationProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeSymlink;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeReferenceSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotDiffSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SnapshotSection;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.VersionInfo;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_MASK;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAMESPACE_EXT_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAME_OFFSET;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAME_MASK;

/**
 * 文件级注释：Protobuf格式fsimage的XML转换工具，遍历Protobuf格式的fsimage结构，将其转换为等价的XML文档输出，用于离线查看和调试fsimage内容
 * PBImageXmlWriter walks over an fsimage structure and writes out
 * an equivalent XML document that contains the fsimage's components.
 */
@InterfaceAudience.Private
public final class PBImageXmlWriter {
  public static final String NAME_SECTION_NAME = "NameSection";
  public static final String ERASURE_CODING_SECTION_NAME =
      "ErasureCodingSection";
  public static final String INODE_SECTION_NAME = "INodeSection";
  public static final String SECRET_MANAGER_SECTION_NAME =
      "SecretManagerSection";
  public static final String CACHE_MANAGER_SECTION_NAME = "CacheManagerSection";
  public static final String SNAPSHOT_DIFF_SECTION_NAME = "SnapshotDiffSection";
  public static final String INODE_REFERENCE_SECTION_NAME =
      "INodeReferenceSection";
  public static final String INODE_DIRECTORY_SECTION_NAME =
      "INodeDirectorySection";
  public static final String FILE_UNDER_CONSTRUCTION_SECTION_NAME =
      "FileUnderConstructionSection";
  public static final String SNAPSHOT_SECTION_NAME = "SnapshotSection";

  public static final String SECTION_ID = "id";
  public static final String SECTION_REPLICATION = "replication";
  public static final String SECTION_PATH = "path";
  public static final String SECTION_NAME = "name";

  public static final String NAME_SECTION_NAMESPACE_ID = "namespaceId";
  public static final String NAME_SECTION_GENSTAMPV1 = "genstampV1";
  public static final String NAME_SECTION_GENSTAMPV2 = "genstampV2";
  public static final String NAME_SECTION_GENSTAMPV1_LIMIT = "genstampV1Limit";
  public static final String NAME_SECTION_LAST_ALLOCATED_BLOCK_ID =
      "lastAllocatedBlockId";
  public static final String NAME_SECTION_TXID = "txid";
  public static final String NAME_SECTION_ROLLING_UPGRADE_START_TIME =
      "rollingUpgradeStartTime";
  public static final String NAME_SECTION_LAST_ALLOCATED_STRIPED_BLOCK_ID =
      "lastAllocatedStripedBlockId";

  public static final String ERASURE_CODING_SECTION_POLICY =
      "erasureCodingPolicy";
  public static final String ERASURE_CODING_SECTION_POLICY_ID =
      "policyId";
  public static final String ERASURE_CODING_SECTION_POLICY_NAME =
      "policyName";
  public static final String ERASURE_CODING_SECTION_POLICY_CELL_SIZE =
      "cellSize";
  public static final String ERASURE_CODING_SECTION_POLICY_STATE =
      "policyState";
  public static final String ERASURE_CODING_SECTION_SCHEMA =
      "ecSchema";
  public static final String ERASURE_CODING_SECTION_SCHEMA_CODEC_NAME =
      "codecName";
  public static final String ERASURE_CODING_SECTION_SCHEMA_DATA_UNITS =
      "dataUnits";
  public static final String ERASURE_CODING_SECTION_SCHEMA_PARITY_UNITS =
      "parityUnits";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTIONS =
      "extraOptions";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTION =
      "option";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTION_KEY =
      "key";
  public static final String ERASURE_CODING_SECTION_SCHEMA_OPTION_VALUE =
      "value";

  public static final String INODE_SECTION_LAST_INODE_ID = "lastInodeId";
  public static final String INODE_SECTION_NUM_INODES = "numInodes";
  public static final String INODE_SECTION_TYPE = "type";
  public static final String INODE_SECTION_MTIME = "mtime";
  public static final String INODE_SECTION_ATIME = "atime";
  public static final String INODE_SECTION_PREFERRED_BLOCK_SIZE =
      "preferredBlockSize";
  public static final String INODE_SECTION_PERMISSION = "permission";
  public static final String INODE_SECTION_BLOCKS = "blocks";
  public static final String INODE_SECTION_BLOCK = "block";
  public static final String INODE_SECTION_GENSTAMP = "genstamp";
  public static final String INODE_SECTION_NUM_BYTES = "numBytes";
  public static final String INODE_SECTION_FILE_UNDER_CONSTRUCTION =
      "file-under-construction";
  public static final String INODE_SECTION_CLIENT_NAME = "clientName";
  public static final String INODE_SECTION_CLIENT_MACHINE = "clientMachine";
  public static final String INODE_SECTION_ACL = "acl";
  public static final String INODE_SECTION_ACLS = "acls";
  public static final String INODE_SECTION_XATTR = "xattr";
  public static final String INODE_SECTION_XATTRS = "xattrs";
  public static final String INODE_SECTION_STORAGE_POLICY_ID =
      "storagePolicyId";
  public static final String INODE_SECTION_BLOCK_TYPE = "blockType";
  public static final String INODE_SECTION_EC_POLICY_ID =
      "erasureCodingPolicyId";
  public static final String INODE_SECTION_NS_QUOTA = "nsquota";
  public static final String INODE_SECTION_DS_QUOTA = "dsquota";
  public static final String INODE_SECTION_TYPE_QUOTA = "typeQuota";
  public static final String INODE_SECTION_QUOTA = "quota";
  public static final String INODE_SECTION_TARGET = "target";
  public static final String INODE_SECTION_NS = "ns";
  public static final String INODE_SECTION_VAL = "val";
  public static final String INODE_SECTION_VAL_HEX = "valHex";
  public static final String INODE_SECTION_INODE = "inode";

  public static final String SECRET_MANAGER_SECTION_CURRENT_ID = "currentId";
  public static final String SECRET_MANAGER_SECTION_TOKEN_SEQUENCE_NUMBER =
      "tokenSequenceNumber";
  public static final String SECRET_MANAGER_SECTION_NUM_DELEGATION_KEYS =
      "numDelegationKeys";
  public static final String SECRET_MANAGER_SECTION_NUM_TOKENS = "numTokens";
  public static final String SECRET_MANAGER_SECTION_EXPIRY = "expiry";
  public static final String SECRET_MANAGER_SECTION_KEY = "key";
  public static final String SECRET_MANAGER_SECTION_DELEGATION_KEY =
      "delegationKey";
  public static final String SECRET_MANAGER_SECTION_VERSION = "version";
  public static final String SECRET_MANAGER_SECTION_OWNER = "owner";
  public static final String SECRET_MANAGER_SECTION_RENEWER = "renewer";
  public static final String SECRET_MANAGER_SECTION_REAL_USER = "realUser";
  public static final String SECRET_MANAGER_SECTION_ISSUE_DATE = "issueDate";
  public static final String SECRET_MANAGER_SECTION_MAX_DATE = "maxDate";
  public static final String SECRET_MANAGER_SECTION_SEQUENCE_NUMBER =
      "sequenceNumber";
  public static final String SECRET_MANAGER_SECTION_MASTER_KEY_ID =
      "masterKeyId";
  public static final String SECRET_MANAGER_SECTION_EXPIRY_DATE = "expiryDate";
  public static final String SECRET_MANAGER_SECTION_TOKEN = "token";

  public static final String CACHE_MANAGER_SECTION_NEXT_DIRECTIVE_ID =
      "nextDirectiveId";
  public static final String CACHE_MANAGER_SECTION_NUM_POOLS = "numPools";
  public static final String CACHE_MANAGER_SECTION_NUM_DIRECTIVES =
      "numDirectives";
  public static final String CACHE_MANAGER_SECTION_POOL_NAME = "poolName";
  public static final String CACHE_MANAGER_SECTION_OWNER_NAME = "ownerName";
  public static final String CACHE_MANAGER_SECTION_GROUP_NAME = "groupName";
  public static final String CACHE_MANAGER_SECTION_MODE = "mode";
  public static final String CACHE_MANAGER_SECTION_LIMIT = "limit";
  public static final String CACHE_MANAGER_SECTION_MAX_RELATIVE_EXPIRY =
      "maxRelativeExpiry";
  public static final String CACHE_MANAGER_SECTION_POOL = "pool";
  public static final String CACHE_MANAGER_SECTION_EXPIRATION = "expiration";
  public static final String CACHE_MANAGER_SECTION_MILLIS = "millis";
  public static final String CACHE_MANAGER_SECTION_RELATIVE = "relative";
  public static final String CACHE_MANAGER_SECTION_DIRECTIVE = "directive";

  public static final String SNAPSHOT_DIFF_SECTION_INODE_ID = "inodeId";
  public static final String SNAPSHOT_DIFF_SECTION_COUNT = "count";
  public static final String SNAPSHOT_DIFF_SECTION_SNAPSHOT_ID = "snapshotId";
  public static final String SNAPSHOT_DIFF_SECTION_CHILDREN_SIZE =
      "childrenSize";
  public static final String SNAPSHOT_DIFF_SECTION_IS_SNAPSHOT_ROOT =
      "isSnapshotRoot";
  public static final String SNAPSHOT_DIFF_SECTION_SNAPSHOT_COPY =
      "snapshotCopy";
  public static final String SNAPSHOT_DIFF_SECTION_CREATED_LIST_SIZE =
      "createdListSize";
  public static final String SNAPSHOT_DIFF_SECTION_DELETED_INODE =
      "deletedInode";
  public static final String SNAPSHOT_DIFF_SECTION_DELETED_INODE_REF =
      "deletedInoderef";
  public static final String SNAPSHOT_DIFF_SECTION_CREATED = "created";
  public static final String SNAPSHOT_DIFF_SECTION_SIZE = "size";
  public static final String SNAPSHOT_DIFF_SECTION_FILE_DIFF_ENTRY =
      "fileDiffEntry";
  public static final String SNAPSHOT_DIFF_SECTION_DIR_DIFF_ENTRY =
      "dirDiffEntry";
  public static final String SNAPSHOT_DIFF_SECTION_FILE_DIFF = "fileDiff";
  public static final String SNAPSHOT_DIFF_SECTION_DIR_DIFF = "dirDiff";

  public static final String INODE_REFERENCE_SECTION_REFERRED_ID = "referredId";
  public static final String INODE_REFERENCE_SECTION_DST_SNAPSHOT_ID =
      "dstSnapshotId";
  public static final String INODE_REFERENCE_SECTION_LAST_SNAPSHOT_ID =
      "lastSnapshotId";
  public static final String INODE_REFERENCE_SECTION_REF = "ref";

  public static final String INODE_DIRECTORY_SECTION_PARENT = "parent";
  public static final String INODE_DIRECTORY_SECTION_CHILD = "child";
  public static final String INODE_DIRECTORY_SECTION_REF_CHILD = "refChild";
  public static final String INODE_DIRECTORY_SECTION_DIRECTORY = "directory";

  public static final String SNAPSHOT_SECTION_SNAPSHOT_COUNTER =
      "snapshotCounter";
  public static final String SNAPSHOT_SECTION_NUM_SNAPSHOTS = "numSnapshots";
  public static final String SNAPSHOT_SECTION_SNAPSHOT_TABLE_DIR =
      "snapshottableDir";
  public static final String SNAPSHOT_SECTION_DIR = "dir";
  public static final String SNAPSHOT_SECTION_ROOT = "root";
  public static final String SNAPSHOT_SECTION_SNAPSHOT = "snapshot";

  private final Configuration conf;
  private final PrintStream out;
  private final SimpleDateFormat isoDateFormat;
  private SerialNumberManager.StringTable stringTable;

  /**
   * 创建UTC时区的ISO格式日期格式化器，用于统一格式化时间戳输出
   * @return 配置好的SimpleDateFormat实例
   */
  public static SimpleDateFormat createSimpleDateFormat() {
    SimpleDateFormat format =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format;
  }

  /**
   * 构造PBImageXmlWriter实例，初始化配置和输出流
   * @param conf Hadoop配置对象
   * @param out 输出XML内容的打印流
   */
  public PBImageXmlWriter(Configuration conf, PrintStream out) {
    this.conf = conf;
    this.out = out;
    this.isoDateFormat = createSimpleDateFormat();
  }

  /**
   * 遍历输入fsimage文件，解析所有section