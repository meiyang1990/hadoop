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

package org.apache.hadoop.mapreduce.security.token;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * MapReduce作业令牌标识符，用于标识MapReduce作业的身份认证令牌
 * 存储作业ID信息，用于作业运行过程中的服务间身份认证
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobTokenIdentifier extends TokenIdentifier {
  private Text jobid;
  public final static Text KIND_NAME = new Text("mapreduce.job");
  
  /**
   * 空构造方法，用于反序列化
   */
  public JobTokenIdentifier() {
    this.jobid = new Text();
  }

  /**
   * 根据作业ID构造作业令牌标识符
   * @param jobid 目标作业ID
   */
  public JobTokenIdentifier(Text jobid) {
    this.jobid = jobid;
  }

  /** {@inheritDoc} */
  @Override
  public Text getKind() {
    return KIND_NAME;
  }
  
  /** {@inheritDoc} */
  @Override
  public UserGroupInformation getUser() {
    if (jobid == null || "".equals(jobid.toString())) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(jobid.toString());
  }
  
  /**
   * 获取当前令牌对应的作业ID
   * @return 作业ID
   */
  public Text getJobId() {
    return jobid;
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    jobid.readFields(in);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    jobid.write(out);
  }

  /**
   * 作业令牌 renewer 实现，使用默认的 TrivialRenewer 逻辑处理令牌续租
   */
  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}