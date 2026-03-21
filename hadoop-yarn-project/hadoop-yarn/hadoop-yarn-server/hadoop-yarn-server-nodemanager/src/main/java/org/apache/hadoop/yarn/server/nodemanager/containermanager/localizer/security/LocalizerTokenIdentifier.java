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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * 本地化服务令牌标识符，用于NM与本地化服务之间的RPC身份认证
 */
public class LocalizerTokenIdentifier extends TokenIdentifier {

  /** 令牌类型标识 */
  public static final Text KIND = new Text("Localizer");

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeInt(1);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    in.readInt();
  }

  @Override
  public Text getKind() {
    // TODO Auto-generated method stub
    return KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    // TODO Auto-generated method stub
    // 返回测试用远程用户
    return UserGroupInformation.createRemoteUser("testing");
  }

}