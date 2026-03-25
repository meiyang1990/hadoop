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
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.Comparator;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Booleans;

/**
 * QJournal JournalNode编辑日志段恢复响应比较器，用于在多JournalNode恢复场景下，
 * 确定正确的编辑日志段长度，选出最优恢复版本。
 * 比较规则基于是否存在段、段是否已完成、epoch版本、事务结束偏移量依次判断。
 */
class SegmentRecoveryComparator
    implements Comparator<Entry<AsyncLogger, PrepareRecoveryResponseProto>> {

  /** 单例实例，供恢复流程复用 */
  static final SegmentRecoveryComparator INSTANCE = new SegmentRecoveryComparator();
  
  /**
   * 比较两个异步日志恢复响应，确定哪个版本更优
   * @param a 第一个异步日志+恢复响应条目
   * @param b 第二个异步日志+恢复响应条目
   * @return 比较结果：负数表示a更优，正数表示b更优，0表示相等
   */
  @Override
  public int compare(
      Entry<AsyncLogger, PrepareRecoveryResponseProto> a,
      Entry<AsyncLogger, PrepareRecoveryResponseProto> b) {
    
    PrepareRecoveryResponseProto r1 = a.getValue();
    PrepareRecoveryResponseProto r2 = b.getValue();
    
    // 存在段状态的响应优于不存在的
    if (r1.hasSegmentState() != r2.hasSegmentState()) {
      return Booleans.compare(r1.hasSegmentState(), r2.hasSegmentState());
    }
    
    if (!r1.hasSegmentState()) {
      // 双方都不存在段状态，视为相等
      return 0;
    }
    
    // 双方都存在段状态，取出段信息
    SegmentStateProto r1Seg = r1.getSegmentState();
    SegmentStateProto r2Seg = r2.getSegmentState();
    
    // 校验两个段必须对应相同的起始事务ID
    Preconditions.checkArgument(r1Seg.getStartTxId() == r2Seg.getStartTxId(),
        "Should only be called with responses for corresponding segments: " +
        "%s and %s do not have the same start txid.", r1, r2);

    // 已完成的段优于进行中的段，已完成段优先级更高
    if (r1Seg.getIsInProgress() != r2Seg.getIsInProgress()) {
      return Booleans.compare(!r1Seg.getIsInProgress(), !r2Seg.getIsInProgress());
    }
    
    if (!r1Seg.getIsInProgress()) {
      // 两个段都已完成，校验结束事务ID必须一致
      if (r1Seg.getEndTxId() != r2Seg.getEndTxId()) {
        throw new AssertionError("finalized segs with different lengths: " + 
            r1 + ", " + r2);
      }
      return 0;
    }
    
    // 两个段都处于进行中，计算各自的最新epoch
    long r1SeenEpoch = Math.max(r1.getAcceptedInEpoch(), r1.getLastWriterEpoch());
    long r2SeenEpoch = Math.max(r2.getAcceptedInEpoch(), r2.getLastWriterEpoch());
    
    // 先比较epoch，epoch越大版本越新；epoch相同再比较结束事务ID，事务ID越长越完整
    return ComparisonChain.start()
        .compare(r1SeenEpoch, r2SeenEpoch)
        .compare(r1.getSegmentState().getEndTxId(), r2.getSegmentState().getEndTxId())
        .result();
  }
}