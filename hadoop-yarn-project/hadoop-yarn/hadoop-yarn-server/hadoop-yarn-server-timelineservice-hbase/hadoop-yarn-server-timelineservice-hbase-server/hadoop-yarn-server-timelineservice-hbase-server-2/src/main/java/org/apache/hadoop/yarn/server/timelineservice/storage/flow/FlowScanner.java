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

package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineServerUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.NumericValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimestampGenerator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 流运行表的HBase协处理器扫描器，在查询流运行数据时被调用。
 * 根据单元格标签对存储的指标进行聚合计算（求和、最小/最大值），同时计算流运行的起止时间范围。
 */
class FlowScanner implements RegionScanner, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlowScanner.class);

  /**
   * 特殊应用ID，用于标识流聚合单元格，因为TimestampGenerator需要解析应用ID生成单元格时间戳。
   */
  private static final String FLOW_APP_ID = "application_00000000000_0000";

  private final Region region;
  private final InternalScanner flowRunScanner;
  private final int batchSize;
  private final long appFinalValueRetentionThreshold;
  private RegionScanner regionScanner;
  private boolean hasMore;
  private byte[] currentRow;
  private List<Cell> availableCells = new ArrayList<>();
  private int currentIndex;
  private FlowScannerOperation action = FlowScannerOperation.READ;

  /**
   * 构造函数，用于无传入Scan的场景。
   * @param env HBase区域协处理器环境
   * @param internalScanner 内部扫描器
   * @param action 扫描操作类型
   */
  FlowScanner(RegionCoprocessorEnvironment env, InternalScanner internalScanner,
      FlowScannerOperation action) {
    this(env, null, internalScanner, action);
  }

  /**
   * 构造函数，初始化扫描器配置。
   * @param env HBase区域协处理器环境
   * @param incomingScan 传入的Scan对象
   * @param internalScanner 内部扫描器
   * @param action 扫描操作类型（读取/刷写/小压缩/大压缩）
   */
  FlowScanner(RegionCoprocessorEnvironment env, Scan incomingScan,
      InternalScanner internalScanner, FlowScannerOperation action) {
    this.batchSize = incomingScan == null ? -1 : incomingScan.getBatch();
    // TODO initialize other scan attributes like Scan#maxResultSize
    this.flowRunScanner = internalScanner;
    if (internalScanner instanceof RegionScanner) {
      this.regionScanner = (RegionScanner) internalScanner;
    }
    this.action = action;
    if (env == null) {
      this.appFinalValueRetentionThreshold =
          YarnConfiguration.DEFAULT_APP_FINAL_VALUE_RETENTION_THRESHOLD;
      this.region = null;
    } else {
      this.region = env.getRegion();
      Configuration hbaseConf = env.getConfiguration();
      this.appFinalValueRetentionThreshold = hbaseConf.getLong(
          YarnConfiguration.APP_FINAL_VALUE_RETENTION_THRESHOLD,
          YarnConfiguration.DEFAULT_APP_FINAL_VALUE_RETENTION_THRESHOLD);
    }
    LOG.debug(" batch size={}", batchSize);
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hbase.regionserver.RegionScanner#getRegionInfo()
   */
  @Override
  public HRegionInfo getRegionInfo() {
    return new HRegionInfo(region.getRegionInfo());
  }

  @Override
  public boolean nextRaw(List<Cell> cells) throws IOException {
    return nextRaw(cells, ScannerContext.newBuilder().build());
  }

  @Override
  public boolean nextRaw(List<Cell> cells, ScannerContext scannerContext)
      throws IOException {
    return nextInternal(cells, scannerContext);
  }

  @Override
  public boolean next(List<Cell> cells) throws IOException {
    return next(cells, ScannerContext.newBuilder().build());
  }

  @Override
  public boolean next(List<Cell> cells, ScannerContext scannerContext)
      throws IOException {
    return nextInternal(cells, scannerContext);
  }

  /**
   * 根据列限定符获取对应的数值转换器，匹配不到返回通用转换器。
   * @param colQualifierBytes 列限定符字节数组
   * @return 匹配到的数值转换器
   */
  private static ValueConverter getValueConverter(byte[] colQualifierBytes) {
    // 先匹配列前缀
    for (FlowRunColumnPrefix colPrefix : FlowRunColumnPrefix.values()) {
      byte[] colPrefixBytes = colPrefix.getColumnPrefixBytes("");
      if (Bytes.compareTo(colPrefixBytes, 0, colPrefixBytes.length,
          colQualifierBytes, 0, colPrefixBytes.length) == 0) {
        return colPrefix.getValueConverter();
      }
    }
    // 前缀匹配不到再匹配完整列
    for (FlowRunColumn column : FlowRunColumn.values()) {
      if (Bytes.compareTo(
          column.getColumnQualifierBytes(), colQualifierBytes) == 0) {
        return column.getValueConverter();
      }
    }
    // 都匹配不到返回通用转换器
    return GenericConverter.getInstance();
  }

  /**
   * 核心内部方法，遍历当前行所有单元格，按列分组后根据聚合操作计算结果，输出处理后的单元格。
   * @param cells 输出结果单元格列表
   * @param scannerContext 扫描上下文
   * @return 是否还有更多行可读取
   * @throws IOException 读取HBase时异常
   */
  private boolean nextInternal(List<Cell> cells, ScannerContext scannerContext)
      throws IOException {
    Cell cell = null;
    startNext();
    ByteArrayComparator comp = new ByteArrayComparator();
    byte[] previousColumnQualifier = Separator.EMPTY_BYTES;
    AggregationOperation currentAggOp = null;
    SortedSet<Cell> currentColumnCells = new TreeSet<>(KeyValue.COMPARATOR);
    Set<String> alreadySeenAggDim = new HashSet<>();
    int addedCnt = 0;
    long currentTimestamp = System.currentTimeMillis();
    ValueConverter converter = null;
    int limit = batchSize;

    // 循环读取单元格直到达到批次限制
    while (limit <= 0 || addedCnt < limit) {
      cell = peekAtNextCell(scannerContext);
      if (cell == null) {
        break;
      }
      byte[] currentColumnQualifier = CellUtil.cloneQualifier(cell);
      if (previousColumnQualifier == null) {
        // 第一次进入循环
        previousColumnQualifier = currentColumnQualifier;
      }

      converter = getValueConverter(currentColumnQualifier);
      // 列变更，输出上一列聚合结果，重置状态处理新列
      if (comp.compare(previousColumnQualifier, currentColumnQualifier) != 0) {
        addedCnt += emitCells(cells, currentColumnCells, currentAggOp,
            converter, currentTimestamp);
        resetState(currentColumnCells, alreadySeenAggDim);
        previousColumnQualifier = currentColumnQualifier;
        currentAggOp = getCurrentAggOp(cell);
        converter = getValueConverter(currentColumnQualifier);
      }
      // 收集当前列的单元格
      collectCells(currentColumnCells, currentAggOp, cell, alreadySeenAggDim,
          converter, scannerContext);
      nextCell(scannerContext);
    }
    // 输出最后一列的聚合结果
    if ((!currentColumnCells.isEmpty()) && ((limit <= 0 || addedCnt < limit))) {
      addedCnt += emitCells(cells, currentColumnCells, currentAggOp, converter,
          currentTimestamp);
      if (LOG.isDebugEnabled()) {
        if (addedCnt > 0) {
          LOG.debug("emitted cells. " + addedCnt + " for " + this.action
              + " rowKey="
              + FlowRunRowKey.parseRowKey(CellUtil.cloneRow(cells.get(0))));
        } else {
          LOG.debug("emitted no cells for " + this.action);
        }
      }
    }
    return hasMore();
  }

  /**
   * 从单元格标签中提取当前列的聚合操作类型。
   * @param cell 待处理单元格
   * @return 聚合操作类型
   */
  private AggregationOperation getCurrentAggOp(Cell cell) {
    List<Tag> tags = HBaseTimelineServerUtils.convertCellAsTagList(cell);
    // 假设同一列所有单元格的聚合操作一致
    return HBaseTimelineServerUtils.getAggregationOperationFromTagsList(tags);
  }

  /**
   * 重置状态，为下一列处理做准备。
   * @param currentColumnCells 当前列单元格集合
   * @param alreadySeenAggDim 已处理聚合维度集合
   */
  private void resetState(SortedSet<Cell> currentColumnCells,
      Set<String> alreadySeenAggDim) {
    currentColumnCells.clear();
    alreadySeenAggDim.clear();
  }

  /**
   * 根据当前聚合操作收集并处理单元格。
   * @param currentColumnCells 当前列单元格集合
   * @param currentAggOp 当前聚合操作
   * @param cell 待收集单元格
   * @param alreadySeenAggDim 已处理聚合维度集合
   * @param converter 数值转换器
   * @param scannerContext 扫描上下文
   * @throws IOException 处理异常
   */
  private void collectCells(SortedSet<Cell> currentColumnCells,
      AggregationOperation currentAggOp, Cell cell,
      Set<String> alreadySeenAggDim, ValueConverter converter,
      ScannerContext scannerContext) throws IOException {

    if (currentAggOp == null) {
      // 无聚合操作，直接保留原单元格
      currentColumnCells.add(cell);
      return;
    }

    switch (currentAggOp) {
    case GLOBAL_MIN:
      // 全局最小值，只保留当前最小的单元格
      if (currentColumnCells.size() == 0) {
        currentColumnCells.add(cell);
      } else {
        Cell currentMinCell = currentColumnCells.first();
        Cell newMinCell = compareCellValues(currentMinCell, cell, currentAggOp,
            (NumericValueConverter) converter);
        if (!currentMinCell.equals(newMinCell)) {
          currentColumnCells.remove(currentMinCell);
          currentColumnCells.add(newMinCell);
        }
      }
      break;
    case GLOBAL_MAX:
      // 全局最大值，只保留当前最大的单元格
      if (currentColumnCells.size() == 0) {
        currentColumnCells.add(cell);
      } else {
        Cell currentMaxCell = currentColumnCells.first();
        Cell newMaxCell = compareCellValues(currentMaxCell, cell, currentAggOp,
            (NumericValueConverter) converter);
        if (!currentMaxCell.equals(newMaxCell)) {
          currentColumnCells.remove(currentMaxCell);
          currentColumnCells.add(newMaxCell);
        }
      }
      break;
    case SUM:
    case SUM_FINAL:
      // 求和聚合，每个聚合维度只保留最新的一个单元格
      if (LOG.isTraceEnabled()) {
        LOG.trace("In collect cells "
            + " FlowSannerOperation="
            + this.action
            + " currentAggOp="
            + currentAggOp
            + " cell qualifier="
            + Bytes.toString(CellUtil.cloneQualifier(cell))
            + " cell value= "
            + converter.decodeValue(CellUtil.cloneValue(cell))
            + " timestamp=" + cell.getTimestamp());
      }

      List<Tag> tags = HBaseTimelineServerUtils.convertCellAsTagList(cell);
      String aggDim = HBaseTimelineServerUtils
          .getAggregationCompactionDimension(tags);
      // 每个聚合维度只保留第一个（最新的）单元格，跳过旧单元格
      if (!alreadySeenAggDim.contains(aggDim)) {
        currentColumnCells.add(cell);
        alreadySeenAggDim.add(aggDim);
      }
      break;
    default:
      break;
    } // end of switch case
  }

  /*
   * 根据聚合操作处理当前列收集到的单元格，将结果输出到结果列表。
   */
  private int emitCells(List<Cell> cells, SortedSet<Cell> currentColumnCells,
      AggregationOperation currentAggOp, ValueConverter converter,
      long currentTimestamp) throws IOException {
    if ((currentColumnCells == null) || (currentColumnCells.size() == 0)) {
      return 0;
    }
    if (currentAggOp == null) {
      cells.addAll(currentColumnCells);
      return currentColumnCells.size();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("In emitCells " + this.action + " currentColumnCells size= "
          + currentColumnCells.size() + " currentAggOp" + currentAggOp);
    }

    switch (currentAggOp) {
    case GLOBAL_MIN:
    case GLOBAL_MAX:
      // 最值已经在收集阶段计算完成，直接输出
      cells.addAll(currentColumnCells);
      return currentColumnCells.size();
    case SUM:
    case SUM_FINAL:
      // 根据操作类型不同处理求和
      switch (action) {
      case FLUSH:
      case MINOR_COMPACTION:
        // 刷写和小压缩直接保留原单元格
        cells.addAll(currentColumnCells);
        return currentColumnCells.size();
      case READ:
        // 读取时实时计算总和，输出一个汇总单元格
        Cell sumCell = processSummation(currentColumnCells,
            (NumericValueConverter) converter);
        cells.add(sumCell);
        return 1;
      case MAJOR_COMPACTION:
        // 大压缩时合并已过期的已完成应用，生成流汇总单元格
        List<Cell> finalCells = processSummationMajorCompaction(
            currentColumnCells, (NumericValueConverter) converter,
            currentTimestamp);
        cells.addAll(finalCells);
        return finalCells.size();
      default:
        cells.addAll(currentColumnCells);
        return currentColumnCells.size();
      }
    default:
      cells.addAll(currentColumnCells);
      return currentColumnCells.size();
    }
  }

  /*
   * 对输入单元格集合求和，生成新的汇总单元格，使用最新单元格的时间戳。
   */
  private Cell processSummation(SortedSet<Cell> currentColumnCells,
      NumericValueConverter converter) throws IOException {
    Number sum = 0;
    Number currentValue = 0;
    long ts = 0L;
    long mostCurrentTimestamp = 0L;
    Cell mostRecentCell = null;
    for (Cell cell : currentColumnCells) {
      currentValue = (Number) converter.decodeValue(CellUtil.cloneValue(cell));
      ts = cell.getTimestamp();
      if (mostCurrentTimestamp < ts) {
        mostCurrentTimestamp = ts;
        mostRecentCell = cell;
      }
      sum = converter.add(sum, currentValue);
    }
    byte[] sumBytes = converter.encodeValue(sum);
    Cell sumCell =
        HBaseTimelineServerUtils.createNewCell(mostRecentCell, sumBytes);
    return sumCell;
  }


  /**
   * 大压缩时处理求和聚合，合并已过期的已完成应用指标，生成流汇总单元格，保留未过期应用的原单元格。
   * @param currentColumnCells 当前列所有收集到的单元格
   * @param converter 数值转换器
   * @param currentTimestamp 当前时间戳
   * @return 处理后的单元格列表
   * @throws IOException 处理异常
   */
  @VisibleForTesting
  List<Cell> processSummationMajorComp