#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hadoop YARN Server 核心架构设计文档生成脚本
基于 release-3.3.5-RC0 版本源码分析
"""

import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, grey
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, Image
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Line, Rect, String, Group
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics import renderPDF

# ============================================================
# 字体注册
# ============================================================
def register_fonts():
    """注册中文字体"""
    font_paths = [
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/Supplemental/Songti.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
    ]
    registered = False
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont('ChineseFont', fp))
                pdfmetrics.registerFont(TTFont('ChineseFontBold', fp))
                registered = True
                break
            except Exception:
                continue

    if not registered:
        try:
            pdfmetrics.registerFont(TTFont('ChineseFont', 'STHeiti Light.ttc'))
            pdfmetrics.registerFont(TTFont('ChineseFontBold', 'STHeiti Light.ttc'))
        except Exception:
            print("WARNING: Chinese fonts not found, using Helvetica")
            pdfmetrics.registerAlias('ChineseFont', 'Helvetica')
            pdfmetrics.registerAlias('ChineseFontBold', 'Helvetica-Bold')

register_fonts()

# ============================================================
# 样式定义
# ============================================================
styles = getSampleStyleSheet()

DARK_BLUE = HexColor('#1a237e')
MEDIUM_BLUE = HexColor('#1565c0')
LIGHT_BLUE = HexColor('#e3f2fd')
ACCENT_GREEN = HexColor('#2e7d32')
ACCENT_ORANGE = HexColor('#e65100')
TABLE_HEADER_BG = HexColor('#1565c0')
TABLE_ALT_ROW = HexColor('#f5f5f5')
BORDER_COLOR = HexColor('#bdbdbd')

style_title = ParagraphStyle('DocTitle', fontName='ChineseFontBold', fontSize=28,
    leading=36, alignment=TA_CENTER, textColor=DARK_BLUE, spaceAfter=6*mm)
style_subtitle = ParagraphStyle('DocSubtitle', fontName='ChineseFont', fontSize=14,
    leading=20, alignment=TA_CENTER, textColor=HexColor('#616161'), spaceAfter=10*mm)
style_h1 = ParagraphStyle('H1', fontName='ChineseFontBold', fontSize=20,
    leading=28, textColor=DARK_BLUE, spaceBefore=12*mm, spaceAfter=6*mm,
    borderWidth=0, borderPadding=0,
    leftIndent=0)
style_h2 = ParagraphStyle('H2', fontName='ChineseFontBold', fontSize=16,
    leading=22, textColor=MEDIUM_BLUE, spaceBefore=8*mm, spaceAfter=4*mm)
style_h3 = ParagraphStyle('H3', fontName='ChineseFontBold', fontSize=13,
    leading=18, textColor=ACCENT_GREEN, spaceBefore=6*mm, spaceAfter=3*mm)
style_body = ParagraphStyle('Body', fontName='ChineseFont', fontSize=10.5,
    leading=17, alignment=TA_JUSTIFY, spaceBefore=1*mm, spaceAfter=2*mm)
style_code = ParagraphStyle('Code', fontName='Courier', fontSize=9,
    leading=13, leftIndent=10*mm, backColor=HexColor('#f5f5f5'),
    borderWidth=0.5, borderColor=BORDER_COLOR, borderPadding=4)
style_bullet = ParagraphStyle('Bullet', fontName='ChineseFont', fontSize=10.5,
    leading=16, leftIndent=12*mm, bulletIndent=6*mm, spaceBefore=1*mm, spaceAfter=1*mm)
style_toc = ParagraphStyle('TOC', fontName='ChineseFont', fontSize=12,
    leading=20, leftIndent=8*mm, spaceBefore=2*mm, spaceAfter=2*mm, textColor=MEDIUM_BLUE)
style_table_header = ParagraphStyle('TH', fontName='ChineseFontBold', fontSize=10,
    leading=14, alignment=TA_CENTER, textColor=white)
style_table_cell = ParagraphStyle('TC', fontName='ChineseFont', fontSize=9.5,
    leading=14, alignment=TA_LEFT)
style_caption = ParagraphStyle('Caption', fontName='ChineseFont', fontSize=9,
    leading=13, alignment=TA_CENTER, textColor=HexColor('#757575'),
    spaceBefore=2*mm, spaceAfter=4*mm)

# ============================================================
# 辅助函数
# ============================================================
def H1(text):
    return Paragraph(f"<b>{text}</b>", style_h1)

def H2(text):
    return Paragraph(f"<b>{text}</b>", style_h2)

def H3(text):
    return Paragraph(f"<b>{text}</b>", style_h3)

def P(text):
    return Paragraph(text, style_body)

def Bullet(text):
    return Paragraph(f"<bullet>&bull;</bullet> {text}", style_bullet)

def Code(text):
    return Paragraph(text.replace('\n', '<br/>'), style_code)

def Caption(text):
    return Paragraph(text, style_caption)

def make_table(headers, rows, col_widths=None):
    """创建统一风格的表格"""
    header_row = [Paragraph(h, style_table_header) for h in headers]
    data = [header_row]
    for row in rows:
        data.append([Paragraph(str(c), style_table_cell) for c in row])

    w = col_widths or [170*mm // len(headers)] * len(headers)
    t = Table(data, colWidths=w, repeatRows=1)

    style_cmds = [
        ('BACKGROUND', (0, 0), (-1, 0), TABLE_HEADER_BG),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, BORDER_COLOR),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ('TOPPADDING', (0, 1), (-1, -1), 6),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(('BACKGROUND', (0, i), (-1, i), TABLE_ALT_ROW))
    t.setStyle(TableStyle(style_cmds))
    return t

def horizontal_line():
    d = Drawing(170*mm, 2)
    d.add(Line(0, 1, 170*mm, 1, strokeColor=BORDER_COLOR, strokeWidth=0.5))
    return d

def add_page_number(canvas, doc):
    canvas.saveState()
    canvas.setFont('ChineseFont', 8)
    canvas.setFillColor(HexColor('#9e9e9e'))
    canvas.drawCentredString(A4[0]/2, 15*mm,
        f"Hadoop YARN Server 核心架构设计文档  —  第 {doc.page} 页")
    canvas.restoreState()


def draw_architecture_box(d, x, y, w, h, text, fill_color, text_color=black, font_size=9):
    """在Drawing上绘制一个带文字的圆角矩形"""
    d.add(Rect(x, y, w, h, fillColor=fill_color, strokeColor=HexColor('#90a4ae'), strokeWidth=0.5, rx=3, ry=3))
    d.add(String(x + w/2, y + h/2 - font_size/3, text, fontName='ChineseFont', fontSize=font_size,
                 fillColor=text_color, textAnchor='middle'))


# ============================================================
# 文档内容构建
# ============================================================
def build_document():
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
        "Hadoop_YARNServer_release-3.3.5-RC0_核心架构设计文档.pdf")

    doc = SimpleDocTemplate(output_path, pagesize=A4,
        topMargin=20*mm, bottomMargin=25*mm, leftMargin=20*mm, rightMargin=20*mm)

    story = []

    # ========== 封面 ==========
    story.append(Spacer(1, 40*mm))
    story.append(Paragraph("Hadoop YARN Server", style_title))
    story.append(Paragraph("核心架构设计文档", style_title))
    story.append(Spacer(1, 8*mm))
    story.append(horizontal_line())
    story.append(Spacer(1, 6*mm))
    story.append(Paragraph("基于 release-3.3.5-RC0 版本源码深度分析", style_subtitle))
    story.append(Spacer(1, 15*mm))

    cover_info = [
        ["项目名称", "Apache Hadoop YARN Server"],
        ["源码版本", "release-3.3.5-RC0"],
        ["文档类型", "核心架构设计文档"],
        ["模块范围", "hadoop-yarn-server (14个子模块)"],
        ["核心文件数", "1165+ Java 源文件"],
        ["生成日期", "2026年3月24日"],
    ]
    cover_table = Table(cover_info, colWidths=[45*mm, 110*mm])
    cover_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), 'ChineseFont'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('TEXTCOLOR', (0, 0), (0, -1), MEDIUM_BLUE),
        ('FONTNAME', (0, 0), (0, -1), 'ChineseFontBold'),
        ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ('RIGHTPADDING', (0, 0), (0, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('LINEBELOW', (0, 0), (-1, -2), 0.3, BORDER_COLOR),
    ]))
    story.append(cover_table)
    story.append(PageBreak())

    # ========== 目录 ==========
    story.append(H1("目  录"))
    story.append(Spacer(1, 4*mm))
    toc_items = [
        "1. 项目概述与模块总览",
        "2. 整体架构设计",
        "3. ResourceManager 核心架构",
        "4. NodeManager 核心架构",
        "5. 状态机设计模式详解",
        "6. 调度器体系架构",
        "7. Federation 联邦架构",
        "8. 核心流程 — 应用提交与执行",
        "9. 核心流程 — 容器生命周期管理",
        "10. 核心流程 — 节点心跳与资源更新",
        "11. 核心流程 — 调度与资源分配",
        "12. 关键设计模式总结",
        "13. 关键场景调用链分析",
        "14. 接口与扩展点",
        "15. 总结",
    ]
    for item in toc_items:
        story.append(Paragraph(item, style_toc))
    story.append(PageBreak())

    # ========== 第1章: 项目概述 ==========
    story.append(H1("1. 项目概述与模块总览"))
    story.append(P(
        "Hadoop YARN（Yet Another Resource Negotiator）是 Hadoop 生态系统中的资源管理和任务调度框架。"
        "hadoop-yarn-server 是 YARN 的服务端核心实现，包含了 ResourceManager、NodeManager、Router 等"
        "关键服务组件。本文档基于 release-3.3.5-RC0 版本源码，深入分析其架构设计、核心流程和关键设计模式。"
    ))

    story.append(H2("1.1 子模块总览"))
    story.append(P(
        "hadoop-yarn-server 由 14 个子模块组成，每个模块承担不同的职责。以下表格展示了各模块的"
        "核心定位和规模："))

    module_data = [
        ["hadoop-yarn-server-resourcemanager", "ResourceManager", "500+", "集群资源管理和调度核心"],
        ["hadoop-yarn-server-nodemanager", "NodeManager", "402", "节点级容器生命周期管理"],
        ["hadoop-yarn-server-common", "Common", "348", "公共库、Federation状态存储"],
        ["hadoop-yarn-server-applicationhistoryservice", "AHS", "65", "应用历史记录与Timeline V1"],
        ["hadoop-yarn-server-timelineservice", "TimelineService V2", "64", "V2版本Timeline存储"],
        ["hadoop-yarn-server-timelineservice-hbase-common", "HBase Common", "50+", "HBase存储公共模块"],
        ["hadoop-yarn-server-timelineservice-hbase-client", "HBase Client", "20+", "HBase客户端读写"],
        ["hadoop-yarn-server-timelineservice-hbase-server", "HBase Server", "15+", "HBase协处理器"],
        ["hadoop-yarn-server-timelineservice-documentstore", "DocStore", "10+", "文档型存储后端"],
        ["hadoop-yarn-server-router", "Router", "67", "Federation路由层"],
        ["hadoop-yarn-server-globalpolicygenerator", "GPG", "28", "全局联邦策略生成器"],
        ["hadoop-yarn-server-sharedcachemanager", "SCM", "19", "共享缓存管理服务"],
        ["hadoop-yarn-server-web-proxy", "WebProxy", "13", "AM Web代理"],
        ["hadoop-yarn-server-tests", "Tests", "—", "集成测试模块"],
    ]
    story.append(make_table(
        ["模块名称", "简称", "文件数", "核心职责"],
        module_data,
        col_widths=[55*mm, 28*mm, 18*mm, 69*mm]
    ))
    story.append(Caption("表 1-1: hadoop-yarn-server 子模块总览"))

    story.append(H2("1.2 服务入口点"))
    story.append(P(
        "YARN Server 共有 8 个可独立启动的服务进程，每个进程通过 main() 方法作为入口启动："))
    entries = [
        ("ResourceManager", "org.apache.hadoop.yarn.server.resourcemanager.ResourceManager",
         "集群的全局资源管理器，负责接收应用提交、资源调度、节点管理"),
        ("NodeManager", "org.apache.hadoop.yarn.server.nodemanager.NodeManager",
         "节点代理，负责容器启动/停止/监控、资源隔离"),
        ("ApplicationHistoryServer", "org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer",
         "历史应用记录查询服务，Timeline Service V1"),
        ("TimelineReaderServer", "org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderServer",
         "Timeline Service V2 读取服务"),
        ("GlobalPolicyGenerator", "org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator",
         "Federation 全局策略生成器"),
        ("Router", "org.apache.hadoop.yarn.server.router.Router",
         "Federation 路由网关，客户端透明访问多集群"),
        ("SharedCacheManager", "org.apache.hadoop.yarn.server.sharedcachemanager.SharedCacheManager",
         "分布式缓存管理服务"),
        ("WebAppProxyServer", "org.apache.hadoop.yarn.server.webproxy.WebAppProxyServer",
         "ApplicationMaster Web UI 安全代理"),
    ]
    for name, cls, desc in entries:
        story.append(Bullet(f"<b>{name}</b>: {desc}"))
    story.append(PageBreak())

    # ========== 第2章: 整体架构设计 ==========
    story.append(H1("2. 整体架构设计"))
    story.append(P(
        "YARN Server 采用经典的 Master/Slave 架构。ResourceManager 作为 Master 节点，负责全局资源管理"
        "和调度决策；NodeManager 作为 Slave 节点，运行在每台工作机器上，负责具体的容器管理和资源监控。"
        "在 Federation 场景下，Router 作为统一入口代理多个 YARN 集群的请求。"
    ))

    story.append(H2("2.1 核心架构图"))

    # 绘制架构图
    arch_d = Drawing(480, 420)

    # Client层
    draw_architecture_box(arch_d, 170, 390, 140, 25, "Client / Application", HexColor('#e3f2fd'), DARK_BLUE, 10)

    # 箭头（简化用线）
    arch_d.add(Line(240, 390, 240, 375, strokeColor=grey, strokeWidth=1))
    arch_d.add(Line(240, 375, 120, 375, strokeColor=grey, strokeWidth=1))
    arch_d.add(Line(240, 375, 360, 375, strokeColor=grey, strokeWidth=1))
    arch_d.add(Line(120, 375, 120, 360, strokeColor=grey, strokeWidth=1))
    arch_d.add(Line(360, 375, 360, 360, strokeColor=grey, strokeWidth=1))

    # Router层(可选)
    draw_architecture_box(arch_d, 310, 340, 100, 25, "Router(Federation)", HexColor('#fff3e0'), ACCENT_ORANGE, 8)

    # ResourceManager层
    draw_architecture_box(arch_d, 20, 340, 230, 25, "ResourceManager (Master)", HexColor('#c8e6c9'), ACCENT_GREEN, 11)

    # RM内部组件
    rm_y = 270
    rm_components = [
        (20, rm_y, 70, 22, "ClientRM\nService"),
        (95, rm_y, 80, 22, "ApplicationMaster\nService"),
        (180, rm_y, 70, 22, "Resource\nTracker"),
    ]
    for x, y, w, h, text in rm_components:
        draw_architecture_box(arch_d, x, y, w, h, text, HexColor('#e8f5e9'), ACCENT_GREEN, 7)

    # RM中间层
    rm_mid_y = 235
    draw_architecture_box(arch_d, 20, rm_mid_y, 80, 22, "RMAppManager", HexColor('#e8f5e9'), ACCENT_GREEN, 8)
    draw_architecture_box(arch_d, 105, rm_mid_y, 70, 22, "Scheduler", HexColor('#fff9c4'), ACCENT_ORANGE, 9)
    draw_architecture_box(arch_d, 180, rm_mid_y, 70, 22, "AMLauncher", HexColor('#e8f5e9'), ACCENT_GREEN, 8)

    # 调度器实现
    sched_y = 195
    draw_architecture_box(arch_d, 20, sched_y, 72, 20, "Capacity\nScheduler", HexColor('#fff9c4'), black, 7)
    draw_architecture_box(arch_d, 97, sched_y, 72, 20, "Fair\nScheduler", HexColor('#fff9c4'), black, 7)
    draw_architecture_box(arch_d, 174, sched_y, 72, 20, "FIFO\nScheduler", HexColor('#fff9c4'), black, 7)

    # 状态机层
    sm_y = 160
    draw_architecture_box(arch_d, 20, sm_y, 55, 22, "RMApp", HexColor('#fce4ec'), black, 8)
    draw_architecture_box(arch_d, 80, sm_y, 65, 22, "RMApp\nAttempt", HexColor('#fce4ec'), black, 7)
    draw_architecture_box(arch_d, 150, sm_y, 55, 22, "RMNode", HexColor('#fce4ec'), black, 8)
    draw_architecture_box(arch_d, 210, sm_y, 65, 22, "RMContainer", HexColor('#fce4ec'), black, 7)

    # StateStore / HA
    draw_architecture_box(arch_d, 20, 125, 100, 22, "RMStateStore(ZK/FS)", HexColor('#e0e0e0'), black, 8)
    draw_architecture_box(arch_d, 130, 125, 120, 22, "HA(Active/Standby)", HexColor('#e0e0e0'), black, 8)

    # NodeManager层
    nm_x = 290
    draw_architecture_box(arch_d, nm_x, 270, 170, 25, "NodeManager (Slave)", HexColor('#e3f2fd'), DARK_BLUE, 10)

    nm_comp_y = 235
    draw_architecture_box(arch_d, nm_x, nm_comp_y, 80, 22, "Container\nManager", HexColor('#e3f2fd'), DARK_BLUE, 8)
    draw_architecture_box(arch_d, nm_x + 85, nm_comp_y, 80, 22, "NodeStatus\nUpdater", HexColor('#e3f2fd'), DARK_BLUE, 8)

    nm_comp_y2 = 200
    draw_architecture_box(arch_d, nm_x, nm_comp_y2, 55, 22, "Localizer", HexColor('#e3f2fd'), DARK_BLUE, 8)
    draw_architecture_box(arch_d, nm_x + 60, nm_comp_y2, 55, 22, "Launcher", HexColor('#e3f2fd'), DARK_BLUE, 8)
    draw_architecture_box(arch_d, nm_x + 120, nm_comp_y2, 50, 22, "Monitor", HexColor('#e3f2fd'), DARK_BLUE, 8)

    nm_comp_y3 = 165
    draw_architecture_box(arch_d, nm_x, nm_comp_y3, 80, 22, "ContainerRuntime\n(Linux/Docker)", HexColor('#ede7f6'), black, 7)
    draw_architecture_box(arch_d, nm_x + 85, nm_comp_y3, 80, 22, "CGroups\n资源隔离", HexColor('#ede7f6'), black, 7)

    # NM容器层
    draw_architecture_box(arch_d, nm_x, 130, 50, 22, "Container", HexColor('#bbdefb'), DARK_BLUE, 8)
    draw_architecture_box(arch_d, nm_x + 55, 130, 50, 22, "Container", HexColor('#bbdefb'), DARK_BLUE, 8)
    draw_architecture_box(arch_d, nm_x + 110, 130, 50, 22, "Container", HexColor('#bbdefb'), DARK_BLUE, 8)

    # 公共基础设施
    draw_architecture_box(arch_d, 20, 85, 440, 25, "Common 基础设施: Event/Dispatcher, StateMachine, RPC/Protocol, Security, WebApp", HexColor('#efebe9'), black, 9)

    # Federation层
    draw_architecture_box(arch_d, 20, 50, 440, 25, "Federation: StateStore / PolicyEngine / SubClusterResolver / GPG", HexColor('#fff3e0'), ACCENT_ORANGE, 9)

    # 存储层
    draw_architecture_box(arch_d, 20, 15, 440, 25, "持久化: ZooKeeper / HDFS / LevelDB / HBase / Timeline Store", HexColor('#e0e0e0'), black, 9)

    story.append(arch_d)
    story.append(Caption("图 2-1: YARN Server 整体架构图"))

    story.append(H2("2.2 分层架构说明"))
    story.append(P(
        "YARN Server 的架构可以从上到下分为以下几层："))
    layers_data = [
        ["客户端接入层", "ClientRMService, AMRMService", "接收客户端和AM的RPC请求"],
        ["应用管理层", "RMAppManager, RMApp状态机", "应用提交、生命周期管理、Attempt管理"],
        ["资源调度层", "CapacityScheduler, FairScheduler", "队列管理、资源分配、抢占"],
        ["节点管理层", "ResourceTracker, RMNode状态机", "节点注册/心跳/去委任"],
        ["容器管理层", "RMContainer, ContainerManagerImpl", "容器分配/启动/监控/回收"],
        ["联邦路由层", "Router, FederationClientInterceptor", "跨集群请求路由和聚合"],
        ["事件驱动层", "AsyncDispatcher, EventHandler", "异步事件分发总线"],
        ["状态持久化层", "RMStateStore, NMStateStore", "ZK/FS/LevelDB状态恢复"],
    ]
    story.append(make_table(
        ["层次", "核心组件", "职责"],
        layers_data,
        col_widths=[35*mm, 60*mm, 75*mm]
    ))
    story.append(Caption("表 2-1: YARN Server 分层架构"))
    story.append(PageBreak())

    # ========== 第3章: ResourceManager 核心架构 ==========
    story.append(H1("3. ResourceManager 核心架构"))
    story.append(P(
        "ResourceManager 是 YARN 集群的大脑，继承自 CompositeService，采用组合服务模式（Composite Pattern）"
        "管理内部所有子服务。RM 的服务分为两类：Always-On 服务（无论 HA 状态如何都运行）和 Active 服务"
        "（仅在 Active RM 中运行）。"))

    story.append(H2("3.1 RM 服务组件"))
    story.append(P("ResourceManager 内部包含以下核心组件，它们共同构成了一个完整的资源管理体系："))

    rm_components_data = [
        ["ClientRMService", "处理客户端RPC请求（提交应用、查询状态、Kill应用等）", "Always-On"],
        ["ApplicationMasterService", "处理AM的RPC请求（资源申请、心跳、注册/注销）", "Active"],
        ["ResourceTrackerService", "处理NM的RPC请求（注册、心跳）", "Active"],
        ["ResourceScheduler", "资源调度器（Capacity/Fair/FIFO）", "Active"],
        ["RMAppManager", "应用管理器，负责应用创建和移除", "Active"],
        ["ApplicationMasterLauncher", "AM启动器，向NM发送启动AM容器命令", "Active"],
        ["NMLivelinessMonitor", "NM存活监控，定期检测NM心跳超时", "Active"],
        ["AMLivelinessMonitor", "AM存活监控，定期检测AM心跳超时", "Active"],
        ["RMStateStore", "状态持久化（ZK/FS），支持RM故障恢复", "Active"],
        ["AdminService", "管理员操作接口（刷新队列/节点列表等）", "Always-On"],
        ["RMNodeLabelsManager", "节点标签管理", "Active"],
        ["ContainerAllocationExpirer", "容器分配过期监控", "Active"],
        ["DelegationTokenRenewer", "安全Token续期服务", "Active"],
        ["FederationStateStoreService", "Federation状态存储服务", "Active"],
    ]
    story.append(make_table(
        ["组件名称", "职责描述", "服务类型"],
        rm_components_data,
        col_widths=[50*mm, 85*mm, 25*mm]
    ))
    story.append(Caption("表 3-1: ResourceManager 核心组件"))

    story.append(H2("3.2 RM 事件驱动架构"))
    story.append(P(
        "RM 内部采用中央事件分发器（AsyncDispatcher）实现组件间的松耦合通信。每种事件类型注册一个"
        "EventHandler，Dispatcher 通过事件类型将事件路由到对应的 Handler。核心事件类型包括："))
    event_data = [
        ["RMAppEventType", "应用级事件", "START, KILL, RECOVER, APP_REJECTED, ATTEMPT_*"],
        ["RMAppAttemptEventType", "应用尝试事件", "START, KILL, REGISTERED, CONTAINER_ALLOCATED, EXPIRE"],
        ["RMNodeEventType", "节点事件", "STARTED, STATUS_UPDATE, EXPIRE, DECOMMISSION, RECONNECTED"],
        ["SchedulerEventType", "调度事件", "NODE_ADDED/REMOVED, APP_ADDED/REMOVED, NODE_UPDATE"],
        ["RMContainerEventType", "容器事件", "START, ACQUIRED, LAUNCHED, FINISHED, KILL, EXPIRE"],
        ["AMLauncherEventType", "AM启动事件", "LAUNCH, CLEANUP"],
        ["NodesListManagerEventType", "节点列表事件", "NODE_USABLE, NODE_UNUSABLE"],
    ]
    story.append(make_table(
        ["事件类型", "描述", "关键事件"],
        event_data,
        col_widths=[45*mm, 35*mm, 90*mm]
    ))
    story.append(Caption("表 3-2: RM 核心事件类型"))

    story.append(H2("3.3 RM 高可用架构"))
    story.append(P(
        "ResourceManager 支持 Active/Standby 高可用模式。核心机制包括："))
    story.append(Bullet(
        "<b>Leader Election</b>: 基于 ZooKeeper（CuratorBasedElectorService 或 ActiveStandbyElectorBasedElectorService）"
        "进行主节点选举"))
    story.append(Bullet(
        "<b>RMStateStore</b>: 支持 ZKRMStateStore（ZooKeeper）和 FileSystemRMStateStore（HDFS）两种持久化后端，"
        "用于保存应用/Attempt 状态、安全Token等"))
    story.append(Bullet(
        "<b>Work-Preserving Recovery</b>: Active RM 故障切换后，Standby RM 从 StateStore 恢复应用状态，"
        "NM 重新连接后 Container 继续运行"))
    story.append(Bullet(
        "<b>RMActiveServices</b>: 作为 CompositeService 封装所有仅在 Active RM 上运行的服务，"
        "切换时统一启停"))
    story.append(PageBreak())

    # ========== 第4章: NodeManager 核心架构 ==========
    story.append(H1("4. NodeManager 核心架构"))
    story.append(P(
        "NodeManager 继承自 CompositeService 并实现 EventHandler&lt;NodeManagerEvent&gt; 接口，"
        "运行在每个集群节点上，负责容器生命周期管理、本地资源管理和状态上报。"))

    story.append(H2("4.1 NM 核心组件"))
    nm_comp_data = [
        ["ContainerManagerImpl", "容器管理核心，实现ContainerManagementProtocol", "接收/处理AM的启动/停止容器请求"],
        ["NodeStatusUpdater(Impl)", "节点状态上报器", "定期向RM发送心跳，报告节点资源和容器状态"],
        ["ResourceLocalizationService", "资源本地化服务", "从HDFS下载容器所需的JAR/文件到本地"],
        ["ContainersLauncher", "容器启动器", "通过ContainerExecutor执行容器的实际启动命令"],
        ["ContainersMonitorImpl", "容器资源监控", "监控容器CPU/内存使用，超限则Kill"],
        ["ContainerScheduler", "本地容器调度", "管理Guaranteed/Opportunistic容器的本地队列"],
        ["LogAggregationService", "日志聚合服务", "将容器日志上传到HDFS集中存储"],
        ["DeletionService", "文件清理服务", "异步清理过期的本地化资源和容器工作目录"],
        ["NodeHealthCheckerService", "节点健康检查", "检测节点磁盘健康状态"],
        ["NMStateStoreService", "NM状态持久化", "基于LevelDB，支持NM重启恢复容器状态"],
    ]
    story.append(make_table(
        ["组件名称", "角色", "核心职责"],
        nm_comp_data,
        col_widths=[52*mm, 46*mm, 72*mm]
    ))
    story.append(Caption("表 4-1: NodeManager 核心组件"))

    story.append(H2("4.2 NM 容器执行引擎"))
    story.append(P(
        "NodeManager 支持多种容器运行时，通过 ContainerExecutor 抽象层实现可插拔："))
    story.append(Bullet(
        "<b>DefaultContainerExecutor</b>: 默认执行器，以NM进程的用户身份启动容器"))
    story.append(Bullet(
        "<b>LinuxContainerExecutor</b>: Linux 原生执行器，支持 CGroups v1/v2 资源隔离"))
    story.append(Bullet(
        "<b>DockerLinuxContainerRuntime</b>: Docker 容器运行时，在Docker中执行任务"))
    story.append(Bullet(
        "<b>RuncContainerRuntime</b>: 基于 Runc 的轻量级容器运行时"))
    story.append(P(
        "资源隔离通过 CGroupsHandler（v1）和 CGroupsV2Handler（v2）实现，支持 CPU、内存、网络等"
        "资源维度的限制。"))
    story.append(PageBreak())

    # ========== 第5章: 状态机设计模式 ==========
    story.append(H1("5. 状态机设计模式详解"))
    story.append(P(
        "YARN RM 广泛采用 StateMachineFactory 实现有限状态机（FSM），这是整个系统最核心的设计模式。"
        "每个状态机通过声明式方式定义状态、事件和转移函数的映射关系，配合读写锁保证线程安全。"))

    story.append(H2("5.1 RMApp 状态机"))
    story.append(P(
        "RMApp 表示一个提交到 YARN 的应用，其状态机管理应用从提交到完成的全生命周期。"
        "核心状态包括：NEW → NEW_SAVING → SUBMITTED → ACCEPTED → RUNNING → FINISHING → FINISHED，"
        "以及 KILLING → KILLED、FAILED 等终态。"))

    rmapp_states = [
        ["NEW", "应用刚创建", "接收START事件后进入NEW_SAVING"],
        ["NEW_SAVING", "持久化应用状态到StateStore", "保存成功后进入SUBMITTED"],
        ["SUBMITTED", "已提交到调度器", "调度器接受后进入ACCEPTED"],
        ["ACCEPTED", "调度器已接受，等待AM启动", "AM注册后进入RUNNING"],
        ["RUNNING", "AM已注册，应用正在运行", "AM注销或完成后进入FINISHING/FINISHED"],
        ["FINISHING", "AM已注销，等待最终清理", "清理完成后进入FINISHED"],
        ["FINAL_SAVING", "保存最终状态到StateStore", "保存完成后转入终态"],
        ["FINISHED", "应用正常完成（终态）", "—"],
        ["FAILED", "应用失败（终态）", "—"],
        ["KILLED", "应用被Kill（终态）", "—"],
        ["KILLING", "正在Kill应用的Attempt", "Attempt被Kill后进入FINAL_SAVING"],
    ]
    story.append(make_table(
        ["状态", "含义", "主要转移"],
        rmapp_states,
        col_widths=[30*mm, 55*mm, 85*mm]
    ))
    story.append(Caption("表 5-1: RMApp 状态机状态表"))

    story.append(H2("5.2 RMAppAttempt 状态机"))
    story.append(P(
        "每个RMApp可以有多个RMAppAttempt（应用尝试），代表一次AM的执行。状态机管理从启动到完成的全过程。"))
    attempt_states = [
        ["NEW", "尝试刚创建", "START → SUBMITTED"],
        ["SUBMITTED", "已提交到调度器", "ATTEMPT_ADDED → SCHEDULED"],
        ["SCHEDULED", "等待AM容器分配", "CONTAINER_ALLOCATED → ALLOCATED_SAVING"],
        ["ALLOCATED_SAVING", "保存AM容器信息", "ATTEMPT_NEW_SAVED → ALLOCATED"],
        ["ALLOCATED", "AM容器已分配", "LAUNCHED → LAUNCHED"],
        ["LAUNCHED", "AM已启动，等待注册", "REGISTERED → RUNNING"],
        ["RUNNING", "AM正在运行", "UNREGISTERED → FINAL_SAVING"],
        ["FINAL_SAVING", "保存最终状态", "转入终态(FINISHED/FAILED/KILLED)"],
    ]
    story.append(make_table(
        ["状态", "含义", "关键转移"],
        attempt_states,
        col_widths=[38*mm, 50*mm, 82*mm]
    ))
    story.append(Caption("表 5-2: RMAppAttempt 状态机状态表"))

    story.append(H2("5.3 RMNode 状态机"))
    story.append(P(
        "RMNode 表示集群中的一个工作节点，状态机管理节点从注册到退出的全生命周期。"))
    node_states = [
        ["NEW", "节点刚注册", "STARTED → RUNNING/UNHEALTHY"],
        ["RUNNING", "节点正常运行", "STATUS_UPDATE保持或→UNHEALTHY; EXPIRE→LOST"],
        ["UNHEALTHY", "节点不健康", "STATUS_UPDATE恢复→RUNNING; EXPIRE→LOST"],
        ["DECOMMISSIONING", "优雅退役中", "容器运行完毕后→DECOMMISSIONED"],
        ["DECOMMISSIONED", "已退役（终态）", "—"],
        ["LOST", "心跳超时丢失（终态）", "—"],
        ["REBOOTED", "节点重启（终态）", "—"],
        ["SHUTDOWN", "节点关闭（终态）", "—"],
    ]
    story.append(make_table(
        ["状态", "含义", "关键转移"],
        node_states,
        col_widths=[38*mm, 45*mm, 87*mm]
    ))
    story.append(Caption("表 5-3: RMNode 状态机状态表"))

    story.append(H2("5.4 RMContainer 状态机"))
    story.append(P(
        "RMContainer 是RM端对容器的抽象，跟踪容器从分配到完成的状态变化："))
    container_states = [
        ["NEW", "容器刚创建", "START→ALLOCATED; RESERVED→RESERVED; RECOVER→RUNNING/COMPLETED"],
        ["RESERVED", "资源已预留", "START→ALLOCATED; KILL→KILLED"],
        ["ALLOCATED", "已分配，等待AM获取", "ACQUIRED→ACQUIRED; EXPIRE→EXPIRED"],
        ["ACQUIRED", "AM已获取容器", "LAUNCHED→RUNNING; FINISHED→COMPLETED"],
        ["RUNNING", "容器正在运行", "FINISHED→COMPLETED; KILL→KILLED; RELEASED→RELEASED"],
        ["COMPLETED", "正常完成（终态）", "—"],
        ["EXPIRED", "超时未获取（终态）", "—"],
        ["KILLED", "被Kill（终态）", "—"],
        ["RELEASED", "被释放（终态）", "—"],
    ]
    story.append(make_table(
        ["状态", "含义", "关键转移"],
        container_states,
        col_widths=[28*mm, 45*mm, 97*mm]
    ))
    story.append(Caption("表 5-4: RMContainer 状态机状态表"))
    story.append(PageBreak())

    # ========== 第6章: 调度器体系 ==========
    story.append(H1("6. 调度器体系架构"))
    story.append(P(
        "调度器是YARN的核心大脑，决定如何将有限的集群资源分配给各个应用。YARN支持可插拔的调度器，"
        "通过配置 yarn.resourcemanager.scheduler.class 切换实现。"))

    story.append(H2("6.1 调度器继承体系"))
    story.append(P("调度器的类层次结构如下："))
    story.append(Code(
        "ResourceScheduler (接口)\n"
        "  └─ AbstractYarnScheduler&lt;T, N&gt; (抽象基类)\n"
        "       ├─ CapacityScheduler      (容量调度器 - 默认 &amp; 生产推荐)\n"
        "       ├─ FairScheduler          (公平调度器)\n"
        "       └─ FifoScheduler          (FIFO调度器)"
    ))

    story.append(H2("6.2 AbstractYarnScheduler 基类"))
    story.append(P(
        "AbstractYarnScheduler 是所有调度器的抽象基类，继承 AbstractService 并实现 ResourceScheduler 接口。"
        "它提供了以下核心能力："))
    story.append(Bullet("<b>ClusterNodeTracker</b>: 追踪集群中所有节点的资源状态"))
    story.append(Bullet("<b>读写锁机制</b>: ReentrantReadWriteLock 保护队列变更、应用增删、容器分配等操作"))
    story.append(Bullet("<b>SchedulingMonitorManager</b>: 调度监控管理（抢占监视器等）"))
    story.append(Bullet("<b>UpdateThread</b>: 异步更新线程，定期触发调度决策"))
    story.append(Bullet("<b>应用管理</b>: ConcurrentMap&lt;ApplicationId, SchedulerApplication&gt; 维护所有应用状态"))

    story.append(H2("6.3 CapacityScheduler (容量调度器)"))
    story.append(P(
        "CapacityScheduler 是 YARN 默认且生产环境推荐使用的调度器（134KB+ 代码），核心特性包括："))
    cs_features = [
        ["层次化队列", "树形队列结构，支持 ParentQueue 和 LeafQueue，队列容量可配置百分比"],
        ["容量保证", "每个队列保证最小容量，空闲时可弹性使用其他队列资源"],
        ["多租户隔离", "队列级别的ACL访问控制，按用户/组配置权限"],
        ["抢占机制", "PreemptionManager 支持按队列容量比例回收超额使用的资源"],
        ["节点标签", "Node Labels/Partition 实现异构集群资源分区调度"],
        ["放置约束", "Placement Constraints 支持反亲和性等高级调度约束"],
        ["异步调度", "AsyncSchedulingConfiguration 支持多线程异步调度"],
        ["可变配置", "MutableCSConfigurationProvider 支持运行时动态修改队列配置"],
        ["应用优先级", "AppPriorityACLsManager 管理应用优先级"],
        ["最大运行应用数", "CSMaxRunningAppsEnforcer 控制每个队列/用户的并发应用数"],
    ]
    story.append(make_table(
        ["特性", "说明"],
        cs_features,
        col_widths=[35*mm, 135*mm]
    ))
    story.append(Caption("表 6-1: CapacityScheduler 核心特性"))

    story.append(H2("6.4 调度核心流程"))
    story.append(P("CapacityScheduler 的资源分配发生在 NodeUpdate 事件处理中："))
    story.append(Code(
        "1. NodeManager 发送心跳 → ResourceTracker 接收\n"
        "2. 触发 NODE_UPDATE SchedulerEvent\n"
        "3. CapacityScheduler.nodeUpdate() 被调用\n"
        "4. 从根队列开始递归: RootQueue.assignContainers()\n"
        "5. ParentQueue 按排序选择子队列分配\n"
        "6. LeafQueue 按排序选择应用分配\n"
        "7. FiCaSchedulerApp.assignContainers() 匹配资源请求\n"
        "8. 生成 ResourceCommitRequest 提交分配结果\n"
        "9. 更新 RMContainer/SchedulerNode 状态"
    ))
    story.append(PageBreak())

    # ========== 第7章: Federation 联邦架构 ==========
    story.append(H1("7. Federation 联邦架构"))
    story.append(P(
        "YARN Federation 允许多个独立的 YARN 集群（SubCluster）组成一个超大规模的联邦集群，"
        "对外提供统一的资源管理视图。这是 YARN 实现超万级节点规模的关键架构。"))

    story.append(H2("7.1 Federation 核心组件"))
    fed_comp_data = [
        ["Router", "联邦路由网关", "接收客户端请求，透明路由到目标SubCluster"],
        ["FederationClientInterceptor", "客户端请求拦截器", "实现ApplicationClientProtocol的联邦化"],
        ["FederationInterceptorREST", "REST请求拦截器", "实现REST API的联邦化"],
        ["GlobalPolicyGenerator (GPG)", "全局策略生成器", "基于集群负载生成路由策略"],
        ["FederationStateStore", "联邦状态存储", "存储SubCluster信息、应用归属关系"],
        ["RouterPolicyFacade", "路由策略门面", "封装SubCluster选择策略"],
        ["SubClusterResolver", "子集群解析器", "解析队列到SubCluster的映射"],
    ]
    story.append(make_table(
        ["组件", "角色", "核心职责"],
        fed_comp_data,
        col_widths=[55*mm, 40*mm, 75*mm]
    ))
    story.append(Caption("表 7-1: Federation 核心组件"))

    story.append(H2("7.2 拦截器链模式 (Interceptor Chain)"))
    story.append(P(
        "Router 模块采用责任链（Chain of Responsibility）模式处理请求。每个拦截器实现"
        "ClientRequestInterceptor 接口，按链式顺序处理请求："))
    story.append(Code(
        "ClientRequestInterceptor (接口)\n"
        "  ├─ PassThroughClientRequestInterceptor  (透传)\n"
        "  ├─ FederationClientInterceptor           (联邦路由 - 链尾)\n"
        "  └─ 自定义拦截器（日志、鉴权、限流等）\n\n"
        "RMAdminRequestInterceptor (管理接口)\n"
        "  ├─ DefaultRMAdminRequestInterceptor\n"
        "  └─ FederationRMAdminInterceptor\n\n"
        "RESTRequestInterceptor (REST接口)\n"
        "  ├─ DefaultRequestInterceptorREST\n"
        "  └─ FederationInterceptorREST"
    ))
    story.append(P(
        "FederationClientInterceptor 作为链尾拦截器，通过 FederationStateStoreFacade 查询 SubCluster 信息，"
        "利用 RouterPolicyFacade 选择目标集群，然后为每个 SubCluster 创建 RPC 代理发送请求。"
        "对于聚合类请求（如 getApplications），会并行向所有 SubCluster 发送请求并合并结果。"))
    story.append(PageBreak())

    # ========== 第8章: 应用提交与执行流程 ==========
    story.append(H1("8. 核心流程 — 应用提交与执行"))
    story.append(P(
        "应用提交是 YARN 最核心的流程之一，涉及 Client、RM、Scheduler、NM 等多个组件的协作。"
        "以下是完整的应用提交到执行的时序流程："))

    story.append(H2("8.1 应用提交时序"))
    story.append(Code(
        "Client → ClientRMService: submitApplication(ApplicationSubmissionContext)\n"
        "  ClientRMService → RMAppManager: submitApplication()\n"
        "    RMAppManager: 创建 RMAppImpl\n"
        "    RMAppManager → Dispatcher: RMAppEvent(START)\n"
        "      RMApp状态机: NEW → NEW_SAVING\n"
        "      RMApp → RMStateStore: storeNewApplication()\n"
        "      RMStateStore → Dispatcher: RMAppEvent(APP_NEW_SAVED)\n"
        "      RMApp状态机: NEW_SAVING → SUBMITTED\n"
        "      AddApplicationToSchedulerTransition:\n"
        "        → Dispatcher: SchedulerEvent(APP_ADDED)\n"
        "        Scheduler → Dispatcher: RMAppEvent(APP_ACCEPTED)\n"
        "        RMApp状态机: SUBMITTED → ACCEPTED\n"
        "        StartAppAttemptTransition:\n"
        "          创建 RMAppAttemptImpl\n"
        "          → Dispatcher: RMAppAttemptEvent(START)\n"
        "          RMAppAttempt状态机: NEW → SUBMITTED\n"
        "          AttemptStartedTransition:\n"
        "            → Scheduler: APP_ATTEMPT_ADDED\n"
        "            Scheduler 为AM申请资源...\n"
        "            → Dispatcher: RMAppAttemptEvent(CONTAINER_ALLOCATED)\n"
        "            RMAppAttempt状态机: SCHEDULED → ALLOCATED_SAVING\n"
        "            → RMStateStore: storeNewApplicationAttempt()\n"
        "            → ALLOCATED → LAUNCHED\n"
        "            → AMLauncher: 向NM发送启动AM容器请求"
    ))
    story.append(Caption("图 8-1: 应用提交时序（文本流程图）"))

    story.append(H2("8.2 关键参与类"))
    submit_classes = [
        ["ClientRMService", "接收submitApplication RPC", "参数校验、权限检查、转发给RMAppManager"],
        ["RMAppManager", "应用管理入口", "创建RMAppImpl，触发START事件"],
        ["RMAppImpl", "应用状态机", "驱动应用经历NEW→SUBMITTED→ACCEPTED→RUNNING"],
        ["RMAppAttemptImpl", "Attempt状态机", "管理AM容器分配→启动→注册→运行"],
        ["ResourceScheduler", "资源调度器", "接收APP_ADDED/ATTEMPT_ADDED事件，分配AM容器"],
        ["ApplicationMasterLauncher", "AM启动器", "创建AMLauncherEvent，通过RPC向NM启动AM容器"],
    ]
    story.append(make_table(
        ["类名", "角色", "关键行为"],
        submit_classes,
        col_widths=[48*mm, 35*mm, 87*mm]
    ))
    story.append(Caption("表 8-1: 应用提交流程关键参与类"))
    story.append(PageBreak())

    # ========== 第9章: 容器生命周期管理 ==========
    story.append(H1("9. 核心流程 — 容器生命周期管理"))
    story.append(P(
        "容器（Container）是 YARN 中资源分配的基本单位。容器的生命周期涉及 RM 端（RMContainer）和 NM 端"
        "（ContainerImpl）两个状态机的协同。"))

    story.append(H2("9.1 容器分配流程"))
    story.append(Code(
        "1. AM → AMRMService: allocate(ResourceRequest)\n"
        "2. Scheduler 在 NodeUpdate 时匹配请求\n"
        "3. 创建 RMContainer(状态: NEW → ALLOCATED)\n"
        "4. AM 下次 allocate 心跳获取分配结果\n"
        "   RMContainer: ALLOCATED → ACQUIRED\n"
        "5. AM → ContainerManagerImpl(NM): startContainers()\n"
        "   NM创建ContainerImpl(状态: NEW → LOCALIZING)\n"
        "6. ResourceLocalizationService 下载资源\n"
        "   ContainerImpl: LOCALIZING → LOCALIZED\n"
        "7. ContainersLauncher 启动容器进程\n"
        "   ContainerImpl: LOCALIZED → RUNNING\n"
        "   NM心跳上报 → RMContainer: ACQUIRED → RUNNING\n"
        "8. 容器执行完成\n"
        "   ContainerImpl: RUNNING → EXITED_WITH_SUCCESS/FAILURE\n"
        "   NM心跳上报 → RMContainer: RUNNING → COMPLETED"
    ))
    story.append(Caption("图 9-1: 容器完整生命周期"))

    story.append(H2("9.2 NM端容器状态机"))
    story.append(P(
        "NM端的 ContainerImpl 有更细粒度的状态管理，反映了资源本地化、进程启动、监控等阶段："))
    nm_container_states = [
        ["NEW", "容器刚创建"],
        ["LOCALIZING", "正在下载资源到本地"],
        ["LOCALIZED", "资源下载完成，等待启动"],
        ["SCHEDULED", "ContainerScheduler已调度"],
        ["RUNNING", "容器进程正在运行"],
        ["REINITIALIZING", "容器重初始化中"],
        ["RELAUNCHING", "容器重启中"],
        ["EXITED_WITH_SUCCESS", "正常退出"],
        ["EXITED_WITH_FAILURE", "异常退出"],
        ["KILLING", "正在Kill"],
        ["CONTAINER_CLEANEDUP_AFTER_KILL", "Kill后清理完成"],
        ["DONE", "最终完成"],
    ]
    story.append(make_table(
        ["状态", "描述"],
        nm_container_states,
        col_widths=[65*mm, 105*mm]
    ))
    story.append(Caption("表 9-1: NM 端 ContainerImpl 状态"))
    story.append(PageBreak())

    # ========== 第10章: 节点心跳 ==========
    story.append(H1("10. 核心流程 — 节点心跳与资源更新"))
    story.append(P(
        "节点心跳是 RM 和 NM 之间的核心通信机制，承载了资源状态上报、容器状态同步、命令下发等关键功能。"))

    story.append(H2("10.1 心跳流程"))
    story.append(Code(
        "NM(NodeStatusUpdater) → RM(ResourceTrackerService): nodeHeartbeat()\n"
        "  ├─ 上行数据:\n"
        "  │   ├─ NodeStatus (节点健康状态/资源利用率)\n"
        "  │   ├─ ContainerStatus[] (已完成/运行中容器状态)\n"
        "  │   ├─ keepAliveApplications (活跃应用列表)\n"
        "  │   └─ LogAggregationReport (日志聚合报告)\n"
        "  │\n"
        "  ├─ RM处理:\n"
        "  │   ├─ 更新 NMLivelinessMonitor (重置超时计时器)\n"
        "  │   ├─ 触发 RMNodeEvent(STATUS_UPDATE)\n"
        "  │   │   └─ RMNode状态机: StatusUpdateWhenHealthyTransition\n"
        "  │   │       ├─ 更新容器信息到 nodeUpdateQueue\n"
        "  │   │       └─ 触发 NodeUpdateSchedulerEvent\n"
        "  │   │           └─ Scheduler.nodeUpdate() → 资源分配!\n"
        "  │   └─ 处理已完成容器状态\n"
        "  │\n"
        "  └─ 下行数据 (NodeHeartbeatResponse):\n"
        "      ├─ containersToCleanup (需清理的容器)\n"
        "      ├─ applicationsToCleanup (需清理的应用)\n"
        "      ├─ containersToSignal (信号容器)\n"
        "      ├─ containersToBeRemovedFromNM (从NM移除的容器)\n"
        "      └─ nextHeartBeatInterval (下次心跳间隔)"
    ))
    story.append(Caption("图 10-1: 节点心跳流程"))

    story.append(H2("10.2 心跳驱动调度"))
    story.append(P(
        "值得注意的是，CapacityScheduler 的资源分配是<b>心跳驱动</b>的：每次 NM 心跳触发一次 nodeUpdate()，"
        "调度器在此事件中尝试为待调度的应用分配该节点上的可用资源。这意味着节点越多，心跳越频繁，调度吞吐量越高。"
        "对于大规模集群，可启用异步调度模式（asyncSchedulingConf）通过独立线程持续执行调度，不受心跳频率限制。"))
    story.append(PageBreak())

    # ========== 第11章: 调度与资源分配 ==========
    story.append(H1("11. 核心流程 — 调度与资源分配"))

    story.append(H2("11.1 CapacityScheduler 分配算法"))
    story.append(P("CapacityScheduler 的核心分配逻辑遵循以下步骤："))
    story.append(Code(
        "CapacityScheduler.allocateContainersToNode(FiCaSchedulerNode node):\n"
        "  1. 构建 CandidateNodeSet（候选节点集）\n"
        "  2. 调用 RootQueue.assignContainers(clusterResource, node, ...)\n"
        "     │\n"
        "     ├─ ParentQueue.assignContainers():\n"
        "     │   ├─ 按排序遍历子队列（sortedQueues）\n"
        "     │   ├─ 检查子队列资源限制 (canAssign)\n"
        "     │   └─ 递归调用子队列的 assignContainers\n"
        "     │\n"
        "     └─ LeafQueue.assignContainers():\n"
        "         ├─ 按优先级排序应用列表\n"
        "         ├─ FiCaSchedulerApp.assignContainers():\n"
        "         │   ├─ 匹配 ResourceRequest（NODE_LOCAL > RACK_LOCAL > OFF_SWITCH）\n"
        "         │   ├─ 检查资源是否满足\n"
        "         │   ├─ 检查放置约束(PlacementConstraints)\n"
        "         │   └─ 生成 ContainerAllocationProposal\n"
        "         └─ 提交 ResourceCommitRequest\n"
        "             └─ tryCommit() → 创建RMContainer并分配"
    ))
    story.append(Caption("图 11-1: CapacityScheduler 分配算法"))

    story.append(H2("11.2 资源匹配策略"))
    story.append(P("YARN 支持多层次的资源匹配策略，按本地性从高到低："))
    locality_data = [
        ["NODE_LOCAL", "请求的节点与分配节点完全匹配", "最优，数据本地性最好"],
        ["RACK_LOCAL", "请求的机架与分配节点所在机架匹配", "次优，同机架网络开销较小"],
        ["OFF_SWITCH", "任意节点均可", "最后选择，无本地性保证"],
    ]
    story.append(make_table(
        ["匹配类型", "说明", "优先级"],
        locality_data,
        col_widths=[30*mm, 80*mm, 60*mm]
    ))
    story.append(Caption("表 11-1: 资源匹配本地性策略"))
    story.append(PageBreak())

    # ========== 第12章: 设计模式总结 ==========
    story.append(H1("12. 关键设计模式总结"))
    story.append(P(
        "YARN Server 源码中大量运用了经典的设计模式，这些模式共同构建了一个高度模块化、可扩展的系统。"))

    patterns_data = [
        ["事件驱动/观察者模式", "AsyncDispatcher + EventHandler",
         "所有组件通过事件通信，解耦发送者和接收者。Dispatcher作为中央事件总线，"
         "各组件注册EventHandler处理特定类型事件"],
        ["状态机模式", "StateMachineFactory",
         "RMApp/RMAppAttempt/RMNode/RMContainer/NMContainer 均使用声明式状态机，"
         "通过 addTransition() 定义状态×事件→新状态×动作 的完整映射"],
        ["组合服务模式", "CompositeService",
         "ResourceManager/NodeManager 继承 CompositeService，管理多个子Service的init/start/stop生命周期"],
        ["责任链模式", "Interceptor Chain (Router)",
         "Router模块的ClientRequestInterceptor链式处理请求，支持灵活插入日志/鉴权/限流/路由等逻辑"],
        ["工厂模式", "createScheduler() / createContainerExecutor()",
         "通过反射+配置动态创建调度器、容器执行器等核心组件，支持运行时可插拔"],
        ["策略模式", "PlacementRule / FederationPolicy",
         "队列放置规则和联邦路由策略均可配置替换，实现不同的调度/路由策略"],
        ["模板方法模式", "AbstractYarnScheduler",
         "定义调度流程骨架，子类(CapacityScheduler/FairScheduler)重写具体步骤"],
        ["代理模式", "WebAppProxy / FederationProxy",
         "AM Web UI 代理和 Federation SubCluster RPC 代理"],
        ["单例模式", "ClusterMetrics / RouterMetrics",
         "指标收集器使用单例，全局唯一实例统一收集集群/路由指标"],
        ["读写锁模式", "ReentrantReadWriteLock",
         "状态机操作使用ReadLock/WriteLock，读操作并发，写操作互斥，平衡性能与安全"],
    ]
    story.append(make_table(
        ["设计模式", "应用位置", "说明"],
        patterns_data,
        col_widths=[35*mm, 50*mm, 85*mm]
    ))
    story.append(Caption("表 12-1: YARN Server 核心设计模式"))
    story.append(PageBreak())

    # ========== 第13章: 关键场景调用链 ==========
    story.append(H1("13. 关键场景调用链分析"))

    story.append(H2("13.1 应用提交调用链"))
    story.append(Code(
        "Client.submitApplication()\n"
        "  → ClientRMService.submitApplication()\n"
        "    → RMAppManager.submitApplication()\n"
        "      → new RMAppImpl() // 创建应用状态机\n"
        "      → rmContext.getDispatcher().getEventHandler()\n"
        "           .handle(new RMAppEvent(appId, START))\n"
        "        → RMAppImpl.RMAppNewlySavingTransition.transition()\n"
        "          → rmContext.getStateStore().storeNewApplication()\n"
        "            → [StateStore callback] RMAppEvent(APP_NEW_SAVED)\n"
        "              → AddApplicationToSchedulerTransition.transition()\n"
        "                → scheduler.handle(AppAddedSchedulerEvent)\n"
        "                  → CapacityScheduler.addApplication()\n"
        "                    → LeafQueue.submitApplication()\n"
        "                → [Scheduler callback] RMAppEvent(APP_ACCEPTED)\n"
        "                  → StartAppAttemptTransition.transition()\n"
        "                    → new RMAppAttemptImpl()\n"
        "                    → RMAppStartAttemptEvent"
    ))

    story.append(H2("13.2 AM容器启动调用链"))
    story.append(Code(
        "Scheduler.allocateContainersToNode() // AM容器分配\n"
        "  → RMContainerEvent(START) → RMContainer: NEW→ALLOCATED\n"
        "  → RMAppAttemptEvent(CONTAINER_ALLOCATED)\n"
        "    → AMContainerAllocatedTransition.transition()\n"
        "      → RMStateStore.storeNewApplicationAttempt()\n"
        "        → [callback] RMAppAttemptEvent(ATTEMPT_NEW_SAVED)\n"
        "          → AMLauncher.handle(AMLauncherEvent(LAUNCH))\n"
        "            → AMLauncher.launch() // 新线程\n"
        "              → ContainerManagementProtocol.startContainers()\n"
        "                → NM.ContainerManagerImpl.startContainers()\n"
        "                  → ContainerImpl状态机: NEW → LOCALIZING\n"
        "                  → ResourceLocalizationService (下载资源)\n"
        "                  → LOCALIZING → LOCALIZED → RUNNING"
    ))

    story.append(H2("13.3 心跳触发调度调用链"))
    story.append(Code(
        "NM.NodeStatusUpdater → RM.ResourceTrackerService.nodeHeartbeat()\n"
        "  → NMLivelinessMonitor.receivedPing(nodeId)\n"
        "  → Dispatcher.handle(RMNodeEvent(nodeId, STATUS_UPDATE))\n"
        "    → RMNodeImpl.StatusUpdateWhenHealthyTransition.transition()\n"
        "      → 解析containerStatuses, 更新nodeUpdateQueue\n"
        "      → Dispatcher.handle(NodeUpdateSchedulerEvent)\n"
        "        → CapacityScheduler.nodeUpdate(RMNode)\n"
        "          → processCompletedContainers()\n"
        "          → allocateContainersToNode(FiCaSchedulerNode)\n"
        "            → RootQueue.assignContainers()\n"
        "              → ParentQueue遍历子队列 → LeafQueue\n"
        "                → FiCaSchedulerApp.assignContainers()\n"
        "                  → 匹配ResourceRequest → 创建RMContainer"
    ))

    story.append(H2("13.4 容器完成调用链"))
    story.append(Code(
        "NM: Container进程退出\n"
        "  → ContainerImpl: RUNNING → EXITED_WITH_SUCCESS/FAILURE\n"
        "  → NM心跳上报 ContainerStatus(COMPLETE)\n"
        "  → RM.ResourceTrackerService.nodeHeartbeat()\n"
        "    → RMNodeImpl处理completedContainerStatuses\n"
        "    → Scheduler.completedContainer(RMContainer)\n"
        "      → RMContainerEvent(FINISHED) → RUNNING → COMPLETED\n"
        "      → 回收资源到节点可用资源\n"
        "      → 更新队列资源使用量\n"
        "      → RMAppAttemptEvent(CONTAINER_FINISHED)\n"
        "        → 如果是AM容器: RMAppAttempt状态机处理AM崩溃\n"
        "        → 如果是普通容器: 仅更新统计信息"
    ))

    story.append(H2("13.5 Federation请求路由调用链"))
    story.append(Code(
        "Client → Router.ClientRMService\n"
        "  → RouterClientRMService.submitApplication()\n"
        "    → ClientRequestInterceptor链:\n"
        "      → [自定义拦截器...]\n"
        "      → FederationClientInterceptor.submitApplication()\n"
        "        → policyFacade.getHomeSubcluster() // 选择目标SubCluster\n"
        "        → getClientRMProxyForSubCluster(subClusterId)\n"
        "        → proxy.submitApplication() // 转发给目标RM\n"
        "        → federationFacade.addApplicationHomeSubCluster()\n"
        "             // 记录App归属到StateStore\n"
        "        → return response"
    ))

    story.append(H2("13.6 RM故障恢复调用链"))
    story.append(Code(
        "StandbyRM 被选为新Active:\n"
        "  → ResourceManager.transitionToActive()\n"
        "    → RMActiveServices.serviceStart()\n"
        "      → RMStateStore.loadState()\n"
        "        → 加载所有持久化的Application/Attempt/SecurityToken\n"
        "      → RMAppManager.recover(RMState)\n"
        "        → 遍历所有Application:\n"
        "          → new RMAppImpl(recovered=true)\n"
        "          → RMAppEvent(RECOVER) → RMAppRecoveredTransition\n"
        "            → 根据保存的状态恢复到对应状态\n"
        "            → 恢复RMAppAttempt\n"
        "      → 等待NM重新连接:\n"
        "        → NM.nodeHeartbeat() → RECONNECTED事件\n"
        "        → RMNodeImpl重新注册\n"
        "        → 恢复Container状态 (Work-Preserving)"
    ))
    story.append(PageBreak())

    # ========== 第14章: 接口与扩展点 ==========
    story.append(H1("14. 接口与扩展点"))
    story.append(P(
        "YARN Server 通过精心设计的接口体系提供了丰富的扩展点，允许用户自定义调度策略、容器执行引擎、"
        "安全认证等核心行为。"))

    ext_data = [
        ["ResourceScheduler", "资源调度器接口", "自定义调度算法，继承AbstractYarnScheduler"],
        ["ContainerExecutor", "容器执行器", "自定义容器启动方式（Docker/Runc/自定义）"],
        ["PlacementRule", "队列放置规则", "自定义应用到队列的映射逻辑"],
        ["ClientRequestInterceptor", "客户端请求拦截器(Router)", "自定义请求处理逻辑（限流/鉴权）"],
        ["RMAdminRequestInterceptor", "管理请求拦截器(Router)", "自定义管理操作拦截"],
        ["RESTRequestInterceptor", "REST请求拦截器(Router)", "自定义REST API处理"],
        ["ConfigurationProvider", "配置提供者", "自定义配置加载方式"],
        ["RMStateStore", "状态持久化", "自定义状态存储后端"],
        ["NMStateStoreService", "NM状态持久化", "自定义NM状态恢复机制"],
        ["NodeLabelsProvider", "节点标签提供者", "自定义节点标签获取方式"],
        ["ContainerStateTransitionListener", "容器状态转移监听器", "自定义容器状态变化处理逻辑"],
        ["SystemMetricsPublisher", "系统指标发布器", "自定义指标发布到Timeline Service"],
        ["FederationPolicy", "联邦路由策略", "自定义SubCluster选择算法"],
        ["AuxiliaryService", "辅助服务(NM)", "在NM上运行自定义服务（如Shuffle Service）"],
        ["ContainerRuntime", "容器运行时", "自定义容器进程隔离和执行方式"],
    ]
    story.append(make_table(
        ["接口/扩展点", "用途", "扩展方式"],
        ext_data,
        col_widths=[52*mm, 42*mm, 76*mm]
    ))
    story.append(Caption("表 14-1: YARN Server 核心接口与扩展点"))
    story.append(PageBreak())

    # ========== 第15章: 总结 ==========
    story.append(H1("15. 总结"))
    story.append(P(
        "本文档对 Hadoop YARN Server（release-3.3.5-RC0）的核心架构进行了全面深入的分析。"
        "以下是关键发现和架构特点总结："))

    story.append(H2("15.1 架构特点"))
    story.append(Bullet(
        "<b>事件驱动架构</b>: 整个系统基于 AsyncDispatcher + EventHandler 的事件驱动模型，"
        "实现了组件间的高度解耦。所有核心操作（应用提交、容器分配、节点管理）都通过事件异步触发。"))
    story.append(Bullet(
        "<b>状态机驱动</b>: 四大核心实体（RMApp、RMAppAttempt、RMNode、RMContainer）均采用"
        "声明式状态机管理生命周期，状态转移逻辑清晰可维护。"))
    story.append(Bullet(
        "<b>可插拔设计</b>: 调度器、容器执行器、状态存储等核心组件均通过接口抽象，"
        "支持通过配置动态切换实现。"))
    story.append(Bullet(
        "<b>组合服务模式</b>: ResourceManager 和 NodeManager 使用 CompositeService 管理子服务生命周期，"
        "服务启停有序。"))
    story.append(Bullet(
        "<b>Federation 可扩展</b>: 通过 Router + GPG + FederationStateStore 支持多集群联邦，"
        "拦截器链模式使请求处理逻辑可灵活扩展。"))
    story.append(Bullet(
        "<b>HA 容错</b>: RM 支持 Active/Standby 高可用，基于 ZK 选主，"
        "Work-Preserving Recovery 保证容器在 RM 故障切换后继续运行。"))

    story.append(H2("15.2 规模数据"))
    scale_data = [
        ["模块总数", "14 个子模块"],
        ["源代码文件", "1165+ Java 文件"],
        ["最大单文件", "FederationInterceptorREST.java (164 KB)"],
        ["核心状态机", "4 个（RMApp/RMAppAttempt/RMNode/RMContainer）+ NM端容器状态机"],
        ["调度器实现", "3 种（CapacityScheduler/FairScheduler/FifoScheduler）"],
        ["容器运行时", "3 种（Default/Linux+Docker/Runc）"],
        ["核心接口", "15+ 个可扩展接口"],
        ["服务入口", "8 个可独立启动的进程"],
        ["事件类型", "7+ 种核心事件类型"],
    ]
    story.append(make_table(
        ["指标", "数值"],
        scale_data,
        col_widths=[45*mm, 125*mm]
    ))
    story.append(Caption("表 15-1: YARN Server 规模数据"))

    story.append(Spacer(1, 10*mm))
    story.append(horizontal_line())
    story.append(Spacer(1, 6*mm))
    story.append(Paragraph("— 文档结束 —", ParagraphStyle('End', fontName='ChineseFont',
        fontSize=12, alignment=TA_CENTER, textColor=HexColor('#9e9e9e'))))

    # ========== 构建 PDF ==========
    doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
    print(f"\n✅ PDF 文档已生成: {output_path}")
    return output_path


if __name__ == '__main__':
    build_document()
