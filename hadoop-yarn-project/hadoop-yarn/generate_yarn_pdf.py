#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Hadoop YARN 源码分析文档 PDF 生成器"""

import os, math
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, white, black, lightgrey, darkgrey
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus.flowables import Flowable

# 颜色
C1 = HexColor('#1a5276'); C2 = HexColor('#2e86c1'); CA = HexColor('#e74c3c')
BG1 = HexColor('#ebf5fb'); BG2 = HexColor('#f5f5f5')
CG = HexColor('#27ae60'); CO = HexColor('#f39c12'); CP = HexColor('#8e44ad')
CT = HexColor('#16a085'); CD = HexColor('#2c3e50')

# 注册中文字体
def reg_fonts():
    for fp in ['/System/Library/Fonts/STHeiti Light.ttc',
               '/System/Library/Fonts/PingFang.ttc',
               '/System/Library/Fonts/Hiragino Sans GB.ttc',
               '/Library/Fonts/Arial Unicode.ttf',
               '/System/Library/Fonts/STHeiti Medium.ttc']:
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont('CF', fp))
                return True
            except: continue
    return False

HCF = reg_fonts()
FN = 'CF' if HCF else 'Helvetica'
FB = 'CF' if HCF else 'Helvetica-Bold'

def mkstyles():
    s = getSampleStyleSheet()
    defs = [
        ('CT1', FB, 28, 36, TA_CENTER, C1, 10),
        ('CT2', FN, 16, 22, TA_CENTER, C2, 6),
        ('CT3', FN, 12, 18, TA_CENTER, darkgrey, 4),
        ('CH', FB, 22, 30, TA_LEFT, C1, 12),
        ('SEC', FB, 16, 22, TA_LEFT, C2, 8),
        ('SUB', FB, 13, 18, TA_LEFT, CD, 6),
        ('BD', FN, 10, 16, TA_JUSTIFY, black, 6),
        ('TH', FB, 9, 13, TA_CENTER, white, 0),
        ('TC', FN, 8.5, 12, TA_LEFT, black, 0),
        ('CAP', FN, 9, 13, TA_CENTER, darkgrey, 10),
        ('TOC', FN, 12, 20, TA_LEFT, CD, 0),
        ('TOC2', FN, 10, 16, TA_LEFT, darkgrey, 0),
        ('HL', FN, 10, 15, TA_LEFT, C1, 6),
    ]
    for name, fn, fs, ld, al, tc, sa in defs:
        kw = dict(fontName=fn, fontSize=fs, leading=ld, alignment=al, textColor=tc, spaceAfter=sa)
        if name == 'CH': kw['spaceBefore'] = 20
        if name == 'SEC': kw['spaceBefore'] = 16
        if name == 'SUB': kw['spaceBefore'] = 10
        if name == 'TOC': kw['leftIndent'] = 20
        if name == 'TOC2': kw['leftIndent'] = 40
        if name == 'HL': kw['backColor'] = BG1; kw['borderPadding'] = 8
        s.add(ParagraphStyle(name=name, **kw))
    s.add(ParagraphStyle(name='CB', fontName='Courier', fontSize=8, leading=11,
                         textColor=CD, backColor=BG2, leftIndent=10, rightIndent=10,
                         borderPadding=6, spaceAfter=4, spaceBefore=4))
    return s

class FlowChart(Flowable):
    def __init__(self, w=500, h=280, fn=None):
        Flowable.__init__(self)
        self._width = w; self._height = h; self.fn = fn
    def wrap(self, aw, ah): return (self._width, self._height)
    def draw(self):
        if self.fn: self.fn(self.canv, self._width, self._height)

class CodeBox(Flowable):
    def __init__(self, text, w=470, bg=BG2, tc=CD, fs=8, p=8):
        Flowable.__init__(self)
        self.text=text; self.w=w; self.bg=bg; self.tc=tc; self.fs=fs; self.p=p; self.h=0
    def wrap(self, aw, ah):
        self.h = len(self.text.split('\n'))*(self.fs*1.4)+self.p*2
        return (min(self.w,aw), self.h)
    def draw(self):
        c=self.canv; c.setFillColor(self.bg)
        c.roundRect(0,0,self.w,self.h,4,fill=1,stroke=0)
        c.setFillColor(self.tc); c.setFont('Courier',self.fs)
        y=self.h-self.p-self.fs
        for l in self.text.split('\n'):
            c.drawString(self.p,y,l); y-=self.fs*1.4

def dbox(c,x,y,w,h,t,fc,tc=white,fs=8,r=6):
    c.setFillColor(fc); c.setStrokeColor(HexColor('#bdc3c7')); c.setLineWidth(0.5)
    c.roundRect(x,y,w,h,r,fill=1,stroke=1)
    c.setFillColor(tc); c.setFont(FN,fs)
    ls=t.split('\n'); th=len(ls)*(fs+2); sy=y+h/2+th/2-fs
    for l in ls:
        tw=c.stringWidth(l,FN,fs); c.drawString(x+(w-tw)/2,sy,l); sy-=(fs+2)

def darr(c,x1,y1,x2,y2,cl=darkgrey):
    c.setStrokeColor(cl); c.setFillColor(cl); c.setLineWidth(1.2)
    c.line(x1,y1,x2,y2)
    a=math.atan2(y2-y1,x2-x1); al=6
    ax1=x2-al*math.cos(a-math.pi/6); ay1=y2-al*math.sin(a-math.pi/6)
    ax2=x2-al*math.cos(a+math.pi/6); ay2=y2-al*math.sin(a+math.pi/6)
    p=c.beginPath(); p.moveTo(x2,y2); p.lineTo(ax1,ay1); p.lineTo(ax2,ay2); p.close()
    c.drawPath(p,fill=1,stroke=0)

def dlbl(c,x,y,t,cl=darkgrey,fs=7):
    c.setFillColor(cl); c.setFont(FN,fs); c.drawString(x,y,t)

# ===== 流程图绘制函数 =====
def draw_arch(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"YARN 整体架构示意图")
    dbox(c,20,h-70,100,35,"Client\n(YarnClient)",C2)
    dbox(c,20,h-120,100,35,"Admin\n(RMAdminCLI)",CP)
    # RM border
    c.setFillColor(BG1); c.setStrokeColor(C1); c.setLineWidth(1.5)
    c.roundRect(155,h-200,210,185,8,fill=1,stroke=1)
    c.setFillColor(C1); c.setFont(FB,10); c.drawCentredString(260,h-25,"ResourceManager")
    dbox(c,165,h-65,90,28,"ClientRM\nService",C2,fs=7)
    dbox(c,265,h-65,90,28,"AdminService",CP,fs=7)
    dbox(c,165,h-105,90,28,"ApplicationMaster\nService",CT,fs=7)
    dbox(c,265,h-105,90,28,"ResourceTracker\nService",CO,fs=7)
    dbox(c,165,h-145,90,28,"Scheduler\n(CS/FS/FIFO)",CA,fs=7)
    dbox(c,265,h-145,90,28,"RMAppManager",CD,fs=7)
    dbox(c,190,h-190,135,28,"AsyncDispatcher+StateMachine",HexColor('#7f8c8d'),fs=7)
    # NM border
    c.setFillColor(HexColor('#d5f5e3')); c.setStrokeColor(CG)
    c.roundRect(400,h-200,100,185,8,fill=1,stroke=1)
    c.setFillColor(CG); c.setFont(FB,10); c.drawCentredString(450,h-25,"NodeManager")
    dbox(c,410,h-65,80,25,"ContainerMgr",CG,fs=7)
    dbox(c,410,h-100,80,25,"NodeStatus\nUpdater",CT,fs=7)
    dbox(c,410,h-135,80,25,"Container\nExecutor",CD,fs=7)
    dbox(c,410,h-170,80,25,"Resource\nLocalizer",CO,fs=7)
    dbox(c,165,h-250,125,35,"ApplicationMaster\n(AM)",CT)
    darr(c,120,h-52,165,h-52); darr(c,120,h-102,165,h-102)
    darr(c,355,h-90,410,h-90); darr(c,230,h-215,230,h-197)
    darr(c,290,h-232,410,h-145)
    dlbl(c,125,h-48,"ClientProtocol",fs=6); dlbl(c,122,h-98,"AdminProtocol",fs=6)
    dlbl(c,356,h-85,"ResourceTracker",fs=6)
    dlbl(c,310,h-220,"ContainerMgmt",fs=6)
    c.setFont(FN,7); c.setFillColor(darkgrey)
    c.drawString(20,12,"Client: 提交/查询  |  RM: 全局调度  |  NM: 节点级容器管理  |  AM: 应用协调器")

def draw_submit(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"YARN 应用提交与运行完整流程")
    bw,bh=90,28
    steps=[(30,h-60,"1.Client\ngetNewApp",C2),(140,h-60,"2.Client\nsubmitApp",C2),
           (250,h-60,"3.RMAppMgr\ncreateRMApp",C1),(360,h-60,"4.StateMachine\nNEW->SAVING",CD),
           (30,h-110,"5.StateStore\nAPP_SAVED",CO),(140,h-110,"6.Scheduler\naddApp",CA),
           (250,h-110,"7.RMApp\nACCEPTED",CG),(360,h-110,"8.AMLauncher\nlaunchAM",CT),
           (30,h-160,"9.NM\nstartAM",CG),(140,h-160,"10.AM\nregisterRM",CT),
           (250,h-160,"11.AM\nallocate()",CT),(360,h-160,"12.Scheduler\nassign",CA),
           (30,h-210,"13.AM\nstartCont",CG),(140,h-210,"14.NM\nlocalize",CG),
           (250,h-210,"15.Container\nRUNNING",CG),(360,h-210,"16.AM\nfinishApp",CT)]
    for x,y,t,cl in steps: dbox(c,x,y,bw,bh,t,cl,fs=7)
    for r in [0,4,8,12]:
        for i in range(3):
            darr(c,steps[r+i][0]+bw,steps[r+i][1]+bh/2,steps[r+i+1][0],steps[r+i+1][1]+bh/2)
    for t,b in [(3,4),(7,8),(11,12)]:
        xt=steps[t][0]+bw/2; yt=steps[t][1]; xb=steps[b][0]+bw/2; yb=steps[b][1]+bh
        my=(yt+yb)/2
        c.setStrokeColor(darkgrey); c.setLineWidth(1)
        c.line(xt,yt,xt,my); c.line(xt,my,xb,my); darr(c,xb,my,xb,yb)

def draw_rmapp_sm(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"RMApp 应用状态机")
    bw,bh=80,24
    S={'NEW':(30,h-60,C2),'NEW_SAVING':(140,h-60,C2),'SUBMITTED':(260,h-60,CO),
       'ACCEPTED':(380,h-60,CO),'RUNNING':(380,h-120,CG),'FINAL_SAVING':(220,h-120,CP),
       'FINISHING':(60,h-120,CT),'FINISHED':(30,h-180,CG),'FAILED':(150,h-180,CA),
       'KILLED':(270,h-180,CA),'KILLING':(390,h-180,CD)}
    for n,(x,y,cl) in S.items(): dbox(c,x,y,bw,bh,n,cl,fs=7)
    T=[('NEW','NEW_SAVING','START'),('NEW_SAVING','SUBMITTED','SAVED'),
       ('SUBMITTED','ACCEPTED','ACCEPTED'),('ACCEPTED','RUNNING','ATTEMPT_REG'),
       ('RUNNING','FINAL_SAVING','UNREG'),('FINAL_SAVING','FINISHING',''),
       ('FINISHING','FINISHED',''),('RUNNING','KILLING','KILL'),('KILLING','KILLED','')]
    for s,d,l in T:
        sx,sy,_=S[s]; dx,dy,_=S[d]
        if abs(sy-dy)>10:
            darr(c,sx+bw/2,sy,dx+bw/2,dy+bh,C2)
            if l: dlbl(c,(sx+dx)/2+bw/2-15,(sy+dy+bh)/2,l,CD,6)
        else:
            darr(c,sx+bw,sy+bh/2,dx,dy+bh/2,C2)
            if l: dlbl(c,(sx+bw+dx)/2-15,sy+bh/2+4,l,CD,6)

def draw_attempt_sm(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"RMAppAttempt 应用尝试状态机")
    bw,bh=80,22
    S={'NEW':(30,h-55,C2),'SUBMITTED':(140,h-55,CO),'SCHEDULED':(260,h-55,CO),
       'ALLOC_SAVE':(30,h-100,CP),'ALLOCATED':(160,h-100,CT),'LAUNCHED':(290,h-100,CT),
       'RUNNING':(400,h-100,CG),'FINAL_SAVE':(200,h-145,CP),'FINISHING':(60,h-145,CT),
       'FINISHED':(30,h-190,CG),'FAILED':(160,h-190,CA),'KILLED':(290,h-190,CA)}
    for n,(x,y,cl) in S.items(): dbox(c,x,y,bw,bh,n,cl,fs=6.5)
    T=[('NEW','SUBMITTED'),('SUBMITTED','SCHEDULED'),('SCHEDULED','ALLOC_SAVE'),
       ('ALLOC_SAVE','ALLOCATED'),('ALLOCATED','LAUNCHED'),('LAUNCHED','RUNNING'),
       ('RUNNING','FINAL_SAVE'),('FINAL_SAVE','FINISHING'),('FINISHING','FINISHED')]
    for s,d in T:
        sx,sy,_=S[s]; dx,dy,_=S[d]
        if abs(sy-dy)<5: darr(c,sx+bw,sy+bh/2,dx,dy+bh/2,C2)
        else: darr(c,sx+bw/2,sy,dx+bw/2,dy+bh,C2)

def draw_container_sm(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"NM Container 容器状态机")
    bw,bh=80,22
    S={'NEW':(30,h-55,C2),'LOCALIZING':(150,h-55,CO),'SCHEDULED':(290,h-55,CO),
       'RUNNING':(410,h-55,CG),'EXIT_OK':(30,h-105,CG),'EXIT_FAIL':(150,h-105,CA),
       'KILLING':(290,h-105,CA),'KILLED_CL':(410,h-105,CD),
       'CLEANUP':(150,h-155,CP),'DONE':(310,h-155,CD),'LOC_FAIL':(30,h-155,CA)}
    for n,(x,y,cl) in S.items(): dbox(c,x,y,bw,bh,n,cl,fs=6.5)
    T=[('NEW','LOCALIZING'),('LOCALIZING','SCHEDULED'),('SCHEDULED','RUNNING'),
       ('RUNNING','EXIT_OK'),('RUNNING','EXIT_FAIL'),('RUNNING','KILLING'),
       ('KILLING','KILLED_CL'),('EXIT_OK','CLEANUP'),('EXIT_FAIL','CLEANUP'),
       ('KILLED_CL','CLEANUP'),('CLEANUP','DONE'),('LOCALIZING','LOC_FAIL'),('LOC_FAIL','CLEANUP')]
    for s,d in T:
        sx,sy,_=S[s]; dx,dy,_=S[d]
        if abs(sy-dy)<5: darr(c,sx+bw,sy+bh/2,dx,dy+bh/2,C2)
        else: darr(c,sx+bw/2,sy,dx+bw/2,dy+bh,C2)

def draw_rmcont_sm(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"RMContainer (RM侧) 容器状态机")
    bw,bh=75,22
    S={'NEW':(30,h-55,C2),'RESERVED':(30,h-100,CO),'ALLOCATED':(150,h-55,CO),
       'ACQUIRED':(270,h-55,CT),'RUNNING':(390,h-55,CG),'COMPLETED':(390,h-110,CG),
       'EXPIRED':(270,h-110,CA),'RELEASED':(150,h-110,CD),'KILLED':(30,h-150,CA)}
    for n,(x,y,cl) in S.items(): dbox(c,x,y,bw,bh,n,cl,fs=7)
    T=[('NEW','ALLOCATED','START'),('NEW','RESERVED','RESERVED'),('RESERVED','ALLOCATED','START'),
       ('ALLOCATED','ACQUIRED','ACQUIRED'),('ACQUIRED','RUNNING','LAUNCHED'),
       ('RUNNING','COMPLETED','FINISHED'),('ALLOCATED','EXPIRED','EXPIRE'),
       ('ACQUIRED','RELEASED','RELEASED')]
    for s,d,l in T:
        sx,sy,_=S[s]; dx,dy,_=S[d]
        if abs(sy-dy)<5: darr(c,sx+bw,sy+bh/2,dx,dy+bh/2,C2)
        else: darr(c,sx+bw/2,sy,dx+bw/2,dy+bh,C2)
        if l: dlbl(c,(sx+dx)/2+bw/2-10,(sy+dy)/2+bh/2+3,l,CD,6)

def draw_rmnode_sm(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"RMNode 节点状态机")
    bw,bh=90,24
    S={'NEW':(30,h-55,C2),'RUNNING':(180,h-55,CG),'UNHEALTHY':(350,h-55,CA),
       'DECOM_ING':(50,h-115,CO),'DECOM_ED':(210,h-115,CD),
       'LOST':(370,h-115,CA),'REBOOTED':(50,h-165,CP),'SHUTDOWN':(210,h-165,CD)}
    for n,(x,y,cl) in S.items(): dbox(c,x,y,bw,bh,n,cl,fs=7)
    T=[('NEW','RUNNING','STARTED'),('RUNNING','UNHEALTHY','UNHEALTHY'),
       ('UNHEALTHY','RUNNING','HEALTHY'),('RUNNING','DECOM_ING','GRACEFUL'),
       ('DECOM_ING','DECOM_ED','DECOM'),('RUNNING','LOST','EXPIRE'),
       ('RUNNING','REBOOTED','REBOOT'),('RUNNING','SHUTDOWN','SHUTDOWN')]
    for s,d,l in T:
        sx,sy,_=S[s]; dx,dy,_=S[d]
        if abs(sy-dy)<5:
            darr(c,sx+bw,sy+bh/2,dx,dy+bh/2,C2)
            dlbl(c,(sx+bw+dx)/2-15,sy+bh/2+4,l,CD,5.5)
        else:
            darr(c,sx+bw/2,sy,dx+bw/2,dy+bh,C2)
            dlbl(c,(sx+dx)/2+bw/2+2,(sy+dy+bh)/2+2,l,CD,5.5)

def draw_sched(c,w,h):
    c.setFont(FB,11); c.setFillColor(C1)
    c.drawCentredString(w/2,h-15,"CapacityScheduler 资源分配流程")
    bw,bh=115,25
    S=[(195,h-55,"NM Heartbeat\n(NODE_UPDATE)",CO),(195,h-95,"CapacityScheduler\nhandle()",CA),
       (195,h-135,"allocateContainers\nToNode()",C1),(30,h-175,"Root Queue\nassignContainers()",C2),
       (180,h-175,"Parent Queue\n(sort children)",CT),(330,h-175,"Leaf Queue\n(select app)",CG),
       (330,h-215,"ContainerAllocator\nassign()",CD),(140,h-215,"commit or\nreserve?",CP),
       (30,h-255,"Reserve on\nNode",CO),(260,h-255,"submitResource\nCommitRequest()",CG)]
    for x,y,t,cl in S: dbox(c,x,y,bw,bh,t,cl,fs=7)
    for i in range(3): darr(c,252,S[i][1],252,S[i+1][1]+bh)
    darr(c,30+bw,h-175+bh/2,180,h-175+bh/2)
    darr(c,180+bw,h-175+bh/2,330,h-175+bh/2)
    darr(c,387,h-175,387,h-215+bh)
    darr(c,330,h-215+bh/2,140+bw,h-215+bh/2)
    darr(c,197,h-215,85,h-255+bh)
    darr(c,197,h-215,317,h-255+bh)
    dlbl(c,100,h-238,"reserve",CA,6); dlbl(c,260,h-238,"commit",CG,6)

# ===== 表格辅助 =====
def mktbl(data, widths, header_bg=C1):
    t = Table(data, colWidths=widths)
    t.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),header_bg),('TEXTCOLOR',(0,0),(-1,0),white),
        ('GRID',(0,0),(-1,-1),0.5,lightgrey),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[white,BG2]),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    return t

def P(t,s='TC'): return Paragraph(t,styles[s])
def PH(t): return Paragraph(t,styles['TH'])

# ===== 构建文档 =====
styles = mkstyles()

def build():
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),"Hadoop_YARN_源码分析文档.pdf")
    doc = SimpleDocTemplate(out, pagesize=A4, topMargin=2*cm, bottomMargin=2*cm,
                            leftMargin=2*cm, rightMargin=2*cm,
                            title="Hadoop YARN 源码分析文档", author="YARN Analyzer")
    story = []

    # ===== 封面 =====
    story.append(Spacer(1,80))
    story.append(Paragraph("Hadoop YARN",styles['CT1']))
    story.append(Paragraph("源码架构分析文档",styles['CT1']))
    story.append(Spacer(1,20))
    story.append(HRFlowable(width="60%",thickness=2,color=C2))
    story.append(Spacer(1,20))
    story.append(Paragraph("核心流程图 + 关键类设计说明",styles['CT2']))
    story.append(Spacer(1,10))
    story.append(Paragraph("基于 Hadoop 3.6.0-SNAPSHOT (release-3.3.5-RC0)",styles['CT3']))
    story.append(Spacer(1,30))
    story.append(Paragraph("版本: v1.0",styles['CT3']))
    story.append(PageBreak())

    # ===== 目录 =====
    story.append(Paragraph("目 录",styles['CH']))
    story.append(HRFlowable(width="100%",thickness=1.5,color=C1))
    story.append(Spacer(1,10))
    toc=[("第一章 YARN 整体架构概述",["1.1 模块全景","1.2 整体架构图","1.3 核心设计模式"]),
         ("第二章 核心流程图",["2.1 应用提交与运行流程","2.2 RMApp 应用状态机",
          "2.3 RMAppAttempt 尝试状态机","2.4 NM Container 容器状态机",
          "2.5 RMContainer 容器状态机","2.6 RMNode 节点状态机","2.7 CapacityScheduler 资源分配"]),
         ("第三章 ResourceManager 核心类设计",["3.1 ResourceManager 主入口","3.2 ClientRMService",
          "3.3 RMAppManager","3.4 RMAppImpl","3.5 RMNodeImpl"]),
         ("第四章 调度器设计",["4.1 调度器继承体系","4.2 CapacityScheduler",
          "4.3 FairScheduler","4.4 FifoScheduler"]),
         ("第五章 NodeManager 核心类设计",["5.1 NodeManager 主入口","5.2 ContainerManagerImpl",
          "5.3 ContainerLaunch","5.4 ContainerImpl","5.5 NodeStatusUpdater"]),
         ("第六章 协议与客户端 API",["6.1 四大协议接口","6.2 YarnClient","6.3 AMRMClient","6.4 NMClient"])]
    for ch,secs in toc:
        story.append(Paragraph(ch,styles['TOC']))
        for s in secs: story.append(Paragraph(s,styles['TOC2']))
    story.append(PageBreak())

    # ===== 第一章 =====
    story.append(Paragraph("第一章 YARN 整体架构概述",styles['CH']))
    story.append(HRFlowable(width="100%",thickness=1.5,color=C1))
    story.append(Paragraph("1.1 模块全景",styles['SEC']))
    story.append(Paragraph("Hadoop YARN (Yet Another Resource Negotiator) 是 Hadoop 的资源管理和调度平台。整个 YARN 项目由以下核心模块组成:",styles['BD']))

    story.append(mktbl([
        [PH('<b>模块</b>'),PH('<b>说明</b>'),PH('<b>文件数</b>')],
        [P('hadoop-yarn-api'),P('协议接口、数据记录(records)、配置常量'),P('272')],
        [P('hadoop-yarn-common'),P('事件框架(AsyncDispatcher)、状态机(StateMachineFactory)、RPC、安全机制'),P('500+')],
        [P('hadoop-yarn-server'),P('ResourceManager、NodeManager、Router、Timeline Service 等14个子模块'),P('1500+')],
        [P('hadoop-yarn-client'),P('客户端SDK (YarnClient/AMRMClient/NMClient) 与CLI工具'),P('43')],
        [P('hadoop-yarn-applications'),P('示例应用 (DistributedShell) 和 YARN Services 框架'),P('100+')],
        [P('hadoop-yarn-registry'),P('基于ZooKeeper的服务发现和注册'),P('70+')],
    ],[120,280,70]))
    story.append(Paragraph("表1-1: YARN 核心模块一览",styles['CAP']))

    story.append(Paragraph("1.2 整体架构图",styles['SEC']))
    story.append(FlowChart(500,270,draw_arch))
    story.append(Paragraph("图1-1: YARN 整体架构示意图",styles['CAP']))

    story.append(Paragraph("1.3 核心设计模式",styles['SEC']))
    story.append(Paragraph("YARN 采用 <b>事件驱动 + 有限状态机</b> 的核心设计模式。所有关键组件均通过 StateMachineFactory 定义状态转换, 由 AsyncDispatcher 异步分发事件, 实现组件间的松耦合:",styles['BD']))

    story.append(mktbl([
        [PH('<b>设计模式</b>'),PH('<b>实现类</b>'),PH('<b>说明</b>')],
        [P('事件驱动'),P('AsyncDispatcher, EventHandler'),P('所有组件通过事件解耦, 不同事件类型注册不同Handler')],
        [P('状态机'),P('StateMachineFactory'),P('RMApp/RMAppAttempt/RMNode/Container 均采用FSM管理')],
        [P('服务框架'),P('CompositeService'),P('统一init/start/stop生命周期, 支持服务组合')],
        [P('双层服务'),P('Always-On + Active-Only'),P('支持HA主备切换, Active服务仅在主RM运行')],
        [P('读写锁'),P('ReentrantReadWriteLock'),P('核心类均使用读写锁分离, 保护并发访问')],
    ],[80,160,230]))
    story.append(Paragraph("表1-2: YARN 核心设计模式",styles['CAP']))
    story.append(PageBreak())

    # ===== 第二章 =====
    story.append(Paragraph("第二章 核心流程图",styles['CH']))
    story.append(HRFlowable(width="100%",thickness=1.5,color=C1))

    story.append(Paragraph("2.1 应用提交与运行流程",styles['SEC']))
    story.append(Paragraph("一个 YARN 应用从提交到运行完成, 需要经过16个关键步骤, 涉及 Client、ResourceManager、NodeManager、ApplicationMaster 四方交互:",styles['BD']))
    story.append(FlowChart(500,250,draw_submit))
    story.append(Paragraph("图2-1: 应用提交与运行完整流程",styles['CAP']))
    story.append(Paragraph("<b>流程说明:</b> Client 先获取 ApplicationId, 然后提交应用; RM 创建 RMApp 状态机并持久化到 StateStore; Scheduler 接受应用后创建 AppAttempt; AMLauncher 在某个 NM 上启动 AM 容器; AM 向 RM 注册并通过 allocate() 心跳申请资源; 获得容器后 AM 通过 ContainerManagementProtocol 在 NM 上启动任务容器; 任务完成后 AM 调用 finishApplicationMaster() 注销。",styles['BD']))
    story.append(PageBreak())

    story.append(Paragraph("2.2 RMApp 应用状态机",styles['SEC']))
    story.append(Paragraph("RMAppImpl 定义了11个状态和16种事件类型, 状态机由 StateMachineFactory 构建:",styles['BD']))
    story.append(FlowChart(500,200,draw_rmapp_sm))
    story.append(Paragraph("图2-2: RMApp 应用状态机",styles['CAP']))

    story.append(mktbl([
        [PH('<b>源状态</b>'),PH('<b>事件</b>'),PH('<b>目标状态</b>'),PH('<b>处理类</b>')],
        [P('NEW'),P('START'),P('NEW_SAVING'),P('RMAppNewlySavingTransition')],
        [P('NEW_SAVING'),P('APP_NEW_SAVED'),P('SUBMITTED'),P('AddApplicationToSchedulerTransition')],
        [P('SUBMITTED'),P('APP_ACCEPTED'),P('ACCEPTED'),P('StartAppAttemptTransition')],
        [P('ACCEPTED'),P('ATTEMPT_REGISTERED'),P('RUNNING'),P('RMAppStateUpdateTransition')],
        [P('RUNNING'),P('ATTEMPT_UNREGISTERED'),P('FINAL_SAVING'),P('FinalSavingTransition')],
        [P('ACCEPTED/RUNNING'),P('ATTEMPT_FAILED'),P('ACCEPTED/FINAL_SAVING'),P('AttemptFailedTransition(判断重试)')],
        [P('ACCEPTED/RUNNING'),P('KILL'),P('KILLING'),P('KillAttemptTransition')],
    ],[90,100,110,170]))
    story.append(Paragraph("表2-1: RMApp 关键状态转换",styles['CAP']))
    story.append(PageBreak())

    story.append(Paragraph("2.3 RMAppAttempt 尝试状态机",styles['SEC']))
    story.append(Paragraph("每个 RMApp 可以有多次尝试(Attempt)。RMAppAttemptImpl 定义了12个状态, 管理从 AM 容器分配、启动到注册、运行的完整生命周期:",styles['BD']))
    story.append(FlowChart(500,210,draw_attempt_sm))
    story.append(Paragraph("图2-3: RMAppAttempt 应用尝试状态机",styles['CAP']))
    story.append(Paragraph("<b>关键机制:</b> SCHEDULED 状态等待 Scheduler 分配 AM 容器; ALLOCATED_SAVING 先持久化到 StateStore 再真正分配; LAUNCHED 状态等待 AM 注册(有 AMLivelinessMonitor 超时监控); 非托管 AM 直接跳过容器分配进入 LAUNCHED_UNMANAGED_SAVING 路径。",styles['BD']))
    story.append(PageBreak())

    story.append(Paragraph("2.4 NM Container 容器状态机",styles['SEC']))
    story.append(Paragraph("NodeManager 端的 ContainerImpl 定义了17个状态, 管理容器从初始化、资源本地化、调度、运行到退出的完整生命周期:",styles['BD']))
    story.append(FlowChart(500,190,draw_container_sm))
    story.append(Paragraph("图2-4: NM Container 容器状态机(简化)",styles['CAP']))
    story.append(Paragraph("<b>核心路径:</b> NEW -> LOCALIZING -> SCHEDULED -> RUNNING -> EXITED_WITH_SUCCESS -> CLEANUP -> DONE。KILL 事件可在任何运行中状态触发。支持重试策略 (ContainerRetryContext)。",styles['BD']))

    story.append(Paragraph("2.5 RMContainer (RM侧) 容器状态机",styles['SEC']))
    story.append(FlowChart(500,170,draw_rmcont_sm))
    story.append(Paragraph("图2-5: RMContainer RM侧容器状态机",styles['CAP']))
    story.append(Paragraph("<b>说明:</b> RMContainer 是 RM 视角的容器抽象。NEW -> ALLOCATED -> ACQUIRED -> RUNNING -> COMPLETED 为主线; 支持 RESERVED(预留) 路径; 未被及时拉取的容器会 EXPIRE。",styles['BD']))
    story.append(PageBreak())

    story.append(Paragraph("2.6 RMNode 节点状态机",styles['SEC']))
    story.append(FlowChart(500,200,draw_rmnode_sm))
    story.append(Paragraph("图2-6: RMNode 节点状态机",styles['CAP']))
    story.append(Paragraph("<b>说明:</b> 节点注册后进入 RUNNING/UNHEALTHY; 心跳维持 RUNNING; 超时进入 LOST; 支持优雅退役(GRACEFUL_DECOMMISSION) 和立即退役(DECOMMISSION); UNHEALTHY 恢复健康自动回到 RUNNING。",styles['BD']))

    story.append(Paragraph("2.7 CapacityScheduler 资源分配流程",styles['SEC']))
    story.append(FlowChart(500,280,draw_sched))
    story.append(Paragraph("图2-7: CapacityScheduler 资源分配流程",styles['CAP']))
    story.append(Paragraph("<b>分配算法:</b> NM 心跳触发 NODE_UPDATE; 从 Root 队列开始按子队列已用容量比排序, 自顶向下递归 assignContainers(); Leaf Queue 选择应用, ContainerAllocator 执行实际分配; 资源不足则 Reserve; 分配结果通过 submitResourceCommitRequest() 提交。",styles['BD']))
    story.append(PageBreak())

    # ===== 第三章 =====
    story.append(Paragraph("第三章 ResourceManager 核心类设计",styles['CH']))
    story.append(HRFlowable(width="100%",thickness=1.5,color=C1))

    story.append(Paragraph("3.1 ResourceManager 主入口",styles['SEC']))
    story.append(Paragraph("ResourceManager 继承 CompositeService, 是 YARN 集群的全局协调者。采用双层服务设计: Always-On 层和 Active 层:",styles['BD']))
    story.append(mktbl([
        [PH('<b>组件</b>'),PH('<b>层次</b>'),PH('<b>职责</b>')],
        [P('rmDispatcher'),P('Always-On'),P('全局事件分发器, 注册各类EventHandler')],
        [P('adminService'),P('Always-On'),P('处理管理员命令(刷新队列/节点等)')],
        [P('clientRM'),P('Active'),P('处理客户端RPC(提交/查询/杀死应用)')],
        [P('masterService'),P('Active'),P('处理AM心跳和资源分配请求')],
        [P('resourceTracker'),P('Active'),P('处理NM注册和心跳')],
        [P('scheduler'),P('Active'),P('资源调度(CapacityScheduler/FairScheduler)')],
        [P('rmAppManager'),P('Active'),P('应用生命周期管理(提交/完成/清理)')],
        [P('nodesListManager'),P('Active'),P('节点白名单/黑名单管理')],
        [P('nmLivelinessMonitor'),P('Active'),P('NM存活检测(心跳超时处理)')],
    ],[110,80,280]))
    story.append(Paragraph("表3-1: ResourceManager 核心组件",styles['CAP']))
    story.append(Paragraph("<b>初始化流程 (serviceInit):</b> 创建 RMContextImpl -> 安全登录 -> 加载配置 -> 创建 AsyncDispatcher -> 创建 AdminService -> HA选举(如启用) -> 创建 Active 服务集 -> 注册各事件类型的 Handler。",styles['BD']))

    story.append(Paragraph("3.2 ClientRMService",styles['SEC']))
    story.append(Paragraph("实现 ApplicationClientProtocol 接口, 是客户端与 RM 通信的 RPC 服务端:",styles['BD']))
    story.append(mktbl([
        [PH('<b>RPC方法</b>'),PH('<b>功能</b>'),PH('<b>内部调用</b>')],
        [P('getNewApplication()'),P('生成新的ApplicationId'),P('applicationCounter.incrementAndGet()')],
        [P('submitApplication()'),P('提交应用'),P('rmAppManager.submitApplication()')],
        [P('killApplication()'),P('杀死应用'),P('发送 RMAppKillByClientEvent')],
        [P('getApplicationReport()'),P('查询应用状态'),P('rmApp.createAndGetApplicationReport()')],
        [P('getClusterMetrics()'),P('查询集群指标'),P('scheduler.getClusterResource()')],
        [P('moveApplication()'),P('跨队列移动应用'),P('scheduler.moveApplication()')],
    ],[130,120,220]))
    story.append(Paragraph("表3-2: ClientRMService 核心RPC方法",styles['CAP']))

    story.append(Paragraph("3.3 RMAppManager",styles['SEC']))
    story.append(Paragraph("负责应用的提交、完成和清理。核心方法 submitApplication() 创建 RMAppImpl 实例, 发送 START 事件触发状态机; finishApplication() 执行收尾; checkAppNumCompletedLimit() 按 LRU 策略淘汰已完成应用, 维护内存和 StateStore 上限。实现 Recoverable 接口, 支持 RM 重启后从 StateStore 恢复应用。",styles['BD']))

    story.append(Paragraph("3.4 RMAppImpl",styles['SEC']))
    story.append(Paragraph("RMApp 的默认实现, 核心是 StateMachineFactory 构建的状态机(详见2.2节):",styles['BD']))
    story.append(mktbl([
        [PH('<b>字段</b>'),PH('<b>类型</b>'),PH('<b>说明</b>')],
        [P('applicationId'),P('ApplicationId'),P('应用唯一ID(不可变)')],
        [P('submissionContext'),P('ApplicationSubmissionContext'),P('应用提交上下文(含AM启动信息)')],
        [P('attempts'),P('LinkedHashMap'),P('所有尝试实例(按插入顺序)')],
        [P('currentAttempt'),P('volatile RMAppAttempt'),P('当前活跃尝试')],
        [P('stateMachine'),P('StateMachine'),P('状态机实例')],
        [P('updatedNodes'),P('Map'),P('待处理的节点更新')],
    ],[110,140,220]))
    story.append(Paragraph("表3-3: RMAppImpl 核心字段",styles['CAP']))

    story.append(Paragraph("3.5 RMNodeImpl",styles['SEC']))
    story.append(Paragraph("RM 视角的节点抽象, 实现事件驱动的状态机模式(详见2.6节)。维护节点资源(totalCapability/allocatedResource)、已启动容器集合、待清理容器/应用列表、容器更新队列、节点利用率等。StatusUpdateWhenHealthyTransition 处理心跳中的容器状态更新, 并通过 NODE_UPDATE 事件触发 Scheduler 进行资源分配。",styles['BD']))
    story.append(PageBreak())

    # ===== 第四章 =====
    story.append(Paragraph("第四章 调度器设计",styles['CH']))
    story.append(HRFlowable(width="100%",thickness=1.5,color=C1))

    story.append(Paragraph("4.1 调度器继承体系",styles['SEC']))
    story.append(CodeBox("AbstractService\n  +-- AbstractYarnScheduler<T, N> implements ResourceScheduler\n        +-- CapacityScheduler (default)\n        +-- FairScheduler\n        +-- FifoScheduler",w=470))
    story.append(Paragraph("图4-1: 调度器类继承体系",styles['CAP']))
    story.append(Paragraph("AbstractYarnScheduler 提供公共能力: 集群节点追踪(ClusterNodeTracker)、应用映射表、读写锁、定期更新线程、容器生命周期管理和最小/最大资源配置。",styles['BD']))

    story.append(Paragraph("4.2 CapacityScheduler (默认调度器)",styles['SEC']))
    story.append(Paragraph("YARN 生产环境的默认调度器, 采用树形层级队列实现多租户资源隔离:",styles['BD']))
    story.append(mktbl([
        [PH('<b>特性</b>'),PH('<b>说明</b>')],
        [P('层级队列'),P('CSQueue -> AbstractCSQueue -> ParentQueue/LeafQueue 树形结构')],
        [P('容量配置'),P('支持百分比模式和权重(weight)模式; 有 capacity/maxCapacity 两层配置')],
        [P('弹性共享'),P('空闲资源可被其他队列借用, 当自身需要时通过抢占收回')],
        [P('抢占'),P('PreemptionManager + SchedulingMonitor; 支持懒抢占(先标记后回收)')],
        [P('异步调度'),P('AsyncSchedulingConfiguration 配置独立调度线程')],
        [P('多节点放置'),P('multiNodePlacementEnabled, 支持跨节点寻找最优放置')],
        [P('动态队列'),P('ParentQueue.addDynamicLeafQueue() 运行时自动创建')],
        [P('动态配置'),P('MutableCSConfigurationProvider 支持不重启修改队列配置')],
        [P('节点标签'),P('RMNodeLabelsManager 实现节点分区, 队列绑定特定标签')],
    ],[90,380]))
    story.append(Paragraph("表4-1: CapacityScheduler 核心特性",styles['CAP']))

    story.append(Paragraph("4.3 FairScheduler",styles['SEC']))
    story.append(Paragraph("公平调度器通过 update() 方法定期更新: 递归计算各队列资源需求(updateDemand), 基于公平份额算法分配(update), 更新指标。支持数据局部性调度(node/rack locality), 配置文件热加载(AllocationFileLoaderService), 以及抢占(updateStarvedApps)。",styles['BD']))

    story.append(Paragraph("4.4 FifoScheduler",styles['SEC']))
    story.append(Paragraph("最简单的调度器, 只有一个全局默认队列(DEFAULT_QUEUE, 容量100%)。使用 ConcurrentSkipListMap 按应用提交顺序排序, 严格 FIFO 分配资源。仅使用 DefaultResourceCalculator(只按内存计算)。适合测试和简单场景。",styles['BD']))
    story.append(PageBreak())

    # ===== 第五章 =====
    story.append(Paragraph("第五章 NodeManager 核心类设计",styles['CH']))
    story.append(HRFlowable(width="100%",thickness=1.5,color=C1))

    story.append(Paragraph("5.1 NodeManager 主入口",styles['SEC']))
    story.append(Paragraph("NodeManager 继承 CompositeService, 是每个集群节点上的代理进程, 通过工厂方法组装以下子服务:",styles['BD']))
    story.append(mktbl([
        [PH('<b>子服务</b>'),PH('<b>职责</b>')],
        [P('NMStateStoreService'),P('LevelDB 状态持久化, 支持 NM 重启恢复容器')],
        [P('ContainerExecutor'),P('容器执行器(Default/Linux/Docker), 通过反射创建')],
        [P('DeletionService'),P('异步文件删除服务')],
        [P('NodeHealthCheckerService'),P('节点健康检查(磁盘/脚本)')],
        [P('NodeStatusUpdaterImpl'),P('向 RM 注册节点 + 定期心跳线程')],
        [P('ContainerManagerImpl'),P('容器生命周期管理(启动/监控/本地化/日志聚合)')],
        [P('WebServer'),P('Web UI + REST API (NMWebServices)')],
        [P('AsyncDispatcher'),P('NM 级全局异步事件分发')],
    ],[140,330]))
    story.append(Paragraph("表5-1: NodeManager 子服务组成",styles['CAP']))

    story.append(Paragraph("5.2 ContainerManagerImpl",styles['SEC']))
    story.append(Paragraph("自身也是 CompositeService, 实现 ContainerManagementProtocol。构造函数中注册8种事件处理器:",styles['BD']))
    story.append(mktbl([
        [PH('<b>事件类型</b>'),PH('<b>处理器</b>'),PH('<b>说明</b>')],
        [P('ContainerEventType'),P('ContainerEventDispatcher'),P('路由到具体Container实例')],
        [P('ApplicationEventType'),P('ApplicationEventDispatcher'),P('路由到NM端Application')],
        [P('LocalizationEventType'),P('ResourceLocalizationService'),P('资源下载本地化')],
        [P('ContainersLauncherEventType'),P('ContainersLauncher'),P('容器启动/清理/信号')],
        [P('ContainerSchedulerEventType'),P('ContainerScheduler'),P('保证型+机会型容器调度')],
        [P('ContainersMonitorEventType'),P('ContainersMonitor'),P('容器资源使用监控')],
        [P('LogHandlerEventType'),P('LogHandler'),P('日志聚合/非聚合处理')],
    ],[140,140,190]))
    story.append(Paragraph("表5-2: ContainerManagerImpl 事件注册",styles['CAP']))

    story.append(Paragraph("5.3 ContainerLaunch 启动流程",styles['SEC']))
    story.append(Paragraph("ContainerLaunch 实现 Callable 接口, 由 ContainersLauncher 线程池异步执行:",styles['BD']))
    story.append(mktbl([
        [PH('<b>步骤</b>'),PH('<b>操作</b>')],
        [P('1'),P('验证容器状态 (validateContainerState)')],
        [P('2'),P('获取已本地化的资源列表')],
        [P('3'),P('变量展开: 替换 LOG_DIR、CLASS_PATH_SEPARATOR 等占位符')],
        [P('4'),P('确定工作目录、日志目录、PID文件路径')],
        [P('5'),P('检查磁盘健康状态')],
        [P('6'),P('写入 keystore/truststore (安全集群)')],
        [P('7'),P('写入容器启动脚本 (launch_container.sh)')],
        [P('8'),P('调用 ContainerExecutor.launchContainer() -- 阻塞直到容器退出')],
        [P('9'),P('根据退出码发送 EXITED_WITH_SUCCESS 或 EXITED_WITH_FAILURE')],
    ],[40,430]))
    story.append(Paragraph("表5-3: ContainerLaunch 启动步骤",styles['CAP']))

    story.append(Paragraph("5.4 ContainerImpl 状态机",styles['SEC']))
    story.append(Paragraph("NM 端容器的核心实现(详见2.4节)。17个状态, 支持资源本地化、暂停/恢复、重初始化、升级回滚和重试策略(ContainerRetryContext)。采用读写锁保护并发, volatile 保证 launchContext 可见性。",styles['BD']))

    story.append(Paragraph("5.5 NodeStatusUpdaterImpl 心跳",styles['SEC']))
    story.append(Paragraph("负责 NM 与 RM 的通信。生命周期: serviceInit(获取节点资源配置) -> serviceStart(创建RPC代理, 注册节点, 启动心跳线程) -> serviceStop(注销节点)。心跳线程循环构建 NodeHeartbeatRequest 并发送; 处理 RM 返回的指令: SHUTDOWN/RESYNC/NORMAL; 更新 MasterKey; 支持 sendOutofBandHeartBeat() 立即通知。",styles['BD']))
    story.append(PageBreak())

    # ===== 第六章 =====
    story.append(Paragraph("第六章 协议与客户端 API",styles['CH']))
    story.append(HRFlowable(width="100%",thickness=1.5,color=C1))

    story.append(Paragraph("6.1 四大协议接口",styles['SEC']))
    story.append(mktbl([
        [PH('<b>协议</b>'),PH('<b>通信双方</b>'),PH('<b>核心方法</b>')],
        [P('ApplicationClientProtocol'),P('Client <-> RM'),P('getNewApplication, submitApplication, forceKillApplication, getApplicationReport, getClusterMetrics, getQueueInfo')],
        [P('ApplicationMasterProtocol'),P('AM <-> RM'),P('registerApplicationMaster, allocate (核心心跳+资源请求), finishApplicationMaster')],
        [P('ContainerManagementProtocol'),P('AM <-> NM'),P('startContainers, stopContainers, getContainerStatuses, updateContainer, reInitializeContainer')],
        [P('ResourceManagerAdministrationProtocol'),P('Admin <-> RM'),P('refreshQueues, refreshNodes, updateNodeResource, addToClusterNodeLabels')],
    ],[120,75,275]))
    story.append(Paragraph("表6-1: YARN 四大协议接口",styles['CAP']))

    story.append(Paragraph("<b>allocate() 方法</b> 是整个 YARN 资源调度的核心入口, 它同时兼具: 发送 ResourceRequest 列表请求新容器、归还不需要的容器、更新黑名单、作为心跳保持 AM 存活、返回已分配容器和 headroom 信息。标注为 @AtMostOnce 语义。",styles['BD']))

    story.append(Paragraph("6.2 YarnClient",styles['SEC']))
    story.append(Paragraph("封装 ApplicationClientProtocol, 继承 AbstractService。核心方法: createApplication() 获取新ID; submitApplication() 提交并轮询确认; killApplication() 杀死并轮询; getApplicationReport() 查询(支持回退到AHS)。采用异步轮询确认模式, 处理 RM HA 故障转移时的自动重提交。",styles['BD']))

    story.append(Paragraph("6.3 AMRMClient",styles['SEC']))
    story.append(Paragraph("封装 ApplicationMasterProtocol, 核心方法: registerApplicationMaster() 注册; allocate(progressIndicator) 心跳+资源请求; unregisterApplicationMaster() 注销。内含 ContainerRequest 内部类封装容器需求(capability/nodes/racks/priority/executionType)。本地维护 RemoteRequestsTable, 批量发送。支持 ApplicationMasterNotRegisteredException 时自动重注册。",styles['BD']))

    story.append(Paragraph("6.4 NMClient",styles['SEC']))
    story.append(Paragraph("封装 ContainerManagementProtocol, 通过 ContainerManagementProtocolProxy 管理与多个 NM 的代理连接。核心方法: startContainer(), stopContainer(), getContainerStatus(), updateContainerResource(), reInitializeContainer()。使用 ConcurrentMap 跟踪已启动容器, synchronized 防止 start/stop 竞争。默认停止时自动清理所有运行中容器。",styles['BD']))

    story.append(Spacer(1,30))
    story.append(HRFlowable(width="100%",thickness=1,color=lightgrey))
    story.append(Spacer(1,10))
    story.append(Paragraph("— 文档结束 —",styles['CAP']))

    # Build
    doc.build(story)
    print(f"\n✅ PDF 文档已生成: {out}")
    return out

if __name__ == '__main__':
    build()
