from fastapi import FastAPI, UploadFile, File, Response
from fastapi.middleware.cors import CORSMiddleware
import io
import json
import traceback
import pandas as pd
from openpyxl import load_workbook
from collections import Counter
from typing import Dict, List
import tempfile
import os
from datetime import datetime
import re

app = FastAPI(title="Excel Commission Calculator")

# 允许所有来源访问（重要！）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def process_excel_data(contents):
    """处理Excel数据的通用函数"""
    excel_bytes = io.BytesIO(contents)
    wb = load_workbook(excel_bytes, data_only=True)
    sheet = wb.active

    # 读取所有数据
    max_row = sheet.max_row
    max_col = sheet.max_column
    all_data = []
    
    for row_idx in range(1, max_row + 1):
        row_data = []
        for col_idx in range(1, max_col + 1):
            cell_value = sheet.cell(row=row_idx, column=col_idx).value
            row_data.append(cell_value)
        
        if any(cell is not None and str(cell).strip() != "" for cell in row_data):
            all_data.append(row_data)
    
    if not all_data:
        return None, "Excel文件为空"

    # 检测标题行
    header_row_index = None
    for i, row in enumerate(all_data):
        row_text = " ".join(str(cell) for cell in row if cell)
        if any(keyword in row_text for keyword in ["代理", "ID", "金额", "BONUS", "上分"]):
            header_row_index = i
            break

    if header_row_index is None:
        header_row_index = 0

    # 创建DataFrame
    headers = [str(c).strip() if c else f"Column_{i}" for i, c in enumerate(all_data[header_row_index])]
    data_rows = all_data[header_row_index + 1:]
    
    df = pd.DataFrame(data_rows, columns=headers)
    df = df.dropna(how="all")
    df = df.reset_index(drop=True)
    df = df.where(pd.notnull(df), None)
    
    return df, None

def extract_agent_number(agent_str):
    """从代理序号中提取数字，处理各种格式"""
    if not agent_str:
        return 0
    
    agent_str = str(agent_str).strip()
    
    # 尝试提取数字
    numbers = re.findall(r'\d+', agent_str)
    if numbers:
        return int(numbers[0])
    else:
        return 0

def sort_by_agent_number(df):
    """按代理序号排序：先按上分地方，再按代理序号"""
    if '上分地方' in df.columns and '代理序号' in df.columns:
        # 创建排序键：先按地点，再按代理序号类型，最后按数字
        def sort_key(row):
            location = str(row['上分地方']) if row['上分地方'] is not None else ""
            agent = str(row['代理序号']) if row['代理序号'] is not None else ""
            
            # 地点排序
            location_order = {
                "BELLA": 1,
                "肥子代理 638": 2,
                "OC619-01-01": 3,
                "WS": 4,
                "OC619-01-01-01": 5
            }
            
            # 代理序号排序
            agent_upper = agent.upper()
            if "IN" in agent_upper:
                agent_type = 1  # IN类型在前
            elif "OUT" in agent_upper:
                agent_type = 2  # OUT类型在后
            else:
                agent_type = 3  # 其他类型在最后
            
            # 提取数字
            agent_number = extract_agent_number(agent)
            
            return (location_order.get(location, 99), agent_type, agent_number, location, agent)
        
        # 应用排序
        df_sorted = df.copy()
        df_sorted['sort_key'] = df_sorted.apply(sort_key, axis=1)
        df_sorted = df_sorted.sort_values('sort_key')
        df_sorted = df_sorted.drop('sort_key', axis=1)
        return df_sorted.reset_index(drop=True)
    else:
        # 如果缺少列，就按原始顺序
        return df

def calculate_commission(location, total_amount):
    """计算佣金分配"""
    if "肥子代理 638" in str(location):
        # 肥子代理 638: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            "肥子代理 638": commission
        }
        return total_amount, commission, commission_rate, breakdown
    
    elif "OC619" in str(location):
        # OC619系列: 分层分配 30%
        commission_rate = 0.30
        total_commission = total_amount * commission_rate
        
        # 分层分配
        breakdown = {
            "OC619-01-01": total_amount * 0.05,  # 5%
            "OC619": total_amount * 0.05,        # 5%
            "OC619-01": total_amount * 0.20      # 20%
        }
        
        return total_amount, total_commission, commission_rate, breakdown
    
    elif "BELLA" in str(location).upper():
        # BELLA: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            "BELLA": commission
        }
        return total_amount, commission, commission_rate, breakdown
    
    elif "WS" in str(location).upper():
        # WS: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            "WS": commission
        }
        return total_amount, commission, commission_rate, breakdown
    
    else:
        # 其他代理: 30% 佣金
        commission_rate = 0.30
        commission = total_amount * commission_rate
        breakdown = {
            str(location): commission
        }
        return total_amount, commission, commission_rate, breakdown

@app.get("/")
async def root():
    return {
        "message": "Excel Commission Calculator API",
        "version": "1.0",
        "endpoints": {
            "/docs": "API文档和测试界面",
            "/upload-excel/": "预览数据和佣金计算",
            "/export-sorted/": "导出排序数据和佣金报表"
        },
        "deployed": "Vercel"
    }

@app.post("/upload-excel/")
async def upload_excel(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df, error = process_excel_data(contents)
        
        if error:
            return {"error": error}

        # 按上分地方分组统计
        location_stats = {}
        location_details = {}
        
        for index, row in df.iterrows():
            location = str(row.get("上分地方", "") or "").strip()
            if not location:
                location = "未知地点"
            
            # 统计每个地点的交易
            if location not in location_stats:
                location_stats[location] = 0
                location_details[location] = []
            
            location_stats[location] += 1
            
            # 记录详细信息
            transaction_info = {
                "代理序号": row.get("代理序号"),
                "ID": row.get("ID"),
                "金额": row.get("金额"),
                "BONUS": row.get("BONUS"),
                "行号": index + 1
            }
            location_details[location].append(transaction_info)

        # 计算每个地点的金额统计和佣金
        location_amounts = {}
        commission_calculations = {}
        
        for location, transactions in location_details.items():
            amounts = [t["金额"] for t in transactions if t["金额"] is not None]
            in_transactions = [t for t in transactions if "IN" in str(t["代理序号"]).upper()]
            out_transactions = [t for t in transactions if "OUT" in str(t["代理序号"]).upper()]
            
            if amounts:
                total_amount = sum(amounts)
                
                # 计算佣金
                total_amount, total_commission, commission_rate, commission_breakdown = calculate_commission(location, total_amount)
                
                location_amounts[location] = {
                    "总交易笔数": len(transactions),
                    "IN笔数": len(in_transactions),
                    "OUT笔数": len(out_transactions),
                    "总金额": total_amount,
                    "平均金额": total_amount / len(amounts) if amounts else 0,
                    "最大金额": max(amounts) if amounts else 0,
                    "最小金额": min(amounts) if amounts else 0
                }
                
                commission_calculations[location] = {
                    "佣金率": f"{commission_rate * 100}%",
                    "总佣金": round(total_commission, 2),
                    "佣金分配": {k: round(v, 2) for k, v in commission_breakdown.items()}
                }

        # 预览数据（排序后的）
        df_sorted = sort_by_agent_number(df)
        preview_data = []
        for i in range(min(10, len(df_sorted))):
            row_data = {col: df_sorted.iloc[i][col] for col in df_sorted.columns}
            preview_data.append(row_data)

        return {
            "success": True,
            "total_rows": len(df),
            "location_summary": {
                "by_location": location_stats,
                "amount_analysis": location_amounts,
                "commission_calculation": commission_calculations,
                "total_locations": len(location_stats)
            },
            "preview": preview_data,
            "export_ready": True,
            "sorting_applied": "按上分地方和代理序号排序"
        }

    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.post("/export-sorted/")
async def export_sorted(file: UploadFile = File(...)):
    """完全排序导出 - 先按上分地方，再按代理序号，包含佣金计算"""
    try:
        contents = await file.read()
        df, error = process_excel_data(contents)
        
        if error:
            return {"error": error}

        # 完全排序：先按上分地方，再按代理序号
        df_sorted = sort_by_agent_number(df)
        
        # 计算佣金
        commission_data = []
        total_commission_summary = {}
        
        locations = df_sorted['上分地方'].dropna().unique()
        for location in locations:
            if location:
                location_data = df_sorted[df_sorted['上分地方'] == location]
                total_amount = location_data['金额'].sum()
                
                # 计算佣金
                total_amount, total_commission, commission_rate, commission_breakdown = calculate_commission(location, total_amount)
                
                row_data = {
                    '上分地方': location,
                    '总金额': round(total_amount, 2),
                    '佣金率': f"{commission_rate * 100}%",
                    '总佣金': round(total_commission, 2),
                }
                # 添加佣金分配详情
                for agent, amount in commission_breakdown.items():
                    row_data[agent] = round(amount, 2)
                
                commission_data.append(row_data)
                
                # 汇总总佣金
                for agent, amount in commission_breakdown.items():
                    if agent not in total_commission_summary:
                        total_commission_summary[agent] = 0
                    total_commission_summary[agent] += amount
        
        # 导出
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet 1: 排序后的数据
            df_sorted.to_excel(writer, sheet_name='Sorted_Data', index=False)
            
            # Sheet 2: 地点统计
            location_summary = df_sorted.groupby('上分地方').agg({
                '代理序号': 'count',
                '金额': ['sum', 'mean', 'min', 'max']
            }).round(2)
            
            # 重命名列
            location_summary.columns = ['交易笔数', '总金额', '平均金额', '最小金额', '最大金额']
            location_summary.to_excel(writer, sheet_name='Location_Statistics')
            
            # Sheet 3: 佣金计算
            commission_df = pd.DataFrame(commission_data)
            commission_df.to_excel(writer, sheet_name='Commission_Calculation', index=False)
            
            # Sheet 4: 佣金汇总
            summary_data = [{'代理层级': agent, '总佣金': round(amount, 2)} for agent, amount in total_commission_summary.items()]
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Commission_Summary', index=False)
            
            # Sheet 5: 佣金规则说明
            rules_df = pd.DataFrame({
                '佣金分配规则': [
                    '肥子代理 638: 总金额 × 30%',
                    'BELLA: 总金额 × 30%', 
                    'WS: 总金额 × 30%',
                    'OC619系列分层分配 (总30%):',
                    '  - OC619-01-01: 总金额 × 5%',
                    '  - OC619: 总金额 × 5%',
                    '  - OC619-01: 总金额 × 20%',
                    '其他代理: 总金额 × 30%'
                ]
            })
            rules_df.to_excel(writer, sheet_name='Commission_Rules', index=False)

        output.seek(0)
        
        # 纯英文文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"commission_report_{timestamp}.xlsx"
        
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

# Vercel 部署必需
import os

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)