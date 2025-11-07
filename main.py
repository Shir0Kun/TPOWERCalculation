from fastapi import FastAPI, UploadFile, File, Response
import io
import pandas as pd
from openpyxl import load_workbook
from datetime import datetime
import re

app = FastAPI()

def process_excel_data(contents):
    """处理包含两个工作表的Excel数据"""
    excel_bytes = io.BytesIO(contents)
    wb = load_workbook(excel_bytes, data_only=True)
    
    # 读取第一个工作表 (NO, 用户名, 金额)
    sheet1 = wb.worksheets[0]
    max_row1 = sheet1.max_row
    max_col1 = sheet1.max_column
    
    sheet1_data = []
    for row_idx in range(1, max_row1 + 1):
        row_data = []
        for col_idx in range(1, max_col1 + 1):
            cell_value = sheet1.cell(row=row_idx, column=col_idx).value
            row_data.append(cell_value)
        
        if any(cell is not None and str(cell).strip() != "" for cell in row_data):
            sheet1_data.append(row_data)
    
    if not sheet1_data:
        return None, None, "第一个工作表为空"
    
    # 检测第一个工作表的标题行
    header_row_index1 = None
    for i, row in enumerate(sheet1_data):
        row_text = " ".join(str(cell) for cell in row if cell)
        if any(keyword in row_text for keyword in ["NO", "用户名", "金额", "ID", "用户"]):
            header_row_index1 = i
            break
    
    if header_row_index1 is None:
        header_row_index1 = 0
    
    # 创建第一个工作表的DataFrame
    headers1 = [str(c).strip() if c else f"Column_{i}" for i, c in enumerate(sheet1_data[header_row_index1])]
    data_rows1 = sheet1_data[header_row_index1 + 1:]
    
    df_sheet1 = pd.DataFrame(data_rows1, columns=headers1)
    df_sheet1 = df_sheet1.dropna(how="all")
    df_sheet1 = df_sheet1.reset_index(drop=True)
    df_sheet1 = df_sheet1.where(pd.notnull(df_sheet1), None)
    
    # 读取第二个工作表 (用户名, 层级)
    sheet2 = wb.worksheets[1]
    max_row2 = sheet2.max_row
    max_col2 = sheet2.max_column
    
    sheet2_data = []
    for row_idx in range(1, max_row2 + 1):
        row_data = []
        for col_idx in range(1, max_col2 + 1):
            cell_value = sheet2.cell(row=row_idx, column=col_idx).value
            row_data.append(cell_value)
        
        if any(cell is not None and str(cell).strip() != "" for cell in row_data):
            sheet2_data.append(row_data)
    
    if not sheet2_data:
        return df_sheet1, None, "第二个工作表为空"
    
    # 检测第二个工作表的标题行
    header_row_index2 = None
    for i, row in enumerate(sheet2_data):
        row_text = " ".join(str(cell) for cell in row if cell)
        if any(keyword in row_text for keyword in ["用户名", "层级", "级别", "等级"]):
            header_row_index2 = i
            break
    
    if header_row_index2 is None:
        header_row_index2 = 0
    
    # 创建第二个工作表的DataFrame
    headers2 = [str(c).strip() if c else f"Column_{i}" for i, c in enumerate(sheet2_data[header_row_index2])]
    data_rows2 = sheet2_data[header_row_index2 + 1:]
    
    df_sheet2 = pd.DataFrame(data_rows2, columns=headers2)
    df_sheet2 = df_sheet2.dropna(how="all")
    df_sheet2 = df_sheet2.reset_index(drop=True)
    df_sheet2 = df_sheet2.where(pd.notnull(df_sheet2), None)
    
    return df_sheet1, df_sheet2, None

def merge_user_data(df_sheet1, df_sheet2):
    """合并两个工作表的数据"""
    # 找到用户名列
    username_col1 = None
    username_col2 = None
    
    for col in df_sheet1.columns:
        if "用户" in str(col) or "name" in str(col).lower():
            username_col1 = col
            break
    
    for col in df_sheet2.columns:
        if "用户" in str(col) or "name" in str(col).lower():
            username_col2 = col
            break
    
    if username_col1 is None or username_col2 is None:
        return None, "找不到用户名列"
    
    # 合并数据
    merged_df = df_sheet1.merge(
        df_sheet2, 
        left_on=username_col1, 
        right_on=username_col2, 
        how='left',
        suffixes=('', '_层级表')
    )
    
    return merged_df, None

def sort_by_username_and_level(merged_df):
    """按用户名和层级排序"""
    # 找到用户名列和层级列
    username_col = None
    level_col = None
    
    for col in merged_df.columns:
        if "用户" in str(col) and "_层级表" not in str(col):
            username_col = col
        if "层级" in str(col):
            level_col = col
    
    if username_col is None or level_col is None:
        return merged_df
    
    # 定义层级排序顺序
    level_order = {
        "OC619": 1,
        "OC619-01": 2,
        "OC619-01-01": 3,
        "OC619-01-02": 3,
        "OC619-01-03": 3,
        "OC619-01-01-01": 3
    }
    
    # 添加排序辅助列
    merged_df_sorted = merged_df.copy()
    merged_df_sorted['层级排序'] = merged_df_sorted[level_col].map(level_order).fillna(99)
    
    # 先按用户名排序，再按层级排序
    merged_df_sorted = merged_df_sorted.sort_values(by=[username_col, '层级排序', level_col])
    merged_df_sorted = merged_df_sorted.drop('层级排序', axis=1)
    
    return merged_df_sorted.reset_index(drop=True)

def safe_convert_to_float(value):
    """安全转换为浮点数，保留负数"""
    if value is None:
        return 0.0
    try:
        # 如果是字符串，移除可能的空格和特殊字符
        if isinstance(value, str):
            value = value.strip().replace(',', '').replace(' ', '')
            # 检查是否是负数（包含负号）
            if value.startswith('-') and value[1:].replace('.', '').isdigit():
                return float(value)
            elif value.replace('.', '').isdigit():
                return float(value)
            else:
                return 0.0
        elif isinstance(value, (int, float)):
            return float(value)
        else:
            return 0.0
    except:
        return 0.0

def calculate_hierarchical_commission_correct(merged_df):
    """正确的层级佣金计算规则 - 基于总金额正负判断"""
    # 找到金额列和层级列
    amount_col = None
    level_col = None
    
    for col in merged_df.columns:
        if "金额" in str(col) and "数值" not in str(col):
            amount_col = col
        if "层级" in str(col):
            level_col = col
    
    if amount_col is None:
        return {"error": "找不到金额列"}
    if level_col is None:
        return {"error": "找不到层级列"}
    
    # 安全转换金额，保留负数
    merged_df['金额_数值'] = merged_df[amount_col].apply(safe_convert_to_float)
    
    # 按层级分组计算金额
    level_data = {}
    for level in merged_df[level_col].unique():
        if level is None:
            continue
        
        level_str = str(level).strip()
        level_df = merged_df[merged_df[level_col] == level]
        
        # 计算总金额（正数+负数）
        total_amount = level_df['金额_数值'].sum()
        # 只计算正数金额
        positive_amount = level_df[level_df['金额_数值'] > 0]['金额_数值'].sum()
        # 负数金额
        negative_amount = level_df[level_df['金额_数值'] < 0]['金额_数值'].sum()
        
        level_data[level_str] = {
            "total_amount": total_amount,
            "positive_amount": positive_amount,
            "negative_amount": negative_amount,
            "user_count": len(level_df),
            # 如果总金额 > 0，则有效金额为总金额，否则为0
            "effective_amount": total_amount if total_amount > 0 else 0
        }
    
    # 定义层级关系
    hierarchy_levels = {
        "OC619": {"rate": 0.05, "level": 1},
        "OC619-01": {"rate": 0.20, "level": 2},
        "OC619-01-01": {"rate": 0.05, "level": 3},
        "OC619-01-02": {"rate": 0.05, "level": 3},
        "OC619-01-03": {"rate": 0.05, "level": 3},
        "OC619-01-01-01": {"rate": 0.05, "level": 3}
    }
    
    # 计算各层级的计算基础（基于有效金额）
    def calculate_base_amount(level_name):
        """计算层级的计算基础（基于有效金额）"""
        if level_name not in hierarchy_levels:
            return 0
        
        level_info = hierarchy_levels[level_name]
        
        if level_name == "OC619":  # 第一层
            # 自己 + 第二层 + 第三层的有效金额
            base = level_data.get(level_name, {}).get("effective_amount", 0)
            
            # 加上第二层和第三层的有效金额
            for lv_name, lv_data in level_data.items():
                if lv_name in hierarchy_levels and lv_name != level_name:
                    base += lv_data["effective_amount"]
            
            return base
        
        elif level_name == "OC619-01":  # 第二层
            # 自己的有效金额 + 所有第三层的有效金额
            base = level_data.get(level_name, {}).get("effective_amount", 0)
            
            # 加上所有第三层的有效金额
            third_levels = ["OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]
            for third_level in third_levels:
                if third_level in level_data:
                    base += level_data[third_level]["effective_amount"]
            
            return base
        
        else:  # 第三层
            # 只计算自己的有效金额
            return level_data.get(level_name, {}).get("effective_amount", 0)
    
    # 计算佣金
    commission_results = {}
    
    for level_name, level_info in hierarchy_levels.items():
        if level_name not in level_data:
            # 如果该层级没有数据，但需要计算，使用0
            level_data[level_name] = {
                "total_amount": 0,
                "positive_amount": 0,
                "negative_amount": 0,
                "user_count": 0,
                "effective_amount": 0
            }
        
        base_amount = calculate_base_amount(level_name)
        commission = base_amount * level_info["rate"]
        
        # 获取计算说明
        if level_name == "OC619":
            # 计算所有下层的有效金额总和
            下层有效金额 = 0
            for lv_name, lv_data in level_data.items():
                if lv_name in hierarchy_levels and lv_name != level_name:
                    下层有效金额 += lv_data["effective_amount"]
            
            是否计算 = "计算" if level_data[level_name]["total_amount"] > 0 else "不计算(总金额为负)"
            calculation_note = f"OC619总金额({level_data[level_name]['total_amount']}) {是否计算} + 所有下层有效金额({下层有效金额}) × 5%"
        
        elif level_name == "OC619-01":
            # 计算所有第三层的有效金额总和
            third_levels_effective = 0
            third_levels = ["OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]
            for third_level in third_levels:
                if third_level in level_data:
                    third_levels_effective += level_data[third_level]["effective_amount"]
            
            是否计算 = "计算" if level_data[level_name]["total_amount"] > 0 else "不计算(总金额为负)"
            calculation_note = f"OC619-01总金额({level_data[level_name]['total_amount']}) {是否计算} + 所有第三层有效金额({third_levels_effective}) × 20%"
        
        else:
            是否计算 = "计算" if level_data[level_name]["total_amount"] > 0 else "不计算(总金额为负)"
            calculation_note = f"{level_name}总金额({level_data[level_name]['total_amount']}) {是否计算} × 5%"
        
        commission_results[level_name] = {
            "计算基础": base_amount,  # 修改为统一的键名
            "佣金率": level_info["rate"],
            "佣金": commission,
            "原始总金额": level_data[level_name]["total_amount"],
            "正数金额": level_data[level_name]["positive_amount"],
            "负数金额": level_data[level_name]["negative_amount"],
            "有效金额": level_data[level_name]["effective_amount"],
            "用户数量": level_data[level_name]["user_count"],
            "计算说明": calculation_note
        }
    
    return commission_results

@app.post("/export-sorted/")
async def export_sorted(file: UploadFile = File(...)):
    """处理双工作表Excel文件并计算层级佣金"""
    try:
        contents = await file.read()
        df_sheet1, df_sheet2, error = process_excel_data(contents)
        
        if error:
            return {"error": error}

        # 合并两个工作表的数据
        merged_df, merge_error = merge_user_data(df_sheet1, df_sheet2)
        if merge_error:
            return {"error": merge_error}

        # 按用户名和层级排序
        sorted_df = sort_by_username_and_level(merged_df)

        # 计算正确的层级佣金
        commission_results = calculate_hierarchical_commission_correct(sorted_df)
        
        if "error" in commission_results:
            return {"error": commission_results["error"]}

        # 准备导出数据
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Sheet 1: 按用户名和层级排序后的数据
            sorted_df.to_excel(writer, sheet_name='Sorted_User_Data', index=False)
            
            # Sheet 2: 层级佣金计算
            commission_data = []
            for level, result in commission_results.items():
                commission_data.append({
                    '层级': level,
                    '计算基础': result["计算基础"],
                    '佣金率': f"{result['佣金率']*100}%",
                    '佣金': result["佣金"],
                    '原始总金额': result["原始总金额"],
                    '正数金额': result["正数金额"],
                    '负数金额': result["负数金额"],
                    '有效金额': result["有效金额"],
                    '用户数量': result["用户数量"],
                    '计算说明': result["计算说明"]
                })
            
            commission_df = pd.DataFrame(commission_data)
            commission_df.to_excel(writer, sheet_name='Hierarchical_Commission', index=False)
            
            # Sheet 3: 佣金汇总 - 只显示各层级佣金，按层级顺序排列
            # 定义层级显示顺序
            level_order = ["OC619", "OC619-01", "OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]
            
            summary_data = []
            
            # 按层级顺序添加各层级佣金
            for level in level_order:
                if level in commission_results:
                    summary_data.append({
                        '项目': f'{level}佣金',
                        '金额': commission_results[level]["佣金"]
                    })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Commission_Summary', index=False)

        output.seek(0)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"commission_report_{timestamp}.xlsx"
        
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        return {"error": f"处理文件时出错: {str(e)}"}

@app.get("/")
def root():
    return {
        "message": "Excel Commission Calculation API - 修正版",
        "endpoint": {
            "/export-sorted/": "上传双工作表Excel文件进行层级佣金计算"
        },
        "features": {
            "sorting": "按用户名和层级双重排序",
            "commission_rules": {
                "第一层 OC619": "(OC619有效金额 + 所有下层有效金额) × 5%",
                "第二层 OC619-01": "(OC619-01有效金额 + 所有第三层有效金额) × 20%", 
                "第三层": "(各自有效金额) × 5%"
            },
            "note": "先计算每个层级的总金额(正数+负数)，只有总金额为正数时才参与佣金计算"
        }
    }

