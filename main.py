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
    # 标准化列名
    sheet1_columns = {col: col for col in df_sheet1.columns}
    sheet2_columns = {col: col for col in df_sheet2.columns}
    
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

def calculate_hierarchical_commission_new(merged_df):
    """新的层级佣金计算规则 - 只计算正数金额"""
    # 确保金额列是数值类型
    amount_col = None
    for col in merged_df.columns:
        if "金额" in str(col):
            amount_col = col
            break
    
    if amount_col is None:
        return {"error": "找不到金额列"}
    
    merged_df[amount_col] = pd.to_numeric(merged_df[amount_col], errors='coerce')
    merged_df = merged_df.dropna(subset=[amount_col])
    
    # 找到层级列
    level_col = None
    for col in merged_df.columns:
        if "层级" in str(col):
            level_col = col
            break
    
    if level_col is None:
        return {"error": "找不到层级列"}
    
    # 按层级分组计算金额（只计算正数金额）
    level_data = {}
    for level in merged_df[level_col].unique():
        if level is None:
            continue
        
        level_df = merged_df[merged_df[level_col] == level]
        # 只计算正数金额
        positive_amount = level_df[level_df[amount_col] > 0][amount_col].sum()
        total_amount = level_df[amount_col].sum()
        
        level_data[str(level)] = {
            "total_amount": total_amount,
            "positive_amount": positive_amount,
            "user_count": len(level_df)
        }
    
    # 定义层级关系
    hierarchy_structure = {
        "OC619": {"rate": 0.05, "children": ["OC619-01", "OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]},
        "OC619-01": {"rate": 0.20, "children": ["OC619-01-01", "OC619-01-02", "OC619-01-03", "OC619-01-01-01"]},
        "OC619-01-01": {"rate": 0.05, "children": []},
        "OC619-01-02": {"rate": 0.05, "children": []},
        "OC619-01-03": {"rate": 0.05, "children": []},
        "OC619-01-01-01": {"rate": 0.05, "children": []}
    }
    
    def get_positive_base_amount(level_name):
        """获取层级的正数计算基础"""
        if level_name not in level_data:
            return 0
        
        # 只计算正数金额
        base_amount = level_data[level_name]["positive_amount"]
        
        # 加上所有子层级的正数金额
        if level_name in hierarchy_structure:
            for child in hierarchy_structure[level_name]["children"]:
                base_amount += get_positive_base_amount(child)
        
        return base_amount
    
    # 计算各层级的佣金
    commission_results = {}
    
    for level_name, level_info in hierarchy_structure.items():
        if level_name not in level_data:
            continue
            
        if level_name == "OC619":  # 第一层
            base_amount = get_positive_base_amount(level_name)
            commission = base_amount * level_info["rate"]
            
        elif level_name == "OC619-01":  # 第二层
            # 自己 + 第三层的正数金额
            own_positive = level_data[level_name]["positive_amount"]
            third_level_positive = 0
            for child in level_info["children"]:
                if child in level_data:
                    third_level_positive += level_data[child]["positive_amount"]
            
            base_amount = own_positive + third_level_positive
            commission = base_amount * level_info["rate"]
            
        else:  # 第三层
            # 只计算自己的正数金额
            base_amount = level_data[level_name]["positive_amount"]
            commission = base_amount * level_info["rate"]
        
        commission_results[level_name] = {
            "计算基础(正数金额)": base_amount,
            "佣金率": level_info["rate"],
            "佣金": commission,
            "原始总金额": level_data[level_name]["total_amount"],
            "正数金额": level_data[level_name]["positive_amount"],
            "用户数量": level_data[level_name]["user_count"],
            "计算说明": get_calculation_description(level_name, level_info, level_data)
        }
    
    return commission_results

def get_calculation_description(level_name, level_info, level_data):
    """获取计算说明"""
    if level_name == "OC619":
        children_positive = sum(level_data[child]["positive_amount"] for child in level_info["children"] if child in level_data)
        return f"OC619正数金额({level_data[level_name]['positive_amount']}) + 下层正数金额({children_positive}) × 5%"
    
    elif level_name == "OC619-01":
        children_positive = sum(level_data[child]["positive_amount"] for child in level_info["children"] if child in level_data)
        return f"OC619-01正数金额({level_data[level_name]['positive_amount']}) + 第三层正数金额({children_positive}) × 20%"
    
    else:
        return f"{level_name}正数金额({level_data[level_name]['positive_amount']}) × 5%"

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

        # 计算新的层级佣金
        commission_results = calculate_hierarchical_commission_new(sorted_df)
        
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
                    '计算基础(正数金额)': result["计算基础(正数金额)"],
                    '佣金率': f"{result['佣金率']*100}%",
                    '佣金': result["佣金"],
                    '原始总金额': result["原始总金额"],
                    '正数金额': result["正数金额"],
                    '用户数量': result["用户数量"],
                    '计算说明': result["计算说明"]
                })
            
            commission_df = pd.DataFrame(commission_data)
            commission_df.to_excel(writer, sheet_name='Hierarchical_Commission', index=False)
            
            # Sheet 3: 佣金汇总
            total_commission = sum(result["佣金"] for result in commission_results.values())
            summary_data = [{
                '总佣金': total_commission,
                '计算层级数量': len(commission_results)
            }]
            # 添加各层级佣金明细
            for level, result in commission_results.items():
                summary_data[0][f'{level}佣金'] = result["佣金"]
            
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
        "message": "Excel Commission Calculation API - 双工作表版本",
        "endpoint": {
            "/export-sorted/": "上传双工作表Excel文件进行层级佣金计算"
        },
        "features": {
            "input_sheets": "支持两个工作表：用户金额数据和层级数据",
            "sorting": "按用户名和层级双重排序",
            "commission": "新的层级佣金计算规则（只计算正数金额）",
            "calculation_rules": {
                "第一层 OC619": "(自己正数金额 + 第二层正数金额 + 第三层正数金额) × 5%",
                "第二层 OC619-01": "(自己正数金额 + 第三层正数金额) × 20%", 
                "第三层": "自己正数金额 × 5%"
            }
        }
    }
