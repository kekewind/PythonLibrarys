import xlrd
import xlutils.copy


class Excelop:
    def __init__(self):
        self.old_path = input("输入数据Excel文件路径：")
        self.sheet_old = input("输入数据文件工作簿名称：")
        self.op_col_old = int(input("输入数据文件匹配列索引（第一列为零）："))
        self.op_col_cond = int(input("输入数据文件条件列索引（第一列为零）："))
        self.op_cond = input("输入数据文件条件列值：")
        self.cols = input("输入数据文件增加列起止索引（逗号分隔）").split(',')
        self.op_path = input("输入待匹配Excel文件路径：")
        self.sheet_op = input("输入匹配文件工作簿名称：")
        self.op_col_new = int(input("输入匹配文件匹配列索引（第一列为零）："))
        self.op_col_add = int(input("输入匹配文件新增列起始索引（第一列为零）："))
        self.new_file_path = input("输入生成文件位置：")

    def read_excel(self):
        excel_file = xlrd.open_workbook(self.old_path)
        sheet = excel_file.sheet_by_name(self.sheet_old)
        excel_w = xlrd.open_workbook(self.op_path)
        sheet_w = excel_w.sheet_by_name(self.sheet_op)
        wb = xlutils.copy.copy(excel_w)
        wbs = wb.get_sheet(0)
        list_bj = sheet_w.col_values(self.op_col_old, 0, sheet_w.nrows)
        list_val = []
        for ri in range(sheet.nrows):
            for li in list_bj:
                if str(sheet.cell_value(ri, self.op_col_new)) == li and str(
                        sheet.cell_value(ri, self.op_col_cond)) == self.op_cond:
                    list_val.append({
                        "id": li,
                        "val": sheet.row_values(ri, int(self.cols[0]), int(self.cols[1]))
                    })

        for each in list_val:
            for ri in range(sheet_w.nrows):
                if str(sheet_w.cell_value(ri, self.op_col_old)) == each["id"]:
                    start_index = self.op_col_add
                    for val in each["val"]:
                        wbs.write(ri, start_index, str(val))
                        start_index = start_index + 1
        if not str(self.new_file_path).endswith('\\'):
            self.new_file_path = self.new_file_path + '\\'
        wb.save(self.new_file_path + "New_File.xls")


a = Excelop()
a.read_excel()
