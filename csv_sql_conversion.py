import sqlite3
import pandas as pd

class my_conversion():


    def sql_to_csv(self, conn, table_name):
        
        c = conn.cursor()
        table = c.execute("SELECT * FROM {}".format(table_name))
        with open("list_fault_lines.csv", "w", encoding='utf-8') as csv_file:
            for row in table:
                for i in row:
                    if isinstance(i, str) is False:     # all data needs to be str
                        csv_file.write(str(i))
                    else:
                        csv_file.write(i)                    
                    if i is row[-1]:
                        continue
                    csv_file.write(",")
                csv_file.write("\n")
    
        conn.commit()        


    def csv_to_sql(self, csv_file, table_name):
        
        db_name = csv_file.replace(".csv", ".db")

        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        dataset = pd.read_csv(csv_file)

        column_names = [i for i in dataset.columns]
        columns_stripped = []
        for i in column_names:
            if " " in i:
                columns_stripped.append(i[:i.index(" ")])       # Taking only first word as column name
            else:
                columns_stripped.append(i)
        types = {str: "text", int: "integer", float: "real", None: "null"}

        first_row = [i for i in dataset.iloc[0]]    # sample row to determine type of data
        headings = []
        for i in range(len(columns_stripped)):
            if type(first_row[i])in types:
                headings.append(columns_stripped[i] + " " + types[type(first_row[i])])
            elif type(first_row[i].item())in types:
                headings.append(columns_stripped[i] + " " + types[type(first_row[i].item())])
            else: 
                headings.append(columns_stripped[i] + " " + "blob")
  
        table_headings = ", ".join(headings)
        c.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, table_headings))
        values = [tuple(row) for row in dataset.values]
        question_marks = "?," * len(columns_stripped)
        question_marks = question_marks[:-1]
        c.executemany("INSERT INTO {} VALUES ({})".format(table_name, question_marks), values)
        
        conn.commit()
        conn.close()
        
 
output = my_conversion()
conn = sqlite3.connect("all_fault_line.db")
output.sql_to_csv(conn, "fault_lines")
output.csv_to_sql("list_volcano.csv", "volcanos")