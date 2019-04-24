using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MSDbMigration.DbMigration
{
    public class DataExtractorByConfig
    {
        string _connectionString;

        public DataExtractorByConfig(string connectionString)
        {
            _connectionString = connectionString;
        }

        public void CreateInsertScripts(string connectionString, Database db, List<DTOs.TableMapping> tableMappings, string destDirPath)
        {
            Stopwatch timer = Stopwatch.StartNew();
            foreach (DTOs.TableMapping tableMapping in tableMappings)
            {
                var smoTable = db.Tables[tableMapping.TableFrom];
                Console.Write("Processing table {0} to {1}", tableMapping.TableFrom, tableMapping.TableTo);

                ProcessTable(smoTable, tableMapping, destDirPath);
                Console.Write("done in {0} ms", timer.ElapsedMilliseconds);
                timer.Restart();
                Console.Write(Environment.NewLine);
            }
        }

        #region private methods
        static string SqlReadyValue(string sqlBuiltString, Column column, int columnIndex, SqlDataReader reader)
        {
            if (columnIndex > 0)
                sqlBuiltString += ", ";

            if (reader[columnIndex] == null || String.IsNullOrEmpty(reader[columnIndex].ToString()))
                return sqlBuiltString += "NULL";


            switch (column.DataType.SqlDataType)
            {
                case SqlDataType.Int:
                    sqlBuiltString += reader.GetInt32(columnIndex);
                    break;
                case SqlDataType.SmallInt:
                    sqlBuiltString += reader.GetInt16(columnIndex);
                    break;
                case SqlDataType.BigInt:
                    sqlBuiltString += reader.GetInt64(columnIndex);
                    break;
                case SqlDataType.Float:
                    try
                    {
                        float floatNumber = reader.GetFloat(columnIndex);
                        sqlBuiltString += floatNumber.ToString(CultureInfo.InvariantCulture);
                    }
                    catch
                    {
                        double doubleNumber = reader.GetDouble(columnIndex);
                        sqlBuiltString += doubleNumber.ToString(CultureInfo.InvariantCulture);

                    }
                    break;

                case SqlDataType.Decimal:
                    decimal decimalNumber = reader.GetDecimal(columnIndex);
                    sqlBuiltString += decimalNumber.ToString(CultureInfo.InvariantCulture);
                    break;

                case SqlDataType.DateTime2:
                case SqlDataType.DateTime:
                    var date = reader.GetDateTime(columnIndex);
                    //sqlBuiltString += String.Format("TO_DATE('{0}', 'yyyy-mm-dd hh24:mi:ss')", date.ToString("yyyy-MM-dd HH:mm:ss"));
                    sqlBuiltString += "'" + date.ToString("yyyy-MM-dd HH:mm:ss:ms") + "'";
                    break;

                case SqlDataType.Bit:
                    var bitData = reader.GetBoolean(columnIndex);
                    sqlBuiltString += bitData == true ? "1" : "0";
                    break;

                case SqlDataType.NVarChar:
                case SqlDataType.NVarCharMax:
                case SqlDataType.VarChar:
                    sqlBuiltString += "'" + reader.GetString(columnIndex) + "'";
                    break;

                case SqlDataType.UniqueIdentifier:
                    sqlBuiltString += "'" + reader.GetGuid(columnIndex).ToString() + "'";
                    break;

                case SqlDataType.VarBinary:
                case SqlDataType.VarBinaryMax:
                    sqlBuiltString += "NULL";
                    break;
            }

            return sqlBuiltString;
        }

        void ProcesDbRecord(Table smoTable, DTOs.TableMapping tableMapping, SqlDataReader reader, StreamWriter sw)
        {
            var dicColumnNames = new Dictionary<string, string>();
            var columnValues = String.Empty;
            var columnNames = String.Empty;

            for (var i = 0; i < smoTable.Columns.Count; i++)
            {
                var column = tableMapping.Columns.Where(c => c.ColumnFrom == smoTable.Columns[i].Name).FirstOrDefault();
                if (column != null)
                {
                    var fromName = column.ColumnFrom;
                    var toName = column.ColumnTo;

                    if (!dicColumnNames.ContainsKey(fromName))
                    {
                        dicColumnNames.Add(fromName, toName);

                        columnNames += String.Format(" [{0}],", toName);
                        columnValues = SqlReadyValue(columnValues, smoTable.Columns[i], i, reader);
                    }
                }
            }

            columnValues = columnValues.Trim(',');
            columnNames = columnNames.Trim(',');

            sw.WriteLine("INSERT INTO {0} ({1}) VALUES ({2});", tableMapping.TableTo, columnNames, columnValues);

        }

        void ExtractDataForTable(Table smoTable, DTOs.TableMapping tableMapping, StreamWriter sw)
        {
            using (var conn = new SqlConnection(_connectionString))
            {
                conn.Open();
                var command = new SqlCommand(String.Format("SELECT * FROM {0}", tableMapping.TableFrom), conn);
                SqlDataReader reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        ProcesDbRecord(smoTable, tableMapping, reader, sw);
                    }
                }
            }
        }

        void ProcessTable(Table smoTable, DTOs.TableMapping tableMapping, string destDirPath)
        {
            if (!Directory.Exists(destDirPath))
                Directory.CreateDirectory(destDirPath);

            var filePath = Path.Combine(destDirPath, tableMapping.TableTo + ".sql");
            if (File.Exists(filePath))
                File.Delete(filePath);

            using (StreamWriter sw = File.CreateText(filePath))
            {
                sw.WriteLine($"SET IDENTITY_INSERT [{tableMapping.TableTo}] ON");
                sw.WriteLine("GO");
                ExtractDataForTable(smoTable, tableMapping, sw);
                sw.WriteLine($"SET IDENTITY_INSERT [{tableMapping.TableTo}] OFF");
                sw.WriteLine("GO");
            }
        }
        #endregion
    }
}
