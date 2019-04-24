using Microsoft.SqlServer.Management.Smo;
using MSDbMigration.DbMigration;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MSDbMigration
{
    class Program
    {
        private static string _conn = ConfigurationManager.ConnectionStrings["dbConnStr"].ConnectionString;

        static void Main(string[] args)
        {
            ExtractConfigData();
        }

        static void ExtractConfigData()
        {
            var destDirPath = AppDomain.CurrentDomain.BaseDirectory + "DB";
            var configPath = AppDomain.CurrentDomain.BaseDirectory + "TableMappingConfigs";
            var configFiles = Directory.GetFiles(configPath, "t_*");
            var tableMappings = new List<DTOs.TableMapping>();

            foreach(var configFile in configFiles)
            {
                var tableMapping = Utils.ReadJsonFile<DTOs.TableMapping>(configFile);
                tableMappings.Add(tableMapping);
            }

            var sourceDatabase = GetSmoDatabase();
            var dataExtractorByConfig = new DataExtractorByConfig(_conn);

            dataExtractorByConfig.CreateInsertScripts(_conn, sourceDatabase, tableMappings, destDirPath);
        }

        static Database GetSmoDatabase()
        {
            var builder = new SqlConnectionStringBuilder(_conn);

            Server smoServer = new Server(builder.DataSource);
            smoServer.ConnectionContext.LoginSecure = false;
            smoServer.ConnectionContext.Login = builder.UserID;
            smoServer.ConnectionContext.Password = builder.Password;
            //smoServer.ConnectionContext.Connect();

            Database sourceDatabase = null;

            List<Table> tables = new List<Table>();
            foreach (Database myDatabase in smoServer.Databases)
            {
                if (myDatabase.Name != builder.InitialCatalog)
                    continue;
                sourceDatabase = myDatabase;
            }
            if (sourceDatabase == null)
            {
                Console.WriteLine(String.Format("Database {0} not found on server:{1}", builder.InitialCatalog, builder.DataSource));
                return null;
            }

            return sourceDatabase;
        }
    }
}
