using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MSDbMigration.DTOs
{
    public class TableMapping
    {
        public string TableFrom { get; set; }
        public string TableTo { get; set; }
        //public int MaxRowCount { get; set; }
        public List<TableColumnMapping> Columns { get; set; }
    }
}
