using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using SqlBulkTools.BulkCopy;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkForCollection<T>
    {
        private readonly IEnumerable<T> _list;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public BulkForCollection(IEnumerable<T> list)
        {
            _list = list;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public BulkTable<T> WithTable(string tableName)
        {
            StringBuilder sb = new StringBuilder(tableName.Trim());

            if (sb.ToString().ToCharArray().Count(x => x == '.') > 1)
            {
                throw new SqlBulkToolsException("Table name can't contain more than one period '.' character.");
            }

            sb = sb.Replace("[", string.Empty);
            sb = sb.Replace("]", string.Empty);

            var schemaMatch = Regex.Match(sb.ToString(), @"(?<=\.).*");
           
            // Check if schema is included in table name.
            string schema = schemaMatch.Success ? schemaMatch.Value : Constants.DefaultSchemaName;

            var tableMatch = Regex.Match(sb.ToString(), @"^([^.]*)");
            tableName = tableMatch.Success ? tableMatch.Value : sb.ToString();

            return new BulkTable<T>(_list, tableName, schema);
        }
    }
}
