using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkAddColumn<T> : AbstractColumnSelection<T>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="bulkCopySettings"></param>
        public BulkAddColumn(IEnumerable<T> list, string tableName, HashSet<string> columns, string schema, BulkCopySettings bulkCopySettings) :
            base(list, tableName, columns, schema, bulkCopySettings)
        {

        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _columns.Add(propertyName);
            return this;
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <param name="destination">The actual name of column as represented in SQL table. By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this overload to set up a custom mapping. </param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName, string destination)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _columns.Add(propertyName);
            return this;
        }
    }
}
