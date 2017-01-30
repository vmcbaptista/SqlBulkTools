using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
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
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="bulkCopyDelegates"></param>
        public BulkAddColumn(IEnumerable<T> list, string tableName, HashSet<string> columns, string schema,
            int bulkCopyTimeout, bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions,
            IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates) :
            base(list, tableName, columns, schema, bulkCopyTimeout, bulkCopyEnableStreaming, bulkCopyNotifyAfter, bulkCopyBatchSize, sqlBulkCopyOptions, bulkCopyDelegates)
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

        /// <summary>
        /// Disables non-clustered index. You can select One to Many non-clustered indexes. This option should be considered on 
        /// a case-by-case basis. Understand the consequences before using this option.  
        /// </summary>
        /// <param name="indexName"></param>
        /// <returns></returns>
        public BulkAddColumn<T> AddTmpDisableNonClusteredIndex(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _disableIndexList.Add(indexName);

            return this;
        }

        /// <summary>
        /// Disables all Non-Clustered indexes on the table before the transaction and rebuilds after the 
        /// transaction. This option should be considered on a case-by-case basis. Understand the 
        /// consequences before using this option.  
        /// </summary>
        /// <returns></returns>
        public BulkAddColumn<T> TmpDisableAllNonClusteredIndexes()
        {
            _disableAllIndexes = true;
            return this;
        }
    }
}
