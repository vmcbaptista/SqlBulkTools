using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools.BulkCopy
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkTable<T>
    {
        private readonly IEnumerable<T> _list;
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private BulkCopySettings _bulkCopySettings;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        public BulkTable(IEnumerable<T> list, string tableName)
        {
            _list = list;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _tableName = tableName;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _bulkCopySettings = new BulkCopySettings();
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public BulkAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new BulkAddColumn<T>(_list, _tableName, Columns, _schema, _bulkCopySettings);
        }

        /// <summary>
        /// Adds all properties in model that are either value, string, char[] or byte[] type. 
        /// </summary>
        /// <returns></returns>
        public BulkAddColumnList<T> AddAllColumns()
        {
            Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(typeof(T));
            return new BulkAddColumnList<T>(_list, _tableName, Columns, _schema, _bulkCopySettings);
        }

        /// <summary>
        /// Explicitly set a schema. If a schema is not added, the system default schema name 'dbo' will used.
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public BulkTable<T> WithSchema(string schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="settings"></param>
        /// <returns></returns>
        public BulkTable<T> WithBulkCopySettings(BulkCopySettings settings)
        {
            _bulkCopySettings = settings;
            return this;
        }

    }
}