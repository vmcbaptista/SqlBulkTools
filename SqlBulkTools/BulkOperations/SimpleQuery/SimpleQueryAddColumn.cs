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
    public class SimpleQueryAddColumn<T>
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; }
        private readonly HashSet<string> _columns;
        private readonly string _schema;
        private readonly int _sqlTimeout;
        private readonly List<SqlParameter> _sqlParams;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="sqlParams"></param>
        public SimpleQueryAddColumn(T singleEntity, string tableName, HashSet<string> columns, string schema,
            int sqlTimeout, List<SqlParameter> sqlParams)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _columns = columns;
            _schema = schema;
            _sqlTimeout = sqlTimeout;
            CustomColumnMappings = new Dictionary<string, string>();
            _sqlParams = sqlParams;
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public SimpleQueryAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
            _columns.Add(propertyName);
            return this;
        }

        /// <summary>
        /// Inserts a single entity. This method uses a parameterized query. 
        /// </summary>
        /// <returns></returns>
        public SimpleInsertQueryReady<T> Insert()
        {
            return new SimpleInsertQueryReady<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings,
                _sqlTimeout, _sqlParams);
        }

        /// <summary>
        /// Attempts to update the target entity (using the mandatory MatchTargetOn property). 
        /// If the target entity doesn't exist, insert a new record. This method uses a parameterized query.
        /// </summary>
        /// <returns></returns>
        public SimpleUpsertQueryReady<T> Upsert()
        {
            return new SimpleUpsertQueryReady<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings,
                _sqlTimeout, _sqlParams);
        }



        /// <summary>
        /// All rows matching the condition(s) selected will be updated. If you need to update a collection of objects that can't be
        /// matched by a generic condition, use the BulkUpdate method instead. This method uses a parameterized query. 
        /// </summary>
        /// <returns></returns>
        public SimpleUpdateQueryCondition<T> Update()
        {
            return new SimpleUpdateQueryCondition<T>(_singleEntity, _tableName, _schema, _columns, CustomColumnMappings,
                _sqlTimeout, _sqlParams);
        }

        /// <summary>
        /// By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this method to set up a custom mapping.  
        /// </summary>
        /// <param name="source">
        /// The object member that has a different name in SQL table. 
        /// </param>
        /// <param name="destination">
        /// The actual name of column as represented in SQL table. 
        /// </param>
        /// <returns></returns>
        public SimpleQueryAddColumn<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(source);
            CustomColumnMappings.Add(propertyName, destination);
            return this;
        }
    }
}
