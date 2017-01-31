using System.Collections.Generic;
using System.Data.SqlClient;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class AbstractColumnSelection<T>
    {
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        // ReSharper disable InconsistentNaming
        protected IEnumerable<T> _list;
        protected string _tableName;
        protected string _schema;
        protected Dictionary<string, string> _customColumnMappings { get; }        
        protected HashSet<string> _columns;
        protected bool _disableAllIndexes;
        protected BulkCopySettings _bulkCopySettings;
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member   

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="bulkCopySettings"></param>
        protected AbstractColumnSelection(IEnumerable<T> list, string tableName, HashSet<string> columns, string schema, BulkCopySettings bulkCopySettings)
        {
            _disableAllIndexes = false;
            _customColumnMappings = new Dictionary<string, string>();
            _list = list;
            _tableName = tableName;
            _columns = columns;
            _schema = schema;
            _bulkCopySettings = bulkCopySettings;
        }

        /// <summary>
        /// A bulk insert will attempt to insert all records. If you have any unique constraints on columns, these must be respected. 
        /// Notes: (1) Only the columns configured (via AddColumn) will be evaluated. (3) Use AddAllColumns to add all columns in table. 
        /// </summary>
        /// <returns></returns>
        public BulkInsert<T> BulkInsert()
        {
            return new BulkInsert<T>(_list, _tableName, _schema, _columns, _customColumnMappings, _bulkCopySettings);
        }

        /// <summary>
        /// A bulk insert or update is also known as bulk upsert or merge. All matching rows from the source will be updated.
        /// Any unique rows not found in target but exist in source will be added. Notes: (1) BulkInsertOrUpdate requires at least 
        /// one MatchTargetOn property to be configured. (2) Only the columns configured (via AddColumn) 
        /// will be evaluated. (3) Use AddAllColumns to add all columns in table.
        /// </summary>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> BulkInsertOrUpdate()
        {
            return new BulkInsertOrUpdate<T>(_list, _tableName, _schema, _columns,
                _customColumnMappings, _bulkCopySettings);
        }

        /// <summary>
        /// A bulk update will attempt to update any matching records. Notes: (1) BulkUpdate requires at least one MatchTargetOn 
        /// property to be configured. (2) Only the columns configured (via AddColumn) will be evaluated. (3) Use AddAllColumns to add all columns in table.
        /// </summary>
        /// <returns></returns>
        public BulkUpdate<T> BulkUpdate()
        {
            return new BulkUpdate<T>(_list, _tableName, _schema, _columns, 
                _customColumnMappings, _bulkCopySettings);
        }

        /// <summary>
        /// A bulk delete will delete records when matched. Consider using a DTO with only the needed information (e.g. PK) Notes: 
        /// (1) BulkUpdate requires at least one MatchTargetOn property to be configured.
        /// </summary>
        /// <returns></returns>
        public BulkDelete<T> BulkDelete()
        {
            return new BulkDelete<T>(_list, _tableName, _schema, _columns,  
                _customColumnMappings, _bulkCopySettings);
        }
    }
}
