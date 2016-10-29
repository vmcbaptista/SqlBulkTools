using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// Configurable options for table. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SimpleTable<T>
    {
        private readonly T _singleEntity;
        private HashSet<string> Columns { get; set; }
        private string _schema;
        private readonly string _tableName;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private int _sqlTimeout;
        private List<SqlParameter> _sqlParams;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="ext"></param>
        /// <param name="transactionCount"></param>
        public SimpleTable(T singleEntity, string tableName, List<SqlParameter> sqlParams)
        {
            _singleEntity = singleEntity;
            _sqlTimeout = 600;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _tableName = tableName;
            _schema = Constants.DefaultSchemaName;
            Columns = new HashSet<string>();
            CustomColumnMappings = new Dictionary<string, string>();
            _sqlParams = sqlParams;
        }

        ///// <summary>
        ///// Add each column that you want to include in the query.
        ///// </summary>
        ///// <param name="columnName">Column name as represented in database</param>
        ///// <returns></returns>
        //public InsertQueryAddColumn<T> AddColumn(Expression<Func<T, object>> columnName)
        //{
        //    var propertyName = BulkOperationsHelper.GetPropertyName(columnName);
        //    Columns.Add(propertyName);
        //    return new InsertQueryAddColumn<T>(_singleEntity, _tableName, Columns, _schema,
        //        _sqlTimeout, _sqlParams);
        //}

        ///// <summary>
        ///// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        ///// procedure for best performance. 
        ///// </summary>
        ///// <param name="columnName">Column name as represented in database</param>
        ///// <returns></returns>
        //public InsertQueryAddColumnList<T> AddAllColumns()
        //{
        //    Columns = BulkOperationsHelper.GetAllValueTypeAndStringColumns(typeof(T));

        //    return new InsertQueryAddColumnList<T>(_singleEntity, _tableName, Columns, _schema,
        //        _sqlTimeout, _sqlParams);
        //}

        ///// <summary>
        ///// Explicitley set a schema if your table may have a naming conflict within your database. 
        ///// If a schema is not added, the system default schema name 'dbo' will used.. 
        ///// </summary>
        ///// <param name="schema"></param>
        ///// <returns></returns>
        //public InsertQueryTable<T> WithSchema(string schema)
        //{
        //    _schema = schema;
        //    return this;
        //}

        ///// <summary>
        ///// Default is 600 seconds. See docs for more info. 
        ///// </summary>
        ///// <param name="seconds"></param>
        ///// <returns></returns>
        //public InsertQueryTable<T> WithSqlCommandTimeout(int seconds)
        //{
        //    _sqlTimeout = seconds;
        //    return this;
        //}
    }
}
