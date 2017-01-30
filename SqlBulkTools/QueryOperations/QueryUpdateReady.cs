using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using SqlBulkTools.Enumeration;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class QueryUpdateReady<T> : ITransaction
    {
        private readonly T _singleEntity;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly List<PredicateCondition> _whereConditions;
        private readonly List<PredicateCondition> _andConditions;
        private readonly List<PredicateCondition> _orConditions;
        private readonly List<SqlParameter> _sqlParams;
        private int _conditionSortOrder;
        private string _identityColumn;
        private readonly Dictionary<string, string> _collationColumnDic; 

        /// <summary>
        /// 
        /// </summary>
        /// <param name="singleEntity"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="conditionSortOrder"></param>
        /// <param name="whereConditions"></param>
        /// <param name="sqlParams"></param>
        public QueryUpdateReady(T singleEntity, string tableName, string schema, HashSet<string> columns, Dictionary<string, string> customColumnMappings,
            int conditionSortOrder, List<PredicateCondition> whereConditions, List<SqlParameter> sqlParams)
        {
            _singleEntity = singleEntity;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _customColumnMappings = customColumnMappings;
            _conditionSortOrder = conditionSortOrder;
            _whereConditions = whereConditions;
            _andConditions = new List<PredicateCondition>();
            _orConditions = new List<PredicateCondition>();
            _sqlParams = sqlParams;
            _identityColumn = null;
            _collationColumnDic = new Dictionary<string, string>();
        }

        /// <summary>
        /// Sets the identity column for the table. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public QueryUpdateReady<T> SetIdentityColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("SetIdentityColumn column name can't be null");

            if (_identityColumn == null)
                _identityColumn = propertyName;

            else
            {
                throw new SqlBulkToolsException("Can't have more than one identity column");
            }

            return this;
        }


        /// <summary>
        /// Specify an additional condition to match on.
        /// </summary>
        /// <param name="expression">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public QueryUpdateReady<T> And(Expression<Func<T, bool>> expression)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.And, _andConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;
            return this;
        }

        /// <summary>
        /// Specify an additional condition to match on.
        /// </summary>
        /// <param name="expression">Only explicitly set the collation if there is a collation conflict.</param>
        /// <param name="collation"></param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException">Only explicitly set the collation if there is a collation conflict.</exception>
        public QueryUpdateReady<T> And(Expression<Func<T, bool>> expression, string collation)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.And, _andConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            string leftName = BulkOperationsHelper.GetExpressionLeftName(expression, PredicateType.And, "Collation");
            _collationColumnDic.Add(leftName, collation);

            return this;
        }

        /// <summary>
        /// Specify an additional condition to match on.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public QueryUpdateReady<T> Or(Expression<Func<T, bool>> expression, string collation)
        {
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Or, _orConditions, _sqlParams, _conditionSortOrder, appendParam: Constants.UniqueParamIdentifier);
            _conditionSortOrder++;

            string leftName = BulkOperationsHelper.GetExpressionLeftName(expression, PredicateType.Or, "Collation");
            _collationColumnDic.Add(leftName, collation);

            return this;
        }

        /// <summary>
        /// Set the collation explicitly for join conditions. Can be recalled multiple times for more than one column. 
        /// Note that this should only be used if there is a collation conflict and you can't resolve it by other means. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="collation"></param>
        /// <returns></returns>
        public QueryUpdateReady<T> SetCollationOnColumn(Expression<Func<T, object>> columnName, string collation)
        {
            var propertyName = BulkOperationsHelper.GetPropertyName(columnName);

            if (propertyName == null)
                throw new SqlBulkToolsException("SetCollationOnColumn column name can't be null");

            _collationColumnDic.Add(propertyName, collation);

            return this;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for the operation to be 
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public int Commit(SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (connection.State == ConnectionState.Closed)
                connection.Open();


            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;

            string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                _tableName);


            BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, customColumns: _customColumnMappings);          
            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

            string comm = $"UPDATE {fullQualifiedTableName} " +
            $"{BulkOperationsHelper.BuildUpdateSet(_columns, null, _identityColumn)}" +
            $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery, _collationColumnDic)}";

            command.CommandText = comm;

            if (_sqlParams.Count > 0)
            {
                command.Parameters.AddRange(_sqlParams.ToArray());
            }

            affectedRows = command.ExecuteNonQuery();

            return affectedRows;
        }

        /// <summary>
        /// Commits a transaction to database asynchronously. A valid setup must exist for the operation to be 
        /// successful.
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public async Task<int> CommitAsync(SqlConnection connection)
        {
            int affectedRows = 0;
            if (_singleEntity == null)
            {
                return affectedRows;
            }

            if (connection.State == ConnectionState.Closed)
                await connection.OpenAsync();


            SqlCommand command = connection.CreateCommand();
            command.Connection = connection;

            string fullQualifiedTableName = BulkOperationsHelper.GetFullQualifyingTableName(connection.Database, _schema,
                _tableName);


            BulkOperationsHelper.AddSqlParamsForQuery(_sqlParams, _columns, _singleEntity, customColumns: _customColumnMappings);
            var concatenatedQuery = _whereConditions.Concat(_andConditions).Concat(_orConditions).OrderBy(x => x.SortOrder);
            BulkOperationsHelper.DoColumnMappings(_customColumnMappings, _columns);

            string comm = $"UPDATE {fullQualifiedTableName} " +
            $"{BulkOperationsHelper.BuildUpdateSet(_columns, null, _identityColumn)}" +
            $"{BulkOperationsHelper.BuildPredicateQuery(concatenatedQuery, _collationColumnDic)}";

            command.CommandText = comm;

            if (_sqlParams.Count > 0)
            {
                command.Parameters.AddRange(_sqlParams.ToArray());
            }

            affectedRows = await command.ExecuteNonQueryAsync();

            return affectedRows;
        }
    }
}
