using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;
using SqlBulkTools.Enumeration;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteQueryCondition<T>
    {
        private readonly string _tableName;
        private readonly string _schema;
        private readonly List<PredicateCondition> _whereConditions;
        private readonly List<SqlParameter> _parameters;
        private int _conditionSortOrder;
        private readonly Dictionary<string, string> _collationColumnDic;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="sqlTimeout"></param>
        public DeleteQueryCondition(string tableName, string schema, int sqlTimeout)
        {
            _tableName = tableName;
            _schema = schema;
            _whereConditions = new List<PredicateCondition>();
            _parameters = new List<SqlParameter>();
            _collationColumnDic = new Dictionary<string, string>();
            _conditionSortOrder = 1;
        }

        /// <summary>
        /// Specify a condition.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public DeleteQueryReady<T> Where(Expression<Func<T, bool>> expression)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _parameters, 
                _conditionSortOrder, Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            return new DeleteQueryReady<T>(_tableName, _schema, _conditionSortOrder, 
                _whereConditions, _parameters, _collationColumnDic);
        }

        /// <summary>
        /// Specify a condition.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="collation">Only explicitly set the collation if there is a collation conflict.</param>
        /// <returns></returns>
        /// <exception cref="SqlBulkToolsException"></exception>
        public DeleteQueryReady<T> Where(Expression<Func<T, bool>> expression, string collation)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _parameters,
                _conditionSortOrder, Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            string leftName = BulkOperationsHelper.GetExpressionLeftName(expression, PredicateType.Or, "Collation");
            _collationColumnDic.Add(leftName, collation);

            return new DeleteQueryReady<T>(_tableName, _schema, _conditionSortOrder,
                _whereConditions, _parameters, _collationColumnDic);
        }

        /// <summary>
        /// Please understand the consequences before using this method. This will delete all records in the table. 
        /// </summary>
        /// <returns></returns>
        public DeleteAllRecordsQueryReady<T> AllRecords()
        {
            return new DeleteAllRecordsQueryReady<T>(_tableName, _schema);
        }

    }
}
