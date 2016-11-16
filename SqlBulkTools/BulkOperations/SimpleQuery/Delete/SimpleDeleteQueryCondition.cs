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
    public class SimpleDeleteQueryCondition<T>
    {
        private readonly string _tableName;
        private readonly string _schema;
        private readonly int _sqlTimeout;
        private readonly List<PredicateCondition> _whereConditions;
        private readonly List<SqlParameter> _parameters;
        private int _conditionSortOrder;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="sqlTimeout"></param>
        public SimpleDeleteQueryCondition(string tableName, string schema, int sqlTimeout)
        {
            _tableName = tableName;
            _schema = schema;
            _sqlTimeout = sqlTimeout;
            _whereConditions = new List<PredicateCondition>();
            _parameters = new List<SqlParameter>();
            _conditionSortOrder = 1;
        }

        /// <summary>
        /// Specify a condition.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public SimpleDeleteQueryReady<T> Where(Expression<Func<T, bool>> expression)
        {
            // _whereConditions list will only ever contain one element.
            BulkOperationsHelper.AddPredicate(expression, PredicateType.Where, _whereConditions, _parameters, 
                _conditionSortOrder, Constants.UniqueParamIdentifier);

            _conditionSortOrder++;

            return new SimpleDeleteQueryReady<T>(_tableName, _schema, _sqlTimeout, _conditionSortOrder, 
                _whereConditions, _parameters);
        }

    }
}
