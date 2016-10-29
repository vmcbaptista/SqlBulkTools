using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    public class SimpleQueryForObject<T>
    {
        private readonly T _entity;
        private readonly List<SqlParameter> _sqlParams;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="sqlParams"></param>
        public SimpleQueryForObject(T entity, List<SqlParameter> sqlParams)
        {
            _entity = entity;
            _sqlParams = sqlParams;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public SimpleQueryTable<T> WithTable(string tableName)
        {
            return new SimpleQueryTable<T>(_entity, tableName, _sqlParams);
        }
    }
}
