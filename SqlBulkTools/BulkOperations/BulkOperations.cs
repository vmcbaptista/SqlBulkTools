using System.Data.SqlClient;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkOperations
    {
        private ITransaction _sqlBulkToolsTransaction;
        private SqlTransaction _sqlTransaction;

        internal void SetBulkExt(ITransaction sqlBulkToolsTransaction)
        {
            _sqlBulkToolsTransaction = sqlBulkToolsTransaction;
        }

        internal void SetTransaction(SqlTransaction sqlTransaction)
        {
            _sqlTransaction = sqlTransaction;
        }

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Setup<T> Setup<T>()
        {
            return new Setup<T>(this);
        }

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <returns></returns>
        public Setup Setup()
        {
            return new Setup(this);
        }
    }

}