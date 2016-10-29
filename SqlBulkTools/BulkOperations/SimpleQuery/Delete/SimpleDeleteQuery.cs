namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SimpleDeleteQuery<T>
    {

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public SimpleDeleteQueryTable<T> WithTable(string tableName)
        {
            return new SimpleDeleteQueryTable<T>(tableName);
        }
    }
}
