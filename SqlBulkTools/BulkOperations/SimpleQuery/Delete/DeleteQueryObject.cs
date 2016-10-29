namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeleteQueryObject<T>
    {

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public DeleteQueryTable<T> WithTable(string tableName)
        {
            return new DeleteQueryTable<T>(tableName);
        }
    }
}
