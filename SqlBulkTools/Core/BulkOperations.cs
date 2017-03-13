using System;
using System.Dynamic;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkOperations : IBulkOperations
    {
        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Setup<T> Setup<T>() where T : class
        {
            if (typeof(T) == typeof(ExpandoObject))
                throw new ArgumentException("ExpandoObject is currently not supported.");

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