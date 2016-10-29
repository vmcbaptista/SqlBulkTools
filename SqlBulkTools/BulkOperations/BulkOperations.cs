// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkOperations
    {
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