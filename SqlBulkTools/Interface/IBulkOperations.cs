 // ReSharper disable once CheckNamespace

using SqlBulkTools.Core;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public interface IBulkOperations
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        Setup Setup();
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        Setup<T> Setup<T>() where T : class;
    }
}