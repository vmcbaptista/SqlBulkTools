using System;
using System.Linq.Expressions;
using SqlBulkTools.Enumeration;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class PredicateCondition
    {
        /// <summary>
        /// 
        /// </summary>
        public string LeftName { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string CustomColumnMapping { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Value { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public Type ValueType { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public ExpressionType Expression { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public int SortOrder { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public PredicateType PredicateType { get; set; }
    }
}
