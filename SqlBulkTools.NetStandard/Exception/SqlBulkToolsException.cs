using System;
using SqlBulkTools.Enumeration;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    public class SqlBulkToolsException : Exception
    {
        public SqlBulkToolsException(string message) : base(message)
        {

        }
    }
}
