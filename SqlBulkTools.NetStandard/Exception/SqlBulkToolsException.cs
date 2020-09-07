using System;
using System.Runtime.CompilerServices;
using SqlBulkTools.Enumeration;

[assembly: InternalsVisibleTo("SqlBulkTools.NetStandard.IntegrationTests")]
// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    internal class SqlBulkToolsException : Exception
    {
        public SqlBulkToolsException(string message) : base(message)
        {

        }
    }
}
