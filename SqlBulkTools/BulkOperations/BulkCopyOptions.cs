using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkCopySettings
    {
        /// <summary>
        /// 
        /// </summary>
        public int BulkCopyTimeout { get; set; } = 600;
        /// <summary>
        /// 
        /// </summary>
        public bool EnableStreaming { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } = SqlBulkCopyOptions.Default;

        /// <summary>
        /// 
        /// </summary>
        public int BatchSize { get; set; } = 5000;

        /// <summary>
        /// 
        /// </summary>
        public BulkCopyNotification BulkCopyNotification { get; set; }

    }

    /// <summary>
    /// 
    /// </summary>
    public class BulkCopyNotification
    {
        /// <summary>
        /// Occurs every time the number of rows defined by NotifyAfter has processed
        /// </summary>
        public SqlRowsCopiedEventHandler SqlRowsCopied { get; set; }
        /// <summary>
        /// Defines the number of rows to be processed before generated a notification event
        /// </summary>
        public int NotifyAfter { get; set; }
    }
}
