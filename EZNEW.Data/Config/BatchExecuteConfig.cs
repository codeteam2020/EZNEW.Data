using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.Config
{
    /// <summary>
    /// batch execute config
    /// </summary>
    public class BatchExecuteConfig
    {
        /// <summary>
        /// group statements count
        /// </summary>
        public int GroupStatementsCount
        {
            get; set;
        } = 1000;

        /// <summary>
        /// group parameters count
        /// </summary>
        public int GroupParametersCount
        {
            get; set;
        } = 2000;

        public static readonly BatchExecuteConfig Default = new BatchExecuteConfig();
    }
}
