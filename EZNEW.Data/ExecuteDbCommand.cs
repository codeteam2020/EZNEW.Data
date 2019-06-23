using EZNEW.Develop.Command;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace EZNEW.Data
{
    /// <summary>
    /// execute db command
    /// </summary>
    public class DbExecuteCommand
    {
        /// <summary>
        /// command text
        /// </summary>
        public string CommandText
        {
            get; set;
        }

        /// <summary>
        /// commandType
        /// </summary>
        public CommandType CommandType
        {
            get; set;
        }

        /// <summary>
        /// parameters
        /// </summary>
        public CmdParameters Parameters
        {
            get; set;
        }

        /// <summary>
        /// force return value
        /// </summary>
        public bool ForceReturnValue
        {
            get; set;
        }

        /// <summary>
        /// has pre script
        /// </summary>
        public bool HasPreScript
        {
            get; set;
        }

        /// <summary>
        /// perform alone
        /// </summary>
        public bool PerformAlone
        {
            get
            {
                return HasPreScript || CommandType == CommandType.StoredProcedure;
            }
        }
    }
}
