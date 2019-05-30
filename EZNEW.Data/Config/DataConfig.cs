using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.Config
{
    /// <summary>
    /// db engine config
    /// </summary>
    public class DataConfig
    {
        /// <summary>
        /// servers
        /// </summary>
        public Dictionary<ServerType, DBServerConfig> Servers
        {
            get;set;
        }
    }
}
