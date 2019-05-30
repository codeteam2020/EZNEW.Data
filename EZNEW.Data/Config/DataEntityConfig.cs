using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.Config
{
    /// <summary>
    /// entity & table config
    /// </summary>
    public class DataEntityConfig
    {
        /// <summary>
        /// entity server type config
        /// </summary>
        public Dictionary<ServerType, EntityServerTypeConfig> ServerEntitys
        {
            get;set;
        }
    }
}
