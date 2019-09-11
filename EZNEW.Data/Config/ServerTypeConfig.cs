using EZNEW.Develop.Entity;
using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.Config
{
    /// <summary>
    /// servertype entity config
    /// </summary>
    public class ServerTypeConfig
    {
        /// <summary>
        /// db engine
        /// </summary>
        public string EngineFullTypeName
        {
            get; set;
        }

        /// <summary>
        /// query translator
        /// </summary>
        public string QueryTranslatorFullTypeName
        {
            get; set;
        }

        /// <summary>
        /// entity config
        /// </summary>
        public Dictionary<Type, ServerTypeEntityConfig> EntityConfigs
        {
            get; set;
        }

        /// <summary>
        /// batch execute config
        /// </summary>
        public BatchExecuteConfig BatchExecuteConfig
        {
            get; set;
        }
    }
}
