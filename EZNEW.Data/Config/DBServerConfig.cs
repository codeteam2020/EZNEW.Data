using EZNEW.Develop.CQuery.Translator;
using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.Config
{
    /// <summary>
    /// db server config
    /// </summary>
    public class DBServerConfig
    {
        /// <summary>
        /// db engine
        /// </summary>
        public string EngineFullTypeName
        {
            get;set;
        }

        /// <summary>
        /// query translator
        /// </summary>
        public string QueryTranslatorFullTypeName
        {
            get;set;
        }
    }
}
