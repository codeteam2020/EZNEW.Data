using MicBeach.Develop.Command;
using MicBeach.Develop.CQuery.Translator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MicBeach.Data
{
    /// <summary>
    /// DBConfig
    /// </summary>
    public static class DBConfig
    {
        #region Propertys

        /// <summary>
        /// get or set get database servers method
        /// </summary>
        public static Func<ICommand, IEnumerable<ServerInfo>> GetDBServerMethod
        {
            get; set;
        }

        /// <summary>
        /// db engines
        /// </summary>
        public static Dictionary<ServerType, IDbEngine> DbEngines { get; } = new Dictionary<ServerType, IDbEngine>();

        /// <summary>
        /// translators
        /// </summary>
        public static Dictionary<ServerType, IQueryTranslator> Translators { get; } = new Dictionary<ServerType, IQueryTranslator>();

        #endregion
    }
}
