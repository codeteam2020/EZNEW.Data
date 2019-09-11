using EZNEW.Data.Config;
using EZNEW.Data.Translators;
using EZNEW.Develop.Entity;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.Translator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EZNEW.Data
{
    /// <summary>
    /// translator query object
    /// </summary>
    public static class QueryTranslator
    {
        /// <summary>
        /// translate query object
        /// </summary>
        /// <param name="query">query object</param>
        /// <param name="serverInfo">database server</param>
        /// <returns></returns>
        public static TranslateResult Translate(IQuery query, ServerInfo serverInfo)
        {
            var translator = GetTranslator(serverInfo);
            if (translator == null)
            {
                return TranslateResult.Empty;
            }
            return translator.Translate(query);
        }

        /// <summary>
        /// get translator
        /// </summary>
        /// <param name="server"></param>
        /// <returns></returns>
        public static IQueryTranslator GetTranslator(ServerInfo server)
        {
            if (server == null)
            {
                return null;
            }
            IQueryTranslator translator = null;
            if (DataManager.Translators.TryGetValue(server.ServerType, out translator) && translator != null)
            {
                return translator;
            }
            switch (server.ServerType)
            {
                case ServerType.SQLServer:
                    translator = new SqlServerQueryTranslator();
                    break;
                case ServerType.MySQL:
                    translator = new MySqlQueryTranslator();
                    break;
                case ServerType.Oracle:
                    translator = new OracleQueryTranslator();
                    break;
            }
            return translator;
        }
    }
}
