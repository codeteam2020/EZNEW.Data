using EZNEW.Develop.Command;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EZNEW.Framework.Paging;
using EZNEW.Framework.Extension;
using System.Linq.Expressions;
using System.Reflection;
using EZNEW.Data.Config;
using EZNEW.Develop.Entity;

namespace EZNEW.Data
{
    /// <summary>
    /// db command engine
    /// </summary>
    public class DBCommandEngine : ICommandEngine
    {
        static DBCommandEngine()
        {
        }

        const string identityKey = "eznew_data_defaultdbcommandengine";

        /// <summary>
        /// identity key
        /// </summary>
        public string IdentityKey
        {
            get
            {
                return identityKey;
            }
            set
            {
            }
        }

        #region execute

        /// <summary>
        /// execute command
        /// </summary>
        /// <param name="cmds">commands</param>
        /// <returns>date numbers </returns>
        public int Execute(params ICommand[] cmds)
        {
            return ExecuteAsync(cmds).Result;
        }

        /// <summary>
        /// execute command
        /// </summary>
        /// <param name="cmds">commands</param>
        /// <returns>date numbers </returns>
        public async Task<int> ExecuteAsync(params ICommand[] cmds)
        {
            if (cmds.IsNullOrEmpty())
            {
                return 0;
            }
            Dictionary<string, List<ICommand>> commandGroup = new Dictionary<string, List<ICommand>>();
            Dictionary<string, ServerInfo> serverInfos = new Dictionary<string, ServerInfo>();

            #region get database servers

            foreach (var cmd in cmds)
            {
                var servers = await GetServerAsync(cmd).ConfigureAwait(false);
                foreach (var server in servers)
                {
                    string serverKey = server.Key;
                    if (serverInfos.ContainsKey(serverKey))
                    {
                        commandGroup[serverKey].Add(cmd);
                    }
                    else
                    {
                        commandGroup.Add(serverKey, new List<ICommand>() { cmd });
                        serverInfos.Add(serverKey, server);
                    }
                }
            }

            #endregion

            #region verify database server engine

            IEnumerable<ServerType> serverTypeList = serverInfos.Values.Select(c => c.ServerType).Distinct();
            VerifyServerEngine(serverTypeList.ToArray());

            #endregion

            #region execute commands

            int totalVal = 0;
            foreach (var cmdGroup in commandGroup)
            {
                ServerInfo serverInfo = serverInfos[cmdGroup.Key];
                IDbEngine engine = DataManager.DbEngines[serverInfo.ServerType];
                totalVal += await engine.ExecuteAsync(serverInfo, cmdGroup.Value.ToArray()).ConfigureAwait(false);
            }
            return totalVal;

            #endregion
        }

        #endregion

        #region query datas

        /// <summary>
        /// determine whether data is exist
        /// </summary>
        /// <param name="cmd">command</param>
        /// <returns>data is exist</returns>
        public bool Query(ICommand cmd)
        {
            return QueryAsync(cmd).Result;
        }

        /// <summary>
        /// determine whether data is exist
        /// </summary>
        /// <param name="cmd">command</param>
        /// <returns>data is exist</returns>
        public async Task<bool> QueryAsync(ICommand cmd)
        {
            var servers = await GetServerAsync(cmd).ConfigureAwait(false);
            VerifyServerEngine(servers.Select(c => c.ServerType).ToArray());
            bool result = false;
            foreach (var server in servers)
            {
                var engine = DataManager.DbEngines[server.ServerType];
                result = result || await engine.QueryAsync(server, cmd).ConfigureAwait(false);
                if (result)
                {
                    break;
                }
            }
            return result;
        }

        /// <summary>
        /// execute query
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns>datas</returns>
        public IEnumerable<T> Query<T>(ICommand cmd)
        {
            return QueryAsync<T>(cmd).Result;
        }

        /// <summary>
        /// execute query
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns>datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(ICommand cmd)
        {
            var servers = await GetServerAsync(cmd).ConfigureAwait(false);
            VerifyServerEngine(servers.Select(c => c.ServerType).ToArray());
            IEnumerable<T> dataList = null;
            if (servers.Count == 1)
            {
                var nowServer = servers[0];
                var engine = DataManager.DbEngines[nowServer.ServerType];
                dataList = engine.Query<T>(nowServer, cmd);
            }
            else
            {
                bool notOrder = cmd.Query == null || cmd.Query.Orders.IsNullOrEmpty();
                int dataSize = cmd.Query?.QuerySize ?? 0;
                var entityCompare = new EntityCompare<T>();
                foreach (var server in servers)
                {
                    var engine = DataManager.DbEngines[server.ServerType];
                    var newDataList = await engine.QueryAsync<T>(server, cmd).ConfigureAwait(false);
                    dataList = dataList == null ? newDataList : dataList.Union(newDataList, entityCompare);//merge data
                    if (dataSize > 0 && dataList.Count() >= dataSize && notOrder)
                    {
                        return dataList.Take(dataSize);
                    }
                }
                if (!notOrder)
                {
                    dataList = cmd.Query.Order(dataList);
                }
                if (dataSize > 0 && dataList.Count() > dataSize)
                {
                    dataList = dataList.Take(dataSize);
                }
            }
            return dataList;
        }

        /// <summary>
        /// query data with paging
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        public IPaging<T> QueryPaging<T>(ICommand cmd) where T : BaseEntity<T>
        {
            return QueryPagingAsync<T>(cmd).Result;
        }

        /// <summary>
        /// query data with paging
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        public async Task<IPaging<T>> QueryPagingAsync<T>(ICommand cmd) where T : BaseEntity<T>
        {
            var servers = await GetServerAsync(cmd).ConfigureAwait(false);
            VerifyServerEngine(servers.Select(c => c.ServerType).ToArray());

            #region single server

            if (servers.Count == 1)
            {
                return await SingleServerPagingAsync<T>(servers[0], cmd).ConfigureAwait(false);
            }

            #endregion

            #region multiple server

            IEnumerable<T> datas = new List<T>();
            int pageSize = cmd.Query.PagingInfo.PageSize;
            int page = cmd.Query.PagingInfo.Page;
            long totalCount = 0;
            cmd.Query.PagingInfo.PageSize = page * pageSize;
            cmd.Query.PagingInfo.Page = 1;
            foreach (var server in servers)
            {
                var serverPaging = await SingleServerPagingAsync<T>(server, cmd).ConfigureAwait(false);
                datas = datas.Union(serverPaging);
                totalCount += serverPaging.TotalCount;
            }
            if (cmd.Query != null)
            {
                datas = cmd.Query.Order(datas);
            }
            if (datas.Count() > pageSize)
            {
                datas = datas.Skip((page - 1) * pageSize).Take(pageSize);
            }
            return new Paging<T>(page, pageSize, totalCount, datas);

            #endregion
        }

        /// <summary>
        /// query paging with single server
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        async Task<IPaging<T>> SingleServerPagingAsync<T>(ServerInfo server, ICommand cmd) where T : BaseEntity<T>
        {
            var engine = DataManager.DbEngines[server.ServerType];
            IEnumerable<T> dataList = await engine.QueryPagingAsync<T>(server, cmd).ConfigureAwait(false);
            if (dataList.IsNullOrEmpty())
            {
                return new Paging<T>(1, 0, 0, dataList);
            }
            int totalCount = dataList.ElementAt(0).GetPagingTotalCount();
            int page = 1;
            int pageSize = 1;
            if (cmd.Query != null && cmd.Query.PagingInfo != null)
            {
                page = cmd.Query.PagingInfo.Page;
                pageSize = cmd.Query.PagingInfo.PageSize;
            }
            Paging<T> dataPaging = new Paging<T>(page, pageSize, totalCount, dataList);
            return dataPaging;
        }

        /// <summary>
        /// query a single data
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns>data</returns>
        public T QuerySingle<T>(ICommand cmd)
        {
            return QuerySingleAsync<T>(cmd).Result;
        }

        /// <summary>
        /// query a single data
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns>data</returns>
        public async Task<T> QuerySingleAsync<T>(ICommand cmd)
        {
            T result;
            switch (cmd.Operate)
            {
                case OperateType.Max:
                case OperateType.Min:
                case OperateType.Sum:
                case OperateType.Avg:
                case OperateType.Count:
                    result = await AggregateFunctionAsync<T>(cmd).ConfigureAwait(false);
                    break;
                case OperateType.Query:
                    result = await QuerySingleObjectAsync<T>(cmd).ConfigureAwait(false);
                    break;
                default:
                    result = default(T);
                    break;
            }
            return result;
        }

        /// <summary>
        /// query a single data
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        async Task<T> QuerySingleObjectAsync<T>(ICommand cmd)
        {
            var servers = await GetServerAsync(cmd).ConfigureAwait(false);
            VerifyServerEngine(servers.Select(c => c.ServerType).ToArray());
            T result = default(T);
            foreach (var server in servers)
            {
                var engine = DataManager.DbEngines[server.ServerType];
                var nowData = await engine.QuerySingleAsync<T>(server, cmd).ConfigureAwait(false);
                if (nowData != null)
                {
                    result = nowData;
                    break;
                }
            }
            return result;
        }

        /// <summary>
        /// Aggregate Function
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="cmd">command</param>
        /// <returns>query data</returns>
        async Task<T> AggregateFunctionAsync<T>(ICommand cmd)
        {
            var servers = await GetServerAsync(cmd).ConfigureAwait(false);
            VerifyServerEngine(servers.Select(c => c.ServerType).ToArray());
            List<T> datas = new List<T>(servers.Count);
            foreach (var server in servers)
            {
                var engine = DataManager.DbEngines[server.ServerType];
                datas.Add(await engine.QuerySingleAsync<T>(server, cmd).ConfigureAwait(false));
            }
            if (datas.Count == 1)
            {
                return datas[0];
            }
            dynamic result = default(T);
            switch (cmd.Operate)
            {
                case OperateType.Max:
                    result = datas.Max();
                    break;
                case OperateType.Min:
                    result = datas.Min();
                    break;
                case OperateType.Sum:
                case OperateType.Count:
                    result = Sum(datas);
                    break;
                case OperateType.Avg:
                    result = Average(datas);
                    break;
            }
            return result;
        }

        #endregion

        #region Helper

        /// <summary>
        /// get servers
        /// </summary>
        /// <param name="command">command</param>
        /// <returns>server list</returns>
        static async Task<List<ServerInfo>> GetServerAsync(ICommand command)
        {
            if (command == null || DataManager.GetDBServerAsync == null)
            {
                return null;
            }
            //if (command.Query != null)
            //{
            //    command.Query.ObjectName = command.ObjectName;
            //}
            var servers = await DataManager.GetDBServerAsync(command).ConfigureAwait(false);
            if (servers.IsNullOrEmpty())
            {
                throw new Exception("any ICommand cann't get server");
            }
            return servers.ToList();
        }

        /// <summary>
        /// verify server engine
        /// </summary>
        /// <param name="serverTypes">server types</param>
        void VerifyServerEngine(params ServerType[] serverTypes)
        {
            if (serverTypes == null)
            {
                return;
            }
            if (DataManager.DbEngines == null || DataManager.DbEngines.Count <= 0)
            {
                throw new Exception("not config any IDbEngine Data");
            }
            foreach (var serverType in serverTypes)
            {
                if (!DataManager.DbEngines.ContainsKey(serverType) || DataManager.DbEngines[serverType] == null)
                {
                    throw new Exception(string.Format("ServerType:{0} not special execute engine", serverType.ToString()));
                }
            }
        }

        /// <summary>
        /// calculate sum
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="datas">data list</param>
        /// <returns></returns>
        dynamic Sum<T>(IEnumerable<T> datas)
        {
            dynamic result = default(T);
            foreach (dynamic data in datas)
            {
                result += data;
            }
            return result;
        }

        /// <summary>
        /// calculate average
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="datas">data list</param>
        /// <returns></returns>
        dynamic Average<T>(IEnumerable<T> datas)
        {
            dynamic result = default(T);
            int count = 0;
            foreach (dynamic data in datas)
            {
                result += data;
                count++;
            }
            return result / count;
        }

        /// <summary>
        /// distinct and sort
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="datas">datas</param>
        /// <param name="cmd">command</param>
        /// <returns></returns>
        IEnumerable<T> DistinctAndOrder<T>(IEnumerable<T> datas, ICommand cmd)
        {
            if (datas == null || !datas.Any())
            {
                return datas;
            }
            datas = datas.Distinct(new EntityCompare<T>());
            //order data
            if (cmd.Query != null)
            {
                datas = cmd.Query.Order(datas);
            }
            return datas;
        }

        #endregion
    }
}
