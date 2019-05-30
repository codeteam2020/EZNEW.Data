using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EZNEW.Data
{
    /// <summary>
    /// DB Server
    /// </summary>
    public class ServerInfo
    {
        #region fields

        string _connectionString = string.Empty;
        ServerType _serverType;

        #endregion

        #region Propertys

        /// <summary>
        /// Server Key
        /// </summary>
        public string Key { get; private set; } = string.Empty;

        /// <summary>
        /// Connection String
        /// </summary>
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                _connectionString = value;
                InitKey();
            }
        }

        /// <summary>
        /// Server Type
        /// </summary>
        public ServerType ServerType
        {
            get
            {
                return _serverType;
            }
            set
            {
                _serverType = value;
                InitKey();
            }
        }

        #endregion

        #region Methods

        void InitKey()
        {
            Key = string.Format("{0}_{1}", (int)_serverType, _connectionString);
        }

        public override bool Equals(object otherServer)
        {
            if (otherServer == null)
            {
                return false;
            }
            ServerInfo otherServerInfo = otherServer as ServerInfo;
            if (otherServerInfo == null)
            {
                return false;
            }
            return Key == otherServerInfo.Key;
        }

        public override int GetHashCode()
        {
            return Key.GetHashCode();
        }

        #endregion
    }
}
