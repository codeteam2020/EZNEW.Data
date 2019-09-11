using EZNEW.Develop.Entity;
using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.Config
{
    /// <summary>
    /// server type entity config
    /// </summary>
    public class ServerTypeEntityConfig
    {
        #region Propertys

        /// <summary>
        /// table name
        /// </summary>
        public string TableName
        {
            get; set;
        }

        /// <summary>
        /// fields
        /// </summary>
        public List<EntityField> Fields
        {
            get; set;
        }

        #endregion
    }
}
