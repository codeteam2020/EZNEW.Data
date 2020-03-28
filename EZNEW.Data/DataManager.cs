using EZNEW.Develop.Entity;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Framework.Serialize;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using EZNEW.Framework.Extension;
using EZNEW.Develop.Command;
using EZNEW.Develop.CQuery;
using System.Linq;
using EZNEW.Framework.IoC;
using System.Data;
using EZNEW.Framework.Fault;
using System.Collections.Concurrent;
using System.IO;
using EZNEW.Data.Config;
using Dapper;
using EZNEW.Develop.DataAccess;
using EZNEW.Data.CriteriaConvert;
using EZNEW.Develop.CQuery.CriteriaConvert;

namespace EZNEW.Data
{
    /// <summary>
    /// db manager
    /// </summary>
    public static class DataManager
    {
        static DataManager()
        {
            ContainerManager.Container?.Register(typeof(ICommandEngine), typeof(DBCommandEngine));
            SqlMapper.Settings.ApplyNullValues = true;
            ConfigDefaultCriteriaConvertParse();
        }

        #region propertys

        /// <summary>
        /// get or set get database servers method
        /// </summary>
        public static Func<ICommand, Task<List<ServerInfo>>> GetDBServerAsync
        {
            get; set;
        }

        /// <summary>
        /// get db connection
        /// </summary>
        public static Func<ServerInfo, IDbConnection> GetDBConnection { get; set; }

        /// <summary>
        /// db engines
        /// </summary>
        internal static ConcurrentDictionary<ServerType, IDbEngine> DbEngines { get; } = new ConcurrentDictionary<ServerType, IDbEngine>();

        /// <summary>
        /// translators
        /// </summary>
        internal static ConcurrentDictionary<ServerType, IQueryTranslator> Translators { get; } = new ConcurrentDictionary<ServerType, IQueryTranslator>();

        /// <summary>
        /// servertype&entity object name
        /// key:servertype_entitytype id
        /// value:object name
        /// </summary>
        internal static ConcurrentDictionary<string, string> entityServerTypeCacheObjectNames = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// servertype&entity fields
        /// key:servertype_entitytype id
        /// value:fields
        /// </summary>
        internal static ConcurrentDictionary<string, ConcurrentDictionary<string, EntityField>> entityServerTypeCacheFields = new ConcurrentDictionary<string, ConcurrentDictionary<string, EntityField>>();

        /// <summary>
        /// servertype&entity query fields
        /// key:servertype_entity
        /// value:fieds
        /// </summary>
        internal static ConcurrentDictionary<string, List<EntityField>> entityServerTypeCacheQueryFields = new ConcurrentDictionary<string, List<EntityField>>();

        /// <summary>
        /// servertype&entity edit fields
        /// key:servertype_entity
        /// value:fields
        /// </summary>
        internal static ConcurrentDictionary<string, List<EntityField>> entityServerTypeCacheEditFields = new ConcurrentDictionary<string, List<EntityField>>();

        /// <summary>
        /// server type format entity keys
        /// </summary>
        static ConcurrentDictionary<ServerType, ConcurrentDictionary<Guid, string>> ServerTypeFormatEntityKeys = new ConcurrentDictionary<ServerType, ConcurrentDictionary<Guid, string>>();

        /// <summary>
        /// already config servertype entity
        /// </summary>
        static ConcurrentDictionary<string, bool> AlreadyConfigServerTypeEntityCollection = new ConcurrentDictionary<string, bool>();

        /// <summary>
        /// servertype batch execute config
        /// </summary>
        static Dictionary<ServerType, BatchExecuteConfig> ServerTypeExecuteConfigCollection = new Dictionary<ServerType, BatchExecuteConfig>();

        /// <summary>
        /// server data isolation level collection
        /// </summary>
        static Dictionary<ServerType, DataIsolationLevel> ServerTypeDataIsolationLevelCollection = new Dictionary<ServerType, DataIsolationLevel>()
        {
            { ServerType.MySQL,DataIsolationLevel.RepeatableRead },
            { ServerType.SQLServer,DataIsolationLevel.ReadCommitted },
            { ServerType.Oracle,DataIsolationLevel.ReadCommitted }
        };
        static Dictionary<DataIsolationLevel, IsolationLevel> DataIsolationLevelMapCollection = new Dictionary<DataIsolationLevel, IsolationLevel>()
        {
            { DataIsolationLevel.Chaos,IsolationLevel.Chaos },
            { DataIsolationLevel.ReadCommitted,IsolationLevel.ReadCommitted },
            { DataIsolationLevel.ReadUncommitted,IsolationLevel.ReadUncommitted },
            { DataIsolationLevel.RepeatableRead,IsolationLevel.RepeatableRead },
            { DataIsolationLevel.Serializable,IsolationLevel.Serializable },
            { DataIsolationLevel.Snapshot,IsolationLevel.Snapshot },
            { DataIsolationLevel.Unspecified,IsolationLevel.Unspecified }
        };

        /// <summary>
        /// criteria convert parse config
        /// </summary>
        static Dictionary<string, Func<CriteriaConvertParseOption, string>> CriteriaConvertParseCollection = null;

        #endregion

        #region methods

        #region data config

        /// <summary>
        /// data config
        /// </summary>
        /// <param name="config">data config</param>
        public static void Config(DataConfig config)
        {
            if (config == null || config.Servers == null || config.Servers.Count <= 0)
            {
                return;
            }
            foreach (var serverItem in config.Servers)
            {
                if (serverItem.Value == null)
                {
                    continue;
                }
                //register dbengine
                if (!serverItem.Value.EngineFullTypeName.IsNullOrEmpty())
                {
                    IDbEngine engine = (IDbEngine)Activator.CreateInstance(Type.GetType(serverItem.Value.EngineFullTypeName));
                    RegisterDBEngine(serverItem.Key, engine);
                }
                //register query translator
                if (!serverItem.Value.QueryTranslatorFullTypeName.IsNullOrEmpty())
                {
                    IQueryTranslator translator = (IQueryTranslator)Activator.CreateInstance(Type.GetType(serverItem.Value.QueryTranslatorFullTypeName));
                    RegisterQueryTranslator(serverItem.Key, translator);
                }
                //entity config
                if (!serverItem.Value.EntityConfigs.IsNullOrEmpty())
                {
                    foreach (var entityConfig in serverItem.Value.EntityConfigs)
                    {
                        ConfigServerTypeEntity(serverItem.Key, entityConfig.Key, entityConfig.Value);
                    }
                }
            }
        }

        /// <summary>
        /// config data through json
        /// </summary>
        /// <param name="dataConfigJson">json value</param>
        public static void Config(string dataConfigJson)
        {
            if (string.IsNullOrWhiteSpace(dataConfigJson))
            {
                return;
            }
            var dataConfig = JsonSerialize.JsonToObject<DataConfig>(dataConfigJson);
            if (dataConfig == null)
            {
                return;
            }
            Config(dataConfig);
        }

        /// <summary>
        /// config data access ghrough default config file
        /// </summary>
        /// <param name="configRootPath">data access config root path</param>
        public static void InitByConfigFile(string configRootPath = "")
        {
            if (configRootPath.IsNullOrEmpty())
            {
                configRootPath = Path.Combine(Directory.GetCurrentDirectory(), "Config/DataAccess");
            }
            if (!Directory.Exists(configRootPath))
            {
                return;
            }
            InitFolderConfig(configRootPath);
        }

        static void InitFolderConfig(string path)
        {
            if (string.IsNullOrWhiteSpace(path) || !Directory.Exists(path))
            {
                return;
            }
            var files = Directory.GetFiles(path).Where(c => Path.GetExtension(c).Trim('.').ToLower() == "daconfig").ToArray();
            if (!files.IsNullOrEmpty())
            {
                foreach (var file in files)
                {
                    var fileData = File.ReadAllText(file);
                    Config(fileData);
                }
            }
            var childFolders = new DirectoryInfo(path).GetDirectories();
            foreach (var folder in childFolders)
            {
                InitFolderConfig(folder.FullName);
            }
        }

        /// <summary>
        /// config server type entity
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="entityType">entity type id</param>
        /// <param name="entityConfig">entity config</param>
        static void ConfigServerTypeEntity(ServerType serverType, Type entityType, ServerTypeEntityConfig entityConfig, bool cover = true)
        {
            if (entityConfig == null)
            {
                return;
            }
            string key = serverType.GetServerTypeAndEntityKey(entityType);
            if (string.IsNullOrWhiteSpace(key))
            {
                return;
            }
            //config object name
            ConfigEntityObjectNameToServerType(key, entityConfig.TableName, cover);
            //config fields
            ConfigEntityFieldsToServerType(key, entityType, entityConfig.Fields, cover);
        }

        #endregion

        #region register db engine

        /// <summary>
        /// register dbengine
        /// </summary>
        /// <param name="serverType">db server type</param>
        /// <param name="dbEngine">db engine</param>
        public static void RegisterDBEngine(ServerType serverType, IDbEngine dbEngine)
        {
            if (dbEngine == null)
            {
                return;
            }
            DbEngines[serverType] = dbEngine;
        }

        #endregion

        #region register query translator

        /// <summary>
        /// register query translator
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="queryTranslator">query translator</param>
        public static void RegisterQueryTranslator(ServerType serverType, IQueryTranslator queryTranslator)
        {
            if (queryTranslator == null)
            {
                return;
            }
            Translators[serverType] = queryTranslator;
        }

        #endregion

        #region config entity objectname

        /// <summary>
        /// config entity object name to server type
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="entityType">entity type</param>
        /// <param name="objectName">object name</param>
        /// <param name="cover">cover now object name</param>
        public static void ConfigEntityObjectNameToServerType(ServerType serverType, Type entityType, string objectName, bool cover = true)
        {
            if (entityType == null)
            {
                return;
            }
            var key = serverType.GetServerTypeAndEntityKey(entityType);
            ConfigEntityObjectNameToServerType(key, objectName, cover);
        }

        /// <summary>
        /// config entity object name to server type
        /// </summary>
        /// <param name="serverTypeFormatEntityKey">servertype format entity key</param>
        /// <param name="objectName">object name</param>
        /// <param name="cover">cover now object name</param>
        static void ConfigEntityObjectNameToServerType(string serverTypeFormatEntityKey, string objectName, bool cover = true)
        {
            if (string.IsNullOrWhiteSpace(serverTypeFormatEntityKey) || string.IsNullOrWhiteSpace(objectName))
            {
                return;
            }
            if (cover || !entityServerTypeCacheObjectNames.ContainsKey(serverTypeFormatEntityKey))
            {
                entityServerTypeCacheObjectNames[serverTypeFormatEntityKey] = objectName;
            }
        }

        #endregion

        #region config entity fields

        /// <summary>
        /// config entity fields to server type
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="entityType">entity type</param>
        /// <param name="fields">fields</param>
        /// <param name="cover">conver</param>
        public static void ConfigEntityFieldsToServerType(ServerType serverType, Type entityType, IEnumerable<EntityField> fields, bool cover = true)
        {
            if (entityType == null)
            {
                return;
            }
            var key = serverType.GetServerTypeAndEntityKey(entityType);
            ConfigEntityFieldsToServerType(key, entityType, fields, cover);
        }

        /// <summary>
        /// config entity fields to server type
        /// </summary>
        /// <param name="serverTypeFormatEntityKey">servertype format entity key</param>
        /// <param name="fields">fields</param>
        /// <param name="cover">conver</param>
        static void ConfigEntityFieldsToServerType(string serverTypeFormatEntityKey, Type entityType, IEnumerable<EntityField> fields, bool cover = true)
        {
            if (string.IsNullOrWhiteSpace(serverTypeFormatEntityKey) || fields.IsNullOrEmpty())
            {
                return;
            }
            if (!AlreadyConfigServerTypeEntityCollection.ContainsKey(serverTypeFormatEntityKey))
            {
                ConfigEntityToServerType(serverTypeFormatEntityKey, entityType);
            }
            entityServerTypeCacheFields.TryGetValue(serverTypeFormatEntityKey, out var nowFields);
            nowFields = nowFields ?? new ConcurrentDictionary<string, EntityField>();
            List<EntityField> queryFields = new List<EntityField>();
            List<EntityField> editFields = new List<EntityField>();

            #region new fields

            foreach (var newField in fields)
            {
                if (newField?.PropertyName.IsNullOrEmpty() ?? true)
                {
                    continue;
                }
                nowFields.TryGetValue(newField.PropertyName, out var nowField);
                if (nowField == null || cover)
                {
                    nowField = newField;
                    nowFields[newField.PropertyName] = nowField;
                }
            }

            #endregion

            #region query&edit fields

            foreach (var field in nowFields.Values.OrderByDescending(c => c.IsPrimaryKey).ThenByDescending(c => c.CacheOption))
            {
                if (!field.IsDisableQuery)
                {
                    queryFields.Add(field);
                }
                if (!field.IsDisableEdit)
                {
                    editFields.Add(field);
                }
            }

            #endregion

            //query fields
            entityServerTypeCacheQueryFields[serverTypeFormatEntityKey] = queryFields;
            //edit fields
            entityServerTypeCacheEditFields[serverTypeFormatEntityKey] = editFields;
            //all fields
            entityServerTypeCacheFields[serverTypeFormatEntityKey] = nowFields;
        }

        #endregion

        #region generate servertype&entity key

        /// <summary>
        /// generate servertype&entit key
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="entityType">entity type</param>
        /// <returns></returns>
        static string GenerateServerTypeAndEntityKey(ServerType serverType, Type entityType)
        {
            if (entityType == null)
            {
                throw new EZNEWException("EntityType is null");
            }
            return string.Format("{0}_{1}", (int)serverType, entityType.GUID);
        }

        #endregion

        #region get servertype&entity key

        /// <summary>
        /// get servertype&entity key
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="entityType">entity type</param>
        /// <returns></returns>
        public static string GetServerTypeAndEntityKey(this ServerType serverType, Type entityType)
        {
            if (entityType == null)
            {
                return string.Empty;
            }
            var entityId = entityType.GUID;
            string key = string.Empty;
            ServerTypeFormatEntityKeys.TryGetValue(serverType, out var entityKeys);
            entityKeys?.TryGetValue(entityId, out key);
            if (string.IsNullOrWhiteSpace(key))
            {
                key = GenerateServerTypeAndEntityKey(serverType, entityType);
                if (entityKeys == null)
                {
                    entityKeys = new ConcurrentDictionary<Guid, string>();
                }
                entityKeys[entityId] = key;
                ServerTypeFormatEntityKeys[serverType] = entityKeys;
            }
            return key;
        }

        #endregion

        #region entity config

        /// <summary>
        /// register entity config
        /// </summary>
        /// <param name="entityConfig">entity config</param>
        static void ConfigEntityToServerType(ServerType serverType, Type entityType)
        {
            string key = serverType.GetServerTypeAndEntityKey(entityType);
            ConfigEntityToServerType(key, entityType);
        }

        /// <summary>
        /// register entity config
        /// </summary>
        /// <param name="serverTypeFormatEntityKey">servertype format entity key</param>
        /// <param name="entityType">entity type</param>
        static void ConfigEntityToServerType(string serverTypeFormatEntityKey, Type entityType)
        {
            if (string.IsNullOrWhiteSpace(serverTypeFormatEntityKey) || entityType == null || AlreadyConfigServerTypeEntityCollection.ContainsKey(serverTypeFormatEntityKey))
            {
                return;
            }
            var entityConfig = EntityManager.GetEntityConfig(entityType);
            if (entityConfig == null)
            {
                return;
            }
            AlreadyConfigServerTypeEntityCollection[serverTypeFormatEntityKey] = true;
            //config object name
            ConfigEntityObjectNameToServerType(serverTypeFormatEntityKey, entityConfig.TableName, false);
            //cofnig fields
            ConfigEntityFieldsToServerType(serverTypeFormatEntityKey, entityType, entityConfig.AllFields, false);
        }

        #endregion

        #region get entity object name

        /// <summary>
        ///get entity object name
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="entityType">entity type</param>
        /// <param name="searchEntityConfig">search entity config</param>
        /// <returns></returns>
        public static string GetEntityObjectName(ServerType serverType, Type entityType, string defaultName = "")
        {
            string key = serverType.GetServerTypeAndEntityKey(entityType);
            if (string.IsNullOrWhiteSpace(key))
            {
                return defaultName;
            }
            entityServerTypeCacheObjectNames.TryGetValue(key, out var cacheObjectName);
            if (string.IsNullOrWhiteSpace(cacheObjectName))
            {
                ConfigEntityToServerType(key, entityType);
                entityServerTypeCacheObjectNames.TryGetValue(key, out cacheObjectName);
                cacheObjectName = string.IsNullOrWhiteSpace(cacheObjectName) ? defaultName : cacheObjectName;
            }
            return cacheObjectName ?? string.Empty;
        }

        /// <summary>
        /// get entity object name
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="defaultName">default name</param>
        /// <param name="searchEntityConfig">search entity config</param>
        /// <returns></returns>
        public static string GetEntityObjectName<T>(ServerType serverType, string defaultName = "")
        {
            return GetEntityObjectName(serverType, typeof(T), defaultName);
        }

        /// <summary>
        /// get query object name
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="query">query object</param>
        /// <returns></returns>
        public static string GetQueryRelationObjectName(ServerType serverType, IQuery query)
        {
            if (query == null || query.EntityType == null)
            {
                return string.Empty;
            }
            return GetEntityObjectName(serverType, query.EntityType);
        }

        #endregion

        #region get field

        /// <summary>
        ///  get field
        /// </summary>
        /// <param name="serverType">db server type</param>
        /// <param name="entityType">entity type</param>
        /// <param name="propertyName">property name</param>
        /// <returns></returns>
        public static EntityField GetField(ServerType serverType, Type entityType, string propertyName)
        {
            if (propertyName.IsNullOrEmpty())
            {
                return string.Empty;
            }
            string key = serverType.GetServerTypeAndEntityKey(entityType);
            if (key.IsNullOrEmpty())
            {
                return string.Empty;
            }
            entityServerTypeCacheFields.TryGetValue(key, out var allFields);
            if (allFields.IsNullOrEmpty())
            {
                //config entity
                ConfigEntityToServerType(key, entityType);
                entityServerTypeCacheFields.TryGetValue(key, out allFields);
            }
            EntityField field = null;
            allFields?.TryGetValue(propertyName, out field);
            return field ?? propertyName;
        }

        /// <summary>
        ///  get field
        /// </summary>
        /// <param name="serverType">db server type</param>
        /// <param name="query">query</param>
        /// <param name="propertyName">property name</param>
        /// <returns></returns>
        public static EntityField GetField(ServerType serverType, IQuery query, string propertyName)
        {
            if (query == null || query.EntityType == null)
            {
                return propertyName;
            }
            return GetField(serverType, query.EntityType, propertyName);
        }

        #endregion

        #region get fields

        /// <summary>
        /// get fields
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="entityType">entity type</param>
        /// <param name="propertyNames">property name</param>
        /// <returns></returns>
        public static List<EntityField> GetFields(ServerType serverType, Type entityType, IEnumerable<string> propertyNames)
        {
            if (propertyNames.IsNullOrEmpty())
            {
                return new List<EntityField>(0);
            }
            var propertyFields = propertyNames.Select<string, EntityField>(p => p).ToList();
            var key = serverType.GetServerTypeAndEntityKey(entityType);
            if (key.IsNullOrEmpty())
            {
                return propertyFields;
            }
            entityServerTypeCacheFields.TryGetValue(key, out var allFields);
            if (allFields.IsNullOrEmpty())
            {
                ConfigEntityToServerType(key, entityType);
                entityServerTypeCacheFields.TryGetValue(key, out allFields);
                if (allFields.IsNullOrEmpty())
                {
                    return propertyFields;
                }
            }
            for (var p = 0; p < propertyFields.Count; p++)
            {
                var propertyField = propertyFields[p];
                if (allFields.TryGetValue(propertyField.PropertyName, out var nowField) && nowField != null)
                {
                    propertyFields[p] = nowField;
                }
            }
            return propertyFields;
        }

        #endregion

        #region get edit fields

        /// <summary>
        /// get edit fields
        /// </summary>
        /// <param name="entityType">entity type</param>
        /// <returns></returns>
        public static List<EntityField> GetEditFields(ServerType serverType, Type entityType)
        {
            string key = serverType.GetServerTypeAndEntityKey(entityType);
            if (key.IsNullOrEmpty())
            {
                return new List<EntityField>(0);
            }
            entityServerTypeCacheEditFields.TryGetValue(key, out var fields);
            if (fields.IsNullOrEmpty())
            {
                ConfigEntityToServerType(key, entityType);
                entityServerTypeCacheEditFields.TryGetValue(key, out fields);
            }
            return fields ?? new List<EntityField>(0);
        }

        #endregion

        #region get query fields

        /// <summary>
        /// get query fields
        /// </summary>
        /// <param name="entityType">entity type</param>
        /// <param name="query">query</param>
        /// <returns></returns>
        public static List<EntityField> GetQueryFields(ServerType serverType, Type entityType, IQuery query)
        {
            string key = serverType.GetServerTypeAndEntityKey(entityType);
            if (key.IsNullOrEmpty())
            {
                return new List<EntityField>(0);
            }
            entityServerTypeCacheQueryFields.TryGetValue(key, out var fields);
            if (fields.IsNullOrEmpty())
            {
                ConfigEntityToServerType(key, entityType);
                entityServerTypeCacheQueryFields.TryGetValue(key, out fields);
            }
            if (fields.IsNullOrEmpty())
            {
                throw new Exception("empty fields");
            }
            if (query.QueryFields.IsNullOrEmpty() && query.NotQueryFields.IsNullOrEmpty())
            {
                return fields;
            }
            var queryFields = query.GetActuallyQueryFields(entityType, true, true);
            return fields.Intersect(queryFields).ToList();
        }

        #endregion

        #region get default field

        /// <summary>
        /// get default field
        /// </summary>
        /// <param name="entityType">entity type</param>
        /// <returns></returns>
        public static EntityField GetDefaultField(ServerType serverType, Type entityType)
        {
            string key = serverType.GetServerTypeAndEntityKey(entityType);
            if (key.IsNullOrEmpty())
            {
                return string.Empty;
            }
            entityServerTypeCacheQueryFields.TryGetValue(key, out var fields);
            if (fields.IsNullOrEmpty())
            {
                ConfigEntityToServerType(key, entityType);
                entityServerTypeCacheQueryFields.TryGetValue(key, out fields);
            }
            EntityField field = null;
            if (fields?.Count > 0)
            {
                field = fields[0];
            }
            field = field ?? string.Empty;
            return field;
        }

        #endregion

        #region batch execute config

        #region config batch execute config

        /// <summary>
        /// config batch execute config
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <param name="batchExecuteConfig">batch execute config</param>
        public static void ConfigBatchExecute(ServerType serverType, BatchExecuteConfig batchExecuteConfig)
        {
            if (batchExecuteConfig == null)
            {
                return;
            }
            ServerTypeExecuteConfigCollection[serverType] = batchExecuteConfig;
        }

        #endregion

        #region get batch execute config

        /// <summary>
        /// get batch execute config
        /// </summary>
        /// <param name="serverType">server type</param>
        /// <returns></returns>
        public static BatchExecuteConfig GetBatchExecuteConfig(ServerType serverType)
        {
            ServerTypeExecuteConfigCollection.TryGetValue(serverType, out var config);
            return config;
        }

        #endregion

        #endregion

        #region isolation level

        #region config db server data isolation level

        /// <summary>
        /// config db server data isolation level
        /// </summary>
        /// <param name="serverType">db server type</param>
        /// <param name="dataIsolationLevel">data isolation level</param>
        public static void ConfigServerDataIsolationLevel(ServerType serverType, DataIsolationLevel dataIsolationLevel)
        {
            ServerTypeDataIsolationLevelCollection[serverType] = dataIsolationLevel;
        }

        #endregion

        #region get db server data isolation level

        /// <summary>
        /// get db server data isolation level
        /// </summary>
        /// <param name="serverType">db server type</param>
        /// <returns></returns>
        public static DataIsolationLevel? GetServerDataIsolationLevel(ServerType serverType)
        {
            if (ServerTypeDataIsolationLevelCollection.ContainsKey(serverType))
            {
                return ServerTypeDataIsolationLevelCollection[serverType];
            }
            return null;
        }

        #endregion

        #region get system isolation level by data isolation level

        /// <summary>
        /// get system isolation level by data isolation level
        /// </summary>
        /// <param name="dataIsolationLevel">data isolation level</param>
        /// <returns></returns>
        public static IsolationLevel? GetSystemIsolationLevel(DataIsolationLevel? dataIsolationLevel)
        {
            IsolationLevel? isolationLevel = null;
            if (dataIsolationLevel.HasValue && DataIsolationLevelMapCollection.ContainsKey(dataIsolationLevel.Value))
            {
                isolationLevel = DataIsolationLevelMapCollection[dataIsolationLevel.Value];
            }
            return isolationLevel;
        }

        #endregion

        #endregion

        #region criteria convert

        /// <summary>
        /// config default criteria convert parse
        /// </summary>
        static void ConfigDefaultCriteriaConvertParse()
        {
            CriteriaConvertParseCollection = new Dictionary<string, Func<CriteriaConvertParseOption, string>>();
            Func<CriteriaConvertParseOption, string> defaultParseFunc = option =>
             {
                 string format = null;
                 switch (option.ServerType)
                 {
                     case ServerType.SQLServer:
                         format = SqlServerCriteriaConvertParse.Parse(option);
                         break;
                     case ServerType.MySQL:
                         format = MySqlCriteriaConvertParse.Parse(option);
                         break;
                     case ServerType.Oracle:
                         format = OracleCriteriaConvertParse.Parse(option);
                         break;
                     default:
                         throw new EZNEWException($"cann't resolve criteria convert:{option?.CriteriaConvert?.Name}");
                 }
                 return format;
             };
            //string length
            CriteriaConvertParseCollection[CriteriaConvertNames.StringLength] = defaultParseFunc;
        }

        /// <summary>
        /// parse criteria convert
        /// </summary>
        /// <param name="convert">criteria convert</param>
        /// <param name="serverType">db server type</param>
        /// <param name="objectName">object name</param>
        /// <param name="fieldName">field name</param>
        /// <returns></returns>
        internal static string ParseCriteriaConvert(ICriteriaConvert convert, ServerType serverType, string objectName, string fieldName)
        {
            if (CriteriaConvertParseCollection.TryGetValue(convert.Name, out var parser))
            {
                return parser(new CriteriaConvertParseOption()
                {
                    CriteriaConvert = convert,
                    ServerType = serverType,
                    ObjectName = objectName,
                    FieldName = fieldName
                });
            }
            throw new EZNEWException($"didn't config criteria convert:{convert?.Name}");
        }

        /// <summary>
        /// config parse criteria
        /// </summary>
        /// <param name="convertConfigName">convert config name</param>
        /// <param name="convertParseFunc">convert parse func</param>
        public static void ConfigParseCriteria(string convertConfigName, Func<CriteriaConvertParseOption, string> convertParseFunc)
        {
            if (string.IsNullOrWhiteSpace(convertConfigName) || convertParseFunc == null)
            {
                return;
            }
            CriteriaConvertParseCollection[convertConfigName] = convertParseFunc;
        }

        #endregion

        #endregion
    }
}
