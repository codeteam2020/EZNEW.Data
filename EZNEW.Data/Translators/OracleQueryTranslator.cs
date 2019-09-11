using EZNEW.Data.Config;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.CriteriaConvert;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Develop.Entity;
using EZNEW.Framework.Extension;
using EZNEW.Framework.Fault;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EZNEW.Data.Translators
{
    /// <summary>
    /// query translator implement for oracle
    /// </summary>
    public class OracleQueryTranslator : IQueryTranslator
    {
        #region Fields

        const string EqualOperator = "=";
        const string GreaterThanOperator = ">";
        const string GreaterThanOrEqualOperator = ">=";
        const string NotEqualOperator = "<>";
        const string LessThanOperator = "<";
        const string LessThanOrEqualOperator = "<=";
        const string InOperator = "IN";
        const string NotInOperator = "NOT IN";
        const string LikeOperator = "LIKE";
        const string NotLikeOperator = "NOT LIKE";
        public const string ObjPetName = "TB";
        int subObjectSequence = 0;
        int recurveObjectSequence = 0;
        string parameterPrefix = ":";
        const string TreeTableName = "RecurveTable";
        const string TreeTablePetName = "RTT";
        static Dictionary<JoinType, string> joinOperatorDict = new Dictionary<JoinType, string>()
        {
            { JoinType.InnerJoin,"INNER JOIN" },
            { JoinType.CrossJoin,"CROSS JOIN" },
            { JoinType.LeftJoin,"LEFT JOIN" },
            { JoinType.RightJoin,"RIGHT JOIN" },
            { JoinType.FullJoin,"FULL JOIN" }
        };

        #endregion

        #region Propertys

        /// <summary>
        /// Query Object Pet Name
        /// </summary>
        public string ObjectPetName
        {
            get
            {
                return ObjPetName;
            }
        }

        /// <summary>
        /// parameter sequence
        /// </summary>
        public int ParameterSequence { get; set; } = 0;

        #endregion

        #region Functions

        /// <summary>
        /// Translate Query Object
        /// </summary>
        /// <param name="query">query object</param>
        /// <returns>translate result</returns>
        public TranslateResult Translate(IQuery query)
        {
            Init();
            var result = ExecuteTranslate(query);
            if (!result.WithScripts.IsNullOrEmpty())
            {
                result.PreScript = FormatWithScript(result.WithScripts);
            }
            return result;
        }

        /// <summary>
        /// Execute Translate
        /// </summary>
        /// <param name="query">query object</param>
        /// <param name="paras">parameters</param>
        /// <param name="objectName">query object name</param>
        /// <returns></returns>
        public TranslateResult ExecuteTranslate(IQuery query, Dictionary<string, object> paras = null, string objectName = "", bool subQuery = false)
        {
            if (query == null)
            {
                return TranslateResult.Empty;
            }
            StringBuilder conditionBuilder = new StringBuilder();
            if (query.QueryType == QueryCommandType.QueryObject)
            {
                StringBuilder orderBuilder = new StringBuilder();
                Dictionary<string, object> parameters = paras ?? new Dictionary<string, object>();
                objectName = string.IsNullOrWhiteSpace(objectName) ? ObjPetName : objectName;
                List<string> withScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;

                #region query condition

                if (query.Criterias != null && query.Criterias.Count > 0)
                {
                    int index = 0;
                    foreach (var queryItem in query.Criterias)
                    {
                        var queryItemCondition = TranslateCondition(query, queryItem, parameters, objectName);
                        if (!queryItemCondition.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(queryItemCondition.WithScripts);
                            recurveTableName = queryItemCondition.RecurveObjectName;
                            recurveTablePetName = queryItemCondition.RecurvePetName;
                        }
                        conditionBuilder.AppendFormat("{0} {1}"
                            , index > 0 ? " " + queryItem.Item1.ToString() : ""
                            , queryItemCondition.ConditionString);
                        index++;
                    }
                }

                #endregion

                #region sort

                if (!subQuery && query.Orders != null && query.Orders.Count > 0)
                {
                    foreach (var orderItem in query.Orders)
                    {
                        orderBuilder.AppendFormat("{0} {1},"
                            , ConvertOrderCriteriaName(query, objectName, orderItem)
                            , orderItem.Desc ? "DESC" : "ASC");
                    }
                }

                #endregion

                #region join

                bool allowJoin = true;
                StringBuilder joinBuilder = new StringBuilder();
                if (!query.JoinItems.IsNullOrEmpty())
                {
                    foreach (var joinItem in query.JoinItems)
                    {
                        if (joinItem == null || joinItem.JoinQuery == null)
                        {
                            continue;
                        }
                        if (joinItem.JoinQuery.EntityType == null)
                        {
                            throw new EZNEWException("IQuery object must set entity type if use in join operation");
                        }
                        string joinObjName = "TSB" + subObjectSequence;
                        subObjectSequence++;
                        var joinQueryResult = ExecuteTranslate(joinItem.JoinQuery, parameters, joinObjName, true);
                        if (!joinQueryResult.ConditionString.IsNullOrEmpty())
                        {
                            conditionBuilder.AppendFormat("{0}{1}"
                                , conditionBuilder.Length == 0 ? "" : " AND"
                                , joinQueryResult.ConditionString);
                        }
                        joinBuilder.AppendFormat(" {0} {1} {2}{3}"
                            , GetJoinOperator(joinItem.JoinType)
                            , DataManager.GetQueryRelationObjectName(ServerType.Oracle, joinItem.JoinQuery)
                            , joinObjName
                            , GetJoinCondition(query, joinItem, objectName, joinObjName));
                        if (!joinQueryResult.JoinScript.IsNullOrEmpty())
                        {
                            joinBuilder.AppendFormat(" {0}", joinQueryResult.JoinScript);
                        }
                        if (!joinQueryResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(joinQueryResult.WithScripts);
                            recurveTableName = joinQueryResult.RecurveObjectName;
                            recurveTablePetName = joinQueryResult.RecurvePetName;
                        }
                    }
                }
                string joinScript = joinBuilder.ToString();

                #endregion

                #region recurve script

                string conditionString = conditionBuilder.ToString();
                if (query.RecurveCriteria != null)
                {
                    allowJoin = false;
                    string nowConditionString = conditionBuilder.ToString();
                    EntityField recurveField = DataManager.GetField(ServerType.Oracle, query, query.RecurveCriteria.Key);
                    EntityField recurveRelationField = DataManager.GetField(ServerType.Oracle, query, query.RecurveCriteria.RelationKey);
                    var recurveIndex = recurveObjectSequence++;
                    recurveTableName = string.Format("{0}{1}", TreeTableName, recurveIndex);
                    recurveTablePetName = string.Format("{0}{1}", TreeTablePetName, recurveIndex);
                    conditionString = string.Format("{0}.{1} IN (SELECT {3}.{1} FROM {2} {3})"
                        , objectName
                        , recurveField.FieldName
                        , recurveTableName
                        , recurveTablePetName);
                    string firstObjectPetName = objectName;
                    string secondObjectPetName = objectName;
                    string firstTreeTablePetName = recurveTablePetName;
                    string queryObjectName = DataManager.GetQueryRelationObjectName(ServerType.Oracle, query);
                    string withScript = string.Format("{0} AS (SELECT {1}.{2},{1}.{3} FROM {4} {1}{8} {5} UNION ALL SELECT {6}.{2},{6}.{3} FROM {4} {6} JOIN {0} {7}"
                        , recurveTableName
                        , firstObjectPetName
                        , recurveField.FieldName
                        , recurveRelationField.FieldName
                        , queryObjectName
                        , string.IsNullOrWhiteSpace(nowConditionString) ? "" : string.Format("WHERE {0}", nowConditionString)
                        , secondObjectPetName
                        , firstTreeTablePetName
                        , joinScript);
                    if (query.RecurveCriteria.Direction == RecurveDirection.Up)
                    {
                        withScript = string.Format("{0} ON {1}.{2}={3}.{4})"
                            , withScript
                            , secondObjectPetName
                            , recurveField.FieldName
                            , firstTreeTablePetName
                            , recurveRelationField.FieldName);
                    }
                    else
                    {
                        withScript = string.Format("{0} ON {1}.{2}={3}.{4})"
                            , withScript
                            , secondObjectPetName
                            , recurveRelationField.FieldName
                            , firstTreeTablePetName
                            , recurveField.FieldName);
                    }
                    withScripts.Add(withScript);
                }
                var result = TranslateResult.CreateNewResult(conditionString, orderBuilder.ToString().Trim(','), parameters);
                result.JoinScript = joinScript;
                result.AllowJoin = allowJoin;
                result.WithScripts = withScripts;
                result.RecurveObjectName = recurveTableName;
                result.RecurvePetName = recurveTablePetName;

                #endregion

                return result;
            }
            else
            {
                conditionBuilder.Append(query.QueryText);
                return TranslateResult.CreateNewResult(conditionBuilder.ToString(), string.Empty, query.QueryTextParameters);
            }
        }

        /// <summary>
        /// translate query condition
        /// </summary>
        /// <param name="queryItem">query condition</param>
        /// <returns></returns>
        TranslateResult TranslateCondition(IQuery query, Tuple<QueryOperator, IQueryItem> queryItem, Dictionary<string, object> parameters, string objectName)
        {
            if (queryItem == null)
            {
                return TranslateResult.Empty;
            }
            Criteria criteria = queryItem.Item2 as Criteria;
            if (criteria != null)
            {
                return TranslateCriteria(query, criteria, parameters, objectName);
            }
            IQuery groupQuery = queryItem.Item2 as IQuery;
            if (groupQuery != null && groupQuery.Criterias != null && groupQuery.Criterias.Count > 0)
            {
                if (groupQuery.Criterias.Count == 1)
                {
                    var firstCriterias = groupQuery.Criterias[0];
                    if (firstCriterias.Item2 is Criteria)
                    {
                        return TranslateCriteria(groupQuery, firstCriterias.Item2 as Criteria, parameters, objectName);
                    }
                    return TranslateCondition(groupQuery, firstCriterias, parameters, objectName);
                }
                StringBuilder subCondition = new StringBuilder("(");
                List<string> groupWithScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;
                int index = 0;
                foreach (var subQueryItem in groupQuery.Criterias)
                {
                    var subGroupResult = TranslateCondition(groupQuery, subQueryItem, parameters, objectName);
                    if (!subGroupResult.WithScripts.IsNullOrEmpty())
                    {
                        recurveTableName = subGroupResult.RecurveObjectName;
                        recurveTablePetName = subGroupResult.RecurvePetName;
                        groupWithScripts.AddRange(subGroupResult.WithScripts);
                    }
                    subCondition.AppendFormat("{0} {1}"
                        , index > 0 ? " " + subQueryItem.Item1.ToString() : ""
                        , subGroupResult.ConditionString);
                    index++;
                }
                var groupResult = TranslateResult.CreateNewResult(subCondition.Append(")").ToString());
                groupResult.RecurveObjectName = recurveTableName;
                groupResult.RecurvePetName = recurveTablePetName;
                groupResult.WithScripts = groupWithScripts;
                return groupResult;
            }
            return TranslateResult.Empty;
        }

        /// <summary>
        /// Translate Single Criteria
        /// </summary>
        /// <param name="criteria">criteria</param>
        /// <param name="parameters">parameters</param>
        /// <returns></returns>
        TranslateResult TranslateCriteria(IQuery query, Criteria criteria, Dictionary<string, object> parameters, string objectName)
        {
            if (criteria == null)
            {
                return TranslateResult.Empty;
            }
            IQuery valueQuery = criteria.Value as IQuery;
            string parameterName = criteria.Name + ParameterSequence++;
            string sqlOperator = GetOperator(criteria.Operator);
            if (valueQuery != null)
            {
                var valueQueryObjectName = DataManager.GetQueryRelationObjectName(ServerType.Oracle, valueQuery);
                var valueQueryField = DataManager.GetField(ServerType.Oracle, valueQuery, valueQuery.QueryFields[0]);
                string subObjName = "TSB" + subObjectSequence;
                subObjectSequence++;
                var subQueryResult = ExecuteTranslate(valueQuery, parameters, subObjName, true);
                string topString = "";
                if (sqlOperator != InOperator && sqlOperator != NotInOperator)
                {
                    topString = "LIMIT 0,1";
                }
                string conditionString = subQueryResult.ConditionString;
                if (!string.IsNullOrWhiteSpace(conditionString))
                {
                    conditionString = "WHERE " + conditionString;
                }
                var valueQueryCondition = string.Format("{0} {1} (SELECT {2}.{3} FROM {4} {5}{6} {7} {8} {9})"
                    , ConvertCriteriaName(valueQuery, objectName, criteria)
                    , sqlOperator
                    , subObjName
                    , valueQueryField.FieldName
                    , valueQueryObjectName
                    , subObjName
                    , subQueryResult.JoinScript
                    , conditionString
                    , subQueryResult.OrderString
                    , topString);
                var valueQueryResult = TranslateResult.CreateNewResult(valueQueryCondition);
                if (!subQueryResult.WithScripts.IsNullOrEmpty())
                {
                    valueQueryResult.WithScripts = new List<string>(subQueryResult.WithScripts);
                    valueQueryResult.RecurveObjectName = subQueryResult.RecurveObjectName;
                    valueQueryResult.RecurvePetName = subQueryResult.RecurvePetName;
                }
                return valueQueryResult;
            }
            parameters.Add(parameterName, FormatCriteriaValue(criteria.Operator, criteria.GetCriteriaRealValue()));
            var criteriaCondition = string.Format("{0} {1} {2}{3}"
                , ConvertCriteriaName(query, objectName, criteria)
                , sqlOperator
                , parameterPrefix
                , parameterName);
            return TranslateResult.CreateNewResult(criteriaCondition);
        }

        /// <summary>
        /// get sql operator by condition operator
        /// </summary>
        /// <param name="criteriaOperator"></param>
        /// <returns></returns>
        string GetOperator(CriteriaOperator criteriaOperator)
        {
            string sqlOperator = string.Empty;
            switch (criteriaOperator)
            {
                case CriteriaOperator.Equal:
                    sqlOperator = EqualOperator;
                    break;
                case CriteriaOperator.GreaterThan:
                    sqlOperator = GreaterThanOperator;
                    break;
                case CriteriaOperator.GreaterThanOrEqual:
                    sqlOperator = GreaterThanOrEqualOperator;
                    break;
                case CriteriaOperator.NotEqual:
                    sqlOperator = NotEqualOperator;
                    break;
                case CriteriaOperator.LessThan:
                    sqlOperator = LessThanOperator;
                    break;
                case CriteriaOperator.LessThanOrEqual:
                    sqlOperator = LessThanOrEqualOperator;
                    break;
                case CriteriaOperator.In:
                    sqlOperator = InOperator;
                    break;
                case CriteriaOperator.NotIn:
                    sqlOperator = NotInOperator;
                    break;
                case CriteriaOperator.Like:
                case CriteriaOperator.BeginLike:
                case CriteriaOperator.EndLike:
                    sqlOperator = LikeOperator;
                    break;
                case CriteriaOperator.NotLike:
                case CriteriaOperator.NotBeginLike:
                case CriteriaOperator.NotEndLike:
                    sqlOperator = NotLikeOperator;
                    break;
            }
            return sqlOperator;
        }

        /// <summary>
        /// Format Value
        /// </summary>
        /// <param name="criteriaOperator">condition operator</param>
        /// <param name="value">value</param>
        /// <returns></returns>
        dynamic FormatCriteriaValue(CriteriaOperator criteriaOperator, dynamic value)
        {
            dynamic realValue = value;
            switch (criteriaOperator)
            {
                case CriteriaOperator.Like:
                case CriteriaOperator.NotLike:
                    realValue = string.Format("%{0}%", value);
                    break;
                case CriteriaOperator.BeginLike:
                case CriteriaOperator.NotBeginLike:
                    realValue = string.Format("{0}%", value);
                    break;
                case CriteriaOperator.EndLike:
                case CriteriaOperator.NotEndLike:
                    realValue = string.Format("%{0}", value);
                    break;
            }
            return realValue;
        }

        /// <summary>
        /// convert criteria
        /// </summary>
        /// <param name="objectName">object name</param>
        /// <param name="criteria">criteria</param>
        /// <returns></returns>
        string ConvertCriteriaName(IQuery query, string objectName, Criteria criteria)
        {
            return FormatCriteriaName(query, objectName, criteria.Name, criteria.Convert);
        }

        /// <summary>
        /// convert order criteria name
        /// </summary>
        /// <param name="objectName">object name</param>
        /// <param name="orderCriteria">order criteria</param>
        /// <returns></returns>
        string ConvertOrderCriteriaName(IQuery query, string objectName, OrderCriteria orderCriteria)
        {
            return FormatCriteriaName(query, objectName, orderCriteria.Name, orderCriteria.Convert);
        }

        /// <summary>
        /// format criteria name
        /// </summary>
        /// <param name="objectName">object name</param>
        /// <param name="fieldName">field name</param>
        /// <param name="convert">convert</param>
        /// <returns></returns>
        string FormatCriteriaName(IQuery query, string objectName, string fieldName, ICriteriaConvert convert)
        {
            var field = DataManager.GetField(ServerType.Oracle, query, fieldName);
            fieldName = field.FieldName;
            if (convert == null)
            {
                return string.Format("{0}.{1}", objectName, fieldName);
            }
            string convertValue = string.Empty;
            switch (convert.Type)
            {
                case CriteriaConvertType.StringLength:
                    convertValue = string.Format("LENGTH({0}.{1})", objectName, fieldName);
                    break;
                default:
                    convertValue = string.Format("{0}.{1}", objectName, fieldName);
                    break;
            }
            return convertValue;
        }

        /// <summary>
        /// get join operator
        /// </summary>
        /// <param name="joinType">join type</param>
        /// <returns></returns>
        string GetJoinOperator(JoinType joinType)
        {
            return joinOperatorDict[joinType];
        }

        /// <summary>
        /// get join condition
        /// </summary>
        /// <param name="sourceQuery">source query</param>
        /// <param name="joinItem">join item</param>
        /// <returns></returns>
        string GetJoinCondition(IQuery sourceQuery, JoinItem joinItem, string sourceObjShortName, string targetObjShortName)
        {
            if (joinItem.JoinType == JoinType.CrossJoin)
            {
                return string.Empty;
            }
            var joinFields = joinItem?.JoinFields.Where(r => !r.Key.IsNullOrEmpty() && !r.Value.IsNullOrEmpty());
            var sourceEntityType = sourceQuery.EntityType;
            var targetEntityType = joinItem.JoinQuery.EntityType;
            bool useValueAsSource = false;
            if (joinFields.IsNullOrEmpty())
            {
                joinFields = EntityManager.GetRelationFields(sourceEntityType, targetEntityType);
            }
            if (joinFields.IsNullOrEmpty())
            {
                useValueAsSource = true;
                joinFields = EntityManager.GetRelationFields(targetEntityType, sourceEntityType);
            }
            if (joinFields.IsNullOrEmpty())
            {
                return string.Empty;
            }

            List<string> joinList = new List<string>();
            foreach (var joinField in joinFields)
            {
                if (joinField.Key.IsNullOrEmpty() || joinField.Value.IsNullOrEmpty())
                {
                    continue;
                }
                var sourceField = DataManager.GetField(ServerType.Oracle, sourceEntityType, joinField.Key);
                var targetField = DataManager.GetField(ServerType.Oracle, targetEntityType, joinField.Value);
                joinList.Add(string.Format(" {0}.{1}{2}{3}.{4}",
                    sourceObjShortName,
                    useValueAsSource ? targetField.FieldName : sourceField.FieldName,
                    GetJoinOperator(joinItem.Operator),
                    targetObjShortName,
                    useValueAsSource ? sourceField.FieldName : targetField.FieldName
                    ));
            }
            return joinList.IsNullOrEmpty() ? string.Empty : " ON" + string.Join(" AND", joinList);
        }

        /// <summary>
        /// get sql operator by condition operator
        /// </summary>
        /// <param name="joinOperator"></param>
        /// <returns></returns>
        string GetJoinOperator(JoinOperator joinOperator)
        {
            string sqlOperator = string.Empty;
            switch (joinOperator)
            {
                case JoinOperator.Equal:
                    sqlOperator = EqualOperator;
                    break;
                case JoinOperator.GreaterThan:
                    sqlOperator = GreaterThanOperator;
                    break;
                case JoinOperator.GreaterThanOrEqual:
                    sqlOperator = GreaterThanOrEqualOperator;
                    break;
                case JoinOperator.NotEqual:
                    sqlOperator = NotEqualOperator;
                    break;
                case JoinOperator.LessThan:
                    sqlOperator = LessThanOperator;
                    break;
                case JoinOperator.LessThanOrEqual:
                    sqlOperator = LessThanOrEqualOperator;
                    break;
            }
            return sqlOperator;
        }

        /// <summary>
        /// format with script
        /// </summary>
        /// <returns></returns>
        string FormatWithScript(List<string> withScripts)
        {
            if (withScripts.IsNullOrEmpty())
            {
                return string.Empty;
            }
            return string.Format("WITH {0}", string.Join(",", withScripts));
        }

        /// <summary>
        /// init
        /// </summary>
        void Init()
        {
            recurveObjectSequence = subObjectSequence = 0;
        }

        #endregion
    }
}
