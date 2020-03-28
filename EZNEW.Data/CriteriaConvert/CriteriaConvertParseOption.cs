using EZNEW.Develop.CQuery.CriteriaConvert;
using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.CriteriaConvert
{
    /// <summary>
    /// criteria convert parse option
    /// </summary>
    public class CriteriaConvertParseOption
    {
        /// <summary>
        /// criteria convert
        /// </summary>
        public ICriteriaConvert CriteriaConvert { get; set; }

        /// <summary>
        /// db server type
        /// </summary>
        public ServerType ServerType { get; set; }

        /// <summary>
        /// object name
        /// </summary>
        public string ObjectName { get; set; }

        /// <summary>
        /// field name
        /// </summary>
        public string FieldName { get; set; }
    }
}
