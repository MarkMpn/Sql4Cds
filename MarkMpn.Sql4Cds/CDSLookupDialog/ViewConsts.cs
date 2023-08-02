// *********************************************************************
// Created by : Latebound Constants Generator 1.2020.2.1 for XrmToolBox
// Author     : Jonas Rapp https://twitter.com/rappen
// GitHub     : https://github.com/rappen/LCG-UDG
// Source Org : https://jonassandbox.crm4.dynamics.com/
// Filename   : C:\Dev\GitHub\xrmtb.XrmToolBox.Controls\XrmToolBox.Controls\Helper\ViewConsts.cs
// Created    : 2020-05-17 09:53:15
// *********************************************************************

namespace xrmtb.XrmToolBox.Controls.Helper
{
    /// <summary>DisplayName: Saved View, OwnershipType: UserOwned, IntroducedVersion: 5.0.0.0</summary>
    public static class UserQuery
    {
        public const string EntityName = "userquery";
        public const string EntityCollectionName = "userqueries";

        #region Attributes

        /// <summary>Type: Uniqueidentifier, RequiredLevel: SystemRequired</summary>
        public const string PrimaryKey = "userqueryid";
        /// <summary>Type: String, RequiredLevel: SystemRequired, MaxLength: 200, Format: Text</summary>
        public const string PrimaryName = "name";
        /// <summary>Type: Memo, RequiredLevel: None, MaxLength: 1073741823</summary>
        public const string Columnsetxml = "columnsetxml";
        /// <summary>Type: Memo, RequiredLevel: None, MaxLength: 2000</summary>
        public const string Description = "description";
        /// <summary>Type: Memo, RequiredLevel: SystemRequired, MaxLength: 1073741823</summary>
        public const string Fetchxml = "fetchxml";
        /// <summary>Type: Memo, RequiredLevel: None, MaxLength: 1073741823</summary>
        public const string Layoutxml = "layoutxml";
        /// <summary>Type: Integer, RequiredLevel: SystemRequired, MinValue: 0, MaxValue: 1000000000</summary>
        public const string QueryType = "querytype";
        /// <summary>Type: EntityName, RequiredLevel: SystemRequired</summary>
        public const string ReturnedTypeCode = "returnedtypecode";
        /// <summary>Type: State, RequiredLevel: SystemRequired, DisplayName: Status, OptionSetType: State</summary>
        public const string StateCode = "statecode";
        /// <summary>Type: Status, RequiredLevel: None, DisplayName: Status Reason, OptionSetType: Status</summary>
        public const string StatusCode = "statuscode";

        #endregion Attributes

        #region OptionSets

        public enum ReturnedTypeCode_OptionSet
        {
        }
        public enum StateCode_OptionSet
        {
            Active = 0,
            Inactive = 1
        }
        public enum StatusCode_OptionSet
        {
            Active = 1,
            All = 3,
            Inactive = 2
        }

        #endregion OptionSets
    }

    /// <summary>DisplayName: View, OwnershipType: OrganizationOwned, IntroducedVersion: 5.0.0.0</summary>
    public static class Savedquery
    {
        public const string EntityName = "savedquery";
        public const string EntityCollectionName = "savedqueries";

        #region Attributes

        /// <summary>Type: Uniqueidentifier, RequiredLevel: SystemRequired</summary>
        public const string PrimaryKey = "savedqueryid";
        /// <summary>Type: String, RequiredLevel: SystemRequired, MaxLength: 200, Format: Text</summary>
        public const string PrimaryName = "name";
        /// <summary>Type: Memo, RequiredLevel: None, MaxLength: 1073741823</summary>
        public const string Columnsetxml = "columnsetxml";
        /// <summary>Type: Boolean, RequiredLevel: SystemRequired, True: 1, False: 0, DefaultValue: False</summary>
        public const string Isdefault = "isdefault";
        /// <summary>Type: Memo, RequiredLevel: None, MaxLength: 2000</summary>
        public const string Description = "description";
        /// <summary>Type: EntityName, RequiredLevel: SystemRequired</summary>
        public const string ReturnedTypeCode = "returnedtypecode";
        /// <summary>Type: Memo, RequiredLevel: None, MaxLength: 1073741823</summary>
        public const string Fetchxml = "fetchxml";
        /// <summary>Type: Memo, RequiredLevel: None, MaxLength: 1073741823</summary>
        public const string Layoutxml = "layoutxml";
        /// <summary>Type: Integer, RequiredLevel: SystemRequired, MinValue: 0, MaxValue: 1000000000</summary>
        public const string QueryType = "querytype";
        /// <summary>Type: Boolean, RequiredLevel: SystemRequired, True: 1, False: 0, DefaultValue: False</summary>
        public const string Isquickfindquery = "isquickfindquery";
        /// <summary>Type: Boolean, RequiredLevel: SystemRequired, True: 1, False: 0, DefaultValue: False</summary>
        public const string Ismanaged = "ismanaged";
        /// <summary>Type: State, RequiredLevel: SystemRequired, DisplayName: Status, OptionSetType: State</summary>
        public const string StateCode = "statecode";
        /// <summary>Type: Status, RequiredLevel: None, DisplayName: Status Reason, OptionSetType: Status</summary>
        public const string StatusCode = "statuscode";

        #endregion Attributes

        #region OptionSets

        public enum ReturnedTypeCode_OptionSet
        {
        }
        public enum StateCode_OptionSet
        {
            Active = 0,
            Inactive = 1
        }
        public enum StatusCode_OptionSet
        {
            Active = 1,
            Inactive = 2
        }

        #endregion OptionSets
    }
}
