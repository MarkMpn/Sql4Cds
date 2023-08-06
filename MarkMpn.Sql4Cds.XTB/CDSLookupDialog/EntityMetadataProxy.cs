using Microsoft.Xrm.Sdk.Metadata;
using System.Collections.Generic;

namespace xrmtb.XrmToolBox.Controls.Helper
{
    public class EntityMetadataProxy
    {
        #region Public Fields

        public EntityMetadata Metadata;

        #endregion Public Fields

        #region Public Constructors

        public EntityMetadataProxy(EntityMetadata entityMetadata)
        {
            Metadata = entityMetadata;
        }

        #endregion Public Constructors

        #region Public Methods

        public override string ToString()
        {
            if (Metadata != null)
            {
                if (!string.IsNullOrEmpty(Metadata?.DisplayName?.UserLocalizedLabel?.Label))
                {
                    return $"{Metadata.DisplayName.UserLocalizedLabel.Label} ({Metadata.LogicalName})";
                }
                return Metadata.LogicalName;
            }
            return base.ToString();
        }

        #endregion Public Methods
    }
}
