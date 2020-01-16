using System;

namespace Microsoft.Azure.Cosmos
{
    internal static class LockUtils
    {
        /**
         * A utility method to standardize which 'now' to use when creating
         * timestamps.  
         */
        public static DateTime Now
        {
            get { return DateTime.UtcNow; }
        }
    }
}
