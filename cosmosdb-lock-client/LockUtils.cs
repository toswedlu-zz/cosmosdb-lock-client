using System;

namespace Microsoft.Azure.Cosmos
{
    internal static class LockUtils
    {
        public static DateTime Now
        {
            get { return DateTime.UtcNow; }
        }
    }
}
