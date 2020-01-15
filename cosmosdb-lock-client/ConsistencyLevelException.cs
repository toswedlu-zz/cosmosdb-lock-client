using System;

namespace Microsoft.Azure.Cosmos
{
    public class ConsistencyLevelException : Exception
    {
        static string _message = "A consistency level of \"{0}\" is not supported.  Use consistency level Strong.";

        public ConsistencyLevelException(ConsistencyLevel level, Exception innerEx = null)
            : base(string.Format(_message, level), innerEx)
        {
        }
    }
}