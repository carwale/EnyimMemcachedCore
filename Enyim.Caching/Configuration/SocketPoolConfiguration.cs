using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Enyim.Caching.Memcached;

namespace Enyim.Caching.Configuration
{
    public class SocketPoolConfiguration : ISocketPoolConfiguration
    {
        private int minPoolSize = 5;
        private int maxPoolSize = 100;
        private TimeSpan connectionTimeout = new(0, 0, 10);
        private TimeSpan connectionIdleTimeout = new(0, 0, 0);
        private TimeSpan receiveTimeout = new(0, 0, 10);
        private TimeSpan deadTimeout = new(0, 0, 10);
        private TimeSpan queueTimeout = new(0, 0, 0, 0, 100);
        private TimeSpan _initPoolTimeout = new(0, 1, 0);
        private INodeFailurePolicyFactory FailurePolicyFactory = new ThrottlingFailurePolicyFactory(5, TimeSpan.FromMilliseconds(2000));

        int ISocketPoolConfiguration.MinPoolSize
        {
            get { return this.minPoolSize; }
            set { this.minPoolSize = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating the maximum amount of sockets per server in the socket pool.
        /// </summary>
        /// <returns>The maximum amount of sockets per server in the socket pool. The default is 20.</returns>
        /// <remarks>It should be 0.75 * (number of threads) for optimal performance.</remarks>
        int ISocketPoolConfiguration.MaxPoolSize
        {
            get { return this.maxPoolSize; }
            set { this.maxPoolSize = value; }
        }

        TimeSpan ISocketPoolConfiguration.ConnectionTimeout
        {
            get { return this.connectionTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                this.connectionTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.ConnectionIdleTimeout
        {
            get { return this.connectionIdleTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");
                this.connectionIdleTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.ReceiveTimeout
        {
            get { return this.receiveTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                this.receiveTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.QueueTimeout
        {
            get { return this.queueTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                this.queueTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.InitPoolTimeout
        {
            get { return _initPoolTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                _initPoolTimeout = value;
            }
        }

        TimeSpan ISocketPoolConfiguration.DeadTimeout
        {
            get { return this.deadTimeout; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException("value", "value must be positive");

                this.deadTimeout = value;
            }
        }

        INodeFailurePolicyFactory ISocketPoolConfiguration.FailurePolicyFactory
        {
            get { return this.FailurePolicyFactory; }
            set
            {
                this.FailurePolicyFactory = value ?? throw new ArgumentNullException("value");
            }
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk? enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
