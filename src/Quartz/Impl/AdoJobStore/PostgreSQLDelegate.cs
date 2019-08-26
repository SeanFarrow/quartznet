#region License

/*
 * Copyright 2009- Marko Lahma
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

#endregion

using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Quartz.Impl.AdoJobStore
{
    /// <summary>
    /// This is a driver delegate for the PostgreSQL ADO.NET driver.
    /// </summary>
    /// <author>Marko Lahma</author>
    public class PostgreSQLDelegate : StdAdoDelegate
    {
/// <summary>
/// The default permitted number of retries.
/// </summary>
        private readonly int DefaultPermittedRetryCount = Int32.MaxValue;
        /// <summary>
        /// The number of milliseconds between retries.
        /// </summary>
        private readonly TimeSpan DefaultTimeBetweenExecutions = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// Gets the select next trigger to acquire SQL clause.
        /// MySQL version with LIMIT support.
        /// </summary>
        /// <returns></returns>
        protected override string GetSelectNextTriggerToAcquireSql(int maxCount)
        {
            return SqlSelectNextTriggerToAcquire + " LIMIT " + maxCount;
        }

        protected override string GetSelectNextMisfiredTriggersInStateToAcquireSql(int count)
        {
            if (count != -1)
            {
                return SqlSelectHasMisfiredTriggersInState + " LIMIT " + count;
            }
            return base.GetSelectNextMisfiredTriggersInStateToAcquireSql(count);
        }
        
        #region Functions that read/write data.
        /// <summary>
        /// Read a single record from the passed in <see cref="DbDataReader"/>.
        /// </summary>
        /// <param name="rs">The <see cref="DbDataReader"/> from which to read.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<bool> ReadSingleRecordFromDataReaderAsync(DbDataReader rs, CancellationToken cancellationToken = default)
        {
            return await RetryFunction(() => base.ReadSingleRecordFromDataReaderAsync(rs, cancellationToken), DefaultPermittedRetryCount, DefaultTimeBetweenExecutions);
        }

        /// <summary>
        /// Executesthe daprovided <see cref="DbCommand"/> and returns a <see cref="DbDataReader"/>.
        /// </summary>
        /// <param name="cmd">The database command to execute.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<DbDataReader> ExecuteReaderAsync(DbCommand cmd, CancellationToken cancellationToken = default)
        {
            return await RetryFunction(() => base.ExecuteReaderAsync(cmd, cancellationToken), DefaultPermittedRetryCount, DefaultTimeBetweenExecutions);
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query. All other columns and rows are ignored.
        /// </summary>
        /// <param name="cmd">The database command to execute.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<object> ExecuteScalarAsync(DbCommand cmd, CancellationToken cancellationToken = default)
        {
            return await RetryFunction(() => base.ExecuteScalarAsync(cmd, cancellationToken), DefaultPermittedRetryCount, DefaultTimeBetweenExecutions);
        }

        /// <summary>
        /// When overridden in a derived class, executes a SQL statement against a connection object asynchronously.
        /// </summary>
        /// <param name="cmd">The database command to execute.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<int> ExecuteNonQueryAsync(DbCommand cmd, CancellationToken cancellationToken)
        {
            return await RetryFunction(() => base.ExecuteNonQueryAsync(cmd, cancellationToken), DefaultPermittedRetryCount, DefaultTimeBetweenExecutions);
        }
        #endregion

        private async Task<TResult> RetryFunction<TResult>(Func<Task<TResult>> functionToRetry, int permittedRetryCount, TimeSpan timeBetweenExecutions)
        {
            int tryCount = default;
            bool canRetry = true;
            TResult result = default;

            do
            {
                try
                {
                    result = await functionToRetry().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    canRetry = IsTransientException(e);
                    if (!canRetry)
                    {
                        throw;
                    }
                }

                if (tryCount < int.MaxValue)
                {
                    tryCount++;
                }
                canRetry = tryCount < permittedRetryCount;

                await Task.Delay(timeBetweenExecutions).ConfigureAwait(false);
            } while (canRetry);
            return result;
            }

        private bool IsTransientException(Exception e)
        {
            return true;
        }
    }
}