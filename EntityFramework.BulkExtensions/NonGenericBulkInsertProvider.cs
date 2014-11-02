using System;
using System.Collections;
using System.Data.SqlClient;
using System.Data.Entity;

namespace EntityFramework.BulkExtensions
{
    class NonGenericBulkInsertProvider 
    {
        protected SqlConnection CreateConnection(DbContext context)
        {
            var connectionString = (string)context.Database.Connection.GetPrivateFieldValue("_connectionString");
            return new SqlConnection(connectionString);
        }

        public virtual void BulkInsert(DbContext context, Type entityTpe, IEnumerable entities)
        {
            using (var dbConnection = CreateConnection(context))
            {
                dbConnection.Open();

                using (var transaction = dbConnection.BeginTransaction())
                {
                    try
                    {
                        Run(context, entityTpe, entities, transaction, BulkInsertOptions.Defaults);
                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        if (transaction.Connection != null)
                        {
                            transaction.Rollback();
                        }
                        throw;
                    }
                }
            }
        }

        public void Run(DbContext context, Type entityType, IEnumerable entities, SqlTransaction transaction, BulkInsertOptions options)
        {
            var keepIdentity = (SqlBulkCopyOptions.KeepIdentity & options.SqlBulkCopyOptions) > 0;
            using (var reader = new NonGenericMappedDataReader(entityType, entities, context))
            {
                using (var sqlBulkCopy = new SqlBulkCopy(transaction.Connection, options.SqlBulkCopyOptions, transaction))
                {
                    sqlBulkCopy.BulkCopyTimeout = options.TimeOut;
                    sqlBulkCopy.BatchSize = options.BatchSize;
                    sqlBulkCopy.DestinationTableName = string.Format("[{0}].[{1}]", reader.SchemaName, reader.TableName);
#if !NET40
                    sqlBulkCopy.EnableStreaming = options.EnableStreaming;
#endif

                    sqlBulkCopy.NotifyAfter = options.NotifyAfter;
                    if (options.Callback != null)
                    {
                        sqlBulkCopy.SqlRowsCopied += options.Callback;
                    }

                    foreach (var kvp in reader.Cols)
                    {
                        if (kvp.Value.IsIdentity && !keepIdentity)
                        {
                            continue;
                        }
                        sqlBulkCopy.ColumnMappings.Add(kvp.Value.ColumnName, kvp.Value.ColumnName);
                    }

                    sqlBulkCopy.WriteToServer(reader);
                }
            }
        }
    }

    class BulkInsertOptions
    {
        public static BulkInsertOptions Defaults
        {
            get
            {
                return new BulkInsertOptions
                {
                    BatchSize = 5000,
                    SqlBulkCopyOptions = SqlBulkCopyOptions.Default,
                    TimeOut = 30,
                    NotifyAfter = 1000
                };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; }

        /// <summary>
        /// Number of the seconds for the operation to complete before it times out
        /// </summary>
        public int TimeOut { get; set; }

        /// <summary>
        /// Callback event handler. Event is fired after n (value from NotifyAfter) rows have been copied to table where.
        /// </summary>
        public SqlRowsCopiedEventHandler Callback { get; set; }

        /// <summary>
        /// Used with property Callback. Sets number of rows after callback is fired.
        /// </summary>
        public int NotifyAfter { get; set; }

#if !NET40
        /// <summary>
        /// 
        /// </summary>
        public bool EnableStreaming { get; set; }
#endif
    }

}