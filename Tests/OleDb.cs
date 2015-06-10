// <license>
// The MIT License (MIT)
// </license>
// <copyright company="TTRider, L.L.C.">
// Copyright (c) 2014-2015 All Rights Reserved
// </copyright>

using System.Data;
using System.Data.OleDb;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tests
{
    [TestClass]
    public class OleDb
    {
        private static string connectionString;

        [ClassInitialize()]
        public static void ClassInit(TestContext testcontext)
        {
            connectionString = Utilities.CreateTestOleDbDatabase();
        }

        [TestMethod]
        public async Task Reader_NoToken()
        {
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync();

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT Name FROM Person;";
                using (IDataReader reader = await command.ExecuteReaderAsync())
                {
                    do
                    {
                        while (await reader.ReadAsync())
                        {
                            if (!await reader.IsDBNullAsync(0))
                            {
                                var name = reader.GetFieldValueAsync<string>(0);
                                Assert.IsNotNull(name);
                            }
                        }
                    } while (await reader.NextResultAsync());
                }
            }
        }

        [TestMethod]
        public async Task Reader_WithToken()
        {
            var token = CancellationToken.None;
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync(token);

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT Name FROM Person;";
                using (IDataReader reader = await command.ExecuteReaderAsync(token))
                {
                    do
                    {
                        while (await reader.ReadAsync(token))
                        {
                            if (!await reader.IsDBNullAsync(0,token))
                            {
                                var name = reader.GetFieldValueAsync<string>(0,token);
                                Assert.IsNotNull(name);
                            }
                        }
                    } while (await reader.NextResultAsync(token));
                }
            }
        }


        [TestMethod]
        public async Task Reader_With_Behavior_Token()
        {
            var token = CancellationToken.None;
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync(token);

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT Name FROM Person;";
                using (IDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, token))
                {
                    do
                    {
                        while (await reader.ReadAsync(token))
                        {
                            if (!await reader.IsDBNullAsync(0, token))
                            {
                                var name = reader.GetFieldValueAsync<string>(0, token);
                                Assert.IsNotNull(name);
                            }
                        }
                    } while (await reader.NextResultAsync(token));
                }
            }
        }
        [TestMethod]
        public async Task Reader_With_Behavior_NoToken()
        {
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync();

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT Name FROM Person;";
                using (IDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection))
                {
                    do
                    {
                        while (await reader.ReadAsync())
                        {
                            if (!await reader.IsDBNullAsync(0))
                            {
                                var name = reader.GetFieldValueAsync<string>(0);
                                Assert.IsNotNull(name);
                            }
                        }
                    } while (await reader.NextResultAsync());
                }
            }
        }


        [TestMethod]
        public async Task Scalar_With_NoToken()
        {
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync();

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT TOP(1) Name FROM Person;";
                var value = await command.ExecuteScalarAsync();
                Assert.AreEqual("Adam",value);
            }
        }

        [TestMethod]
        public async Task Scalar_With_Token()
        {
            var token = CancellationToken.None;
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync(token);

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "SELECT TOP(1) Name FROM Person;";
                var value = await command.ExecuteScalarAsync(token);
                Assert.AreEqual("Adam", value);
            }
        }


        [TestMethod]
        public async Task NoQuery_With_NoToken()
        {
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync();

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "DECLARE @Name NVARCHAR(MAX) = 'Adam';";
                var value = await command.ExecuteNonQueryAsync();
                Assert.AreEqual(-1, value);
            }
        }

        [TestMethod]
        public async Task NoQuery_With_Token()
        {
            var token = CancellationToken.None;
            using (IDbConnection connection = new OleDbConnection(connectionString))
            {
                await connection.OpenAsync(token);

                IDbCommand command = connection.CreateCommand();
                command.CommandText = "DECLARE @Name NVARCHAR(MAX) = 'Adam';";
                var value = await command.ExecuteNonQueryAsync(token);
                Assert.AreEqual(-1, value);
            }
        }

    
    
    }
}
