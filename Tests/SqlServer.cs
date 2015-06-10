using System;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Tests
{
    [TestClass]
    public class SqlServer
    {
        private static string connectionString;

        [ClassInitialize()]
        public static void ClassInit(TestContext testcontext)
        {
            connectionString = Utilities.CreateTestDatabase();
        }

        [TestMethod]
        public async Task EndToEnd_NoToken()
        {
            using (IDbConnection connection = new SqlConnection(connectionString))
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
        public async Task EndToEnd_WithToken()
        {
            var token = CancellationToken.None;
            using (IDbConnection connection = new SqlConnection(connectionString))
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
        public async Task EndToEnd_WithBehavior_Token()
        {
            var token = CancellationToken.None;
            using (IDbConnection connection = new SqlConnection(connectionString))
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
        public async Task EndToEnd_WithBehavior_NoToken()
        {
            using (IDbConnection connection = new SqlConnection(connectionString))
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
    }
}
