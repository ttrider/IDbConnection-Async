# IDbConnection-Async
shim for IDbConnection/IDbCommand/IDataReader to unlock Async methods

Install it from Nuget:
[https://www.nuget.org/packages/IDbConnection-Async/](https://www.nuget.org/packages/IDbConnection-Async/)

Just reference this assembly, and you can call Async methods on IDbConnection/IDbCommand and IDataReader:

```
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
```


