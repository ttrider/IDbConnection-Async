using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
// ReSharper disable InconsistentNaming

namespace System.Data
{
    public static class AsyncAdapter
    {
        private static readonly ConcurrentDictionary<Type, ConnectionAdapter> ConnectionAdapters =
            new ConcurrentDictionary<Type, ConnectionAdapter>();
        private static readonly ConcurrentDictionary<Type, CommandAdapter> CommandAdapters =
            new ConcurrentDictionary<Type, CommandAdapter>();
        private static readonly ConcurrentDictionary<Type, DataReaderAdapter> DataReaderAdapters =
            new ConcurrentDictionary<Type, DataReaderAdapter>();

        private class ConnectionAdapter
        {
            private readonly MethodInfo open;
            private readonly MethodInfo openToken;

            internal ConnectionAdapter(Type type)
            {
                this.open = type.GetMethod("OpenAsync", new Type[0]);
                this.openToken = type.GetMethod("OpenAsync", new[] { typeof(CancellationToken) });
            }

            internal Task DoOpenAsync(IDbConnection connection)
            {
                if (this.open != null)
                {
                    return (Task)this.open.Invoke(connection, new object[0]);
                }
                connection.Open();
                return Task.FromResult(0);
            }

            internal Task DoOpenAsync(IDbConnection connection, CancellationToken token)
            {
                if (this.openToken != null)
                {
                    return (Task)this.openToken.Invoke(connection, new object[] { token });
                }
                connection.Open();
                return Task.FromResult(0);
            }
        }
        private class CommandAdapter
        {
            private readonly MethodInfo executeNonQueryAsync;
            private readonly MethodInfo executeNonQueryAsyncT;
            private readonly MethodInfo executeReaderAsync;
            private readonly MethodInfo executeReaderAsyncT;
            private readonly MethodInfo executeReaderAsyncB;
            private readonly MethodInfo executeReaderAsyncBt;
            private readonly MethodInfo executeScalarAsync;
            private readonly MethodInfo executeScalarAsyncT;
            internal CommandAdapter(Type type)
            {
                this.executeNonQueryAsync = type.GetMethod("ExecuteNonQueryAsync", new Type[0]);
                this.executeNonQueryAsyncT= type.GetMethod("ExecuteNonQueryAsync", new[] {typeof (CancellationToken)});
                this.executeReaderAsync = type.GetMethod("ExecuteReaderAsync", new Type[0]);
                this.executeReaderAsyncT  = type.GetMethod("ExecuteReaderAsync", new[] {typeof (CancellationToken)});
                this.executeReaderAsyncB  = type.GetMethod("ExecuteReaderAsync", new[] {typeof(CommandBehavior)});
                this.executeReaderAsyncBt = type.GetMethod("ExecuteReaderAsync", new[] {typeof(CommandBehavior), typeof (CancellationToken)});
                this.executeScalarAsync = type.GetMethod("ExecuteScalarAsync", new Type[0]);
                this.executeScalarAsyncT  = type.GetMethod("ExecuteScalarAsync", new[] {typeof (CancellationToken)});
            }


            internal Task<int> DoExecuteNonQueryAsync(IDbCommand command)
            {
                if (this.executeNonQueryAsync != null)
                {
                    return (Task<int>)this.executeNonQueryAsync.Invoke(command, new object[0]);
                }
                return Task.FromResult(command.ExecuteNonQuery());
            }
            internal Task<int> DoExecuteNonQueryAsync(IDbCommand command, CancellationToken token)
            {
                if (this.executeNonQueryAsyncT != null)
                {
                    return (Task<int>)this.executeNonQueryAsyncT.Invoke(command, new object[] { token });
                }
                return Task.FromResult(command.ExecuteNonQuery());
            }
            internal Task<IDataReader> DoExecuteReaderAsync(IDbCommand command)
            {
                if (this.executeNonQueryAsync != null)
                {
                    return (Task<IDataReader>)this.executeReaderAsync.Invoke(command, new object[0]);
                }
                return  Task.FromResult(command.ExecuteReader());
            }
            internal Task<IDataReader> DoExecuteReaderAsync(IDbCommand command, CancellationToken token)
            {
                if (this.executeNonQueryAsyncT != null)
                {
                    return (Task<IDataReader>)this.executeReaderAsyncT.Invoke(command, new object[] { token });
                }
                return Task.FromResult(command.ExecuteReader());
            }
            internal Task<IDataReader> DoExecuteReaderAsync(IDbCommand command, CommandBehavior commandBehavior)
            {
                if (this.executeReaderAsyncB != null)
                {
                    return (Task<IDataReader>)this.executeReaderAsyncB.Invoke(command, new object[] { commandBehavior });
                }
                return Task.FromResult(command.ExecuteReader(commandBehavior));
            }
            internal Task<IDataReader> DoExecuteReaderAsync(IDbCommand command, CommandBehavior commandBehavior, CancellationToken token)
            {
                if (this.executeReaderAsyncBt != null)
                {
                    return (Task<IDataReader>)this.executeReaderAsyncBt.Invoke(command, new object[] { commandBehavior, token });
                }
                return Task.FromResult(command.ExecuteReader(commandBehavior));
            }
            internal Task<Object> DoExecuteScalarAsync(IDbCommand command)
            {
                if (this.executeScalarAsync != null)
                {
                    return (Task<Object>)this.executeScalarAsync.Invoke(command, new object[0]);
                }
                return Task.FromResult(command.ExecuteScalar());
            }
            internal Task<Object> DoExecuteScalarAsync(IDbCommand command, CancellationToken token)
            {
                if (this.executeScalarAsyncT != null)
                {
                    return (Task<Object>)this.executeScalarAsyncT.Invoke(command, new object[] { token });
                }
                return Task.FromResult(command.ExecuteScalar());
            }
        }
        private class DataReaderAdapter
        {
            private readonly MethodInfo getFieldValueAsync;
            private readonly MethodInfo getFieldValueAsyncT;
            private readonly MethodInfo isDbNullAsync;
            private readonly MethodInfo isDbNullAsyncT;
            private readonly MethodInfo nextResultAsync;
            private readonly MethodInfo nextResultAsyncT;
            private readonly MethodInfo readAsync;
            private readonly MethodInfo readAsyncT;
            internal DataReaderAdapter(Type type)
            {
                this.getFieldValueAsync = type.GetMethod("GetFieldValueAsync", new Type[0]);
                this.getFieldValueAsyncT= type.GetMethod("GetFieldValueAsync", new[] {typeof (CancellationToken)});
                this.isDbNullAsync = type.GetMethod("IsDBNullAsync", new Type[0]);
                this.isDbNullAsyncT= type.GetMethod("IsDBNullAsync", new[] {typeof (CancellationToken)});
                this.nextResultAsync = type.GetMethod("NextResultAsync", new Type[0]);
                this.nextResultAsyncT= type.GetMethod("NextResultAsync", new[] {typeof (CancellationToken)});
                this.readAsync = type.GetMethod("ReadAsync", new Type[0]);
                this.readAsyncT= type.GetMethod("ReadAsync", new[] {typeof (CancellationToken)});
            }


            internal Task<T> DoGetFieldValueAsync<T>(IDataReader reader, int ordinal)
            {
                if (this.getFieldValueAsync != null)
                {
                    return (Task<T>)this.getFieldValueAsync.Invoke(reader, new object[]{ordinal});
                }
                return Task.FromResult((T)reader.GetValue(ordinal));
            }
            internal Task<T> DoGetFieldValueAsync<T>(IDataReader reader, int ordinal, CancellationToken token)
            {
                if (this.getFieldValueAsyncT != null)
                {
                    return (Task<T>)this.getFieldValueAsyncT.Invoke(reader, new object[]{ordinal, token});
                }
                return Task.FromResult((T)reader.GetValue(ordinal));
            }
            internal Task<bool> DoIsDBNullAsync(IDataReader reader, int ordinal)
            {
                if (this.isDbNullAsync != null)
                {
                    return (Task<bool>)this.isDbNullAsync.Invoke(reader, new object[]{ordinal});
                }
                return Task.FromResult(reader.IsDBNull(ordinal));
            }
            internal Task<bool> DoIsDBNullAsync(IDataReader reader, int ordinal, CancellationToken token)
            {
                if (this.isDbNullAsyncT != null)
                {
                    return (Task<bool>)this.isDbNullAsyncT.Invoke(reader, new object[]{ordinal, token});
                }
                return Task.FromResult(reader.IsDBNull(ordinal));
            }
            internal Task<bool> DoNextResultAsync(IDataReader reader)
            {
                if (this.nextResultAsync != null)
                {
                    return (Task<bool>)this.nextResultAsync.Invoke(reader, new object[0]);
                }
                return Task.FromResult(reader.NextResult());
            }
            internal Task<bool> DoNextResultAsync(IDataReader reader, CancellationToken token)
            {
                if (this.nextResultAsyncT != null)
                {
                    return (Task<bool>)this.nextResultAsyncT.Invoke(reader, new object[]{token});
                }
                return Task.FromResult(reader.NextResult());
            }
            internal Task<bool> DoReadAsync(IDataReader reader)
            {
                if (this.readAsync != null)
                {
                    return (Task<bool>)this.readAsync.Invoke(reader, new object[0]);
                }
                return Task.FromResult(reader.Read());
            }
            internal Task<bool> DoReadAsync(IDataReader reader, CancellationToken token)
            {
                if (this.readAsyncT != null)
                {
                    return (Task<bool>)this.readAsyncT.Invoke(reader, new object[]{token});
                }
                return Task.FromResult(reader.Read());
            }
        }



        public static Task OpenAsync(this IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            return ConnectionAdapters.GetOrAdd(connection.GetType(),
                type => new ConnectionAdapter(type))
                    .DoOpenAsync(connection);
        }

        public static Task OpenAsync(this IDbConnection connection, CancellationToken token)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            return ConnectionAdapters.GetOrAdd(connection.GetType(),
                type => new ConnectionAdapter(type))
                    .DoOpenAsync(connection, token);
        }


        public static Task<int> ExecuteNonQueryAsync(this IDbCommand command)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteNonQueryAsync(command);
        }

        public static Task<int> ExecuteNonQueryAsync(this IDbCommand command, CancellationToken token)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteNonQueryAsync(command, token);
        }

        public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteReaderAsync(command);
        }

        public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command, CancellationToken token)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteReaderAsync(command, token);
        }

        public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command, CommandBehavior commandBehavior)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteReaderAsync(command, commandBehavior);
        }

        public static Task<IDataReader> ExecuteReaderAsync(this IDbCommand command, CommandBehavior commandBehavior,
            CancellationToken token)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteReaderAsync(command, commandBehavior, token);
        }

        public static Task<Object> ExecuteScalarAsync(this IDbCommand command)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteScalarAsync(command);
        }

        public static Task<Object> ExecuteScalarAsync(this IDbCommand command, CancellationToken token)
        {
            if (command == null) throw new ArgumentNullException("command");
            return CommandAdapters.GetOrAdd(command.GetType(),
                type => new CommandAdapter(type))
                    .DoExecuteScalarAsync(command, token);
        }



        public static Task<T> GetFieldValueAsync<T>(this IDataReader reader, int ordinal)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoGetFieldValueAsync<T>(reader, ordinal);
        }
        public static Task<T> GetFieldValueAsync<T>(this IDataReader reader, int ordinal, CancellationToken token)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoGetFieldValueAsync<T>(reader, ordinal, token);
        }
        public static Task<bool> IsDBNullAsync(this IDataReader reader, int ordinal)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoIsDBNullAsync(reader, ordinal);
        }
        public static Task<bool> IsDBNullAsync(this IDataReader reader, int ordinal, CancellationToken token)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoIsDBNullAsync(reader, ordinal, token);
        }
        public static Task<bool> NextResultAsync(this IDataReader reader)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoNextResultAsync(reader);
        }
        public static Task<bool> NextResultAsync(this IDataReader reader, CancellationToken token)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoNextResultAsync(reader, token);
        }
        public static Task<bool> ReadAsync(this IDataReader reader)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoReadAsync(reader);
        }
        public static Task<bool> ReadAsync(this IDataReader reader, CancellationToken token)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            return DataReaderAdapters.GetOrAdd(reader.GetType(),
                type => new DataReaderAdapter(type))
                    .DoReadAsync(reader, token);
        }
    }
}
