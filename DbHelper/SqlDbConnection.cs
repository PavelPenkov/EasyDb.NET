using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DbHelper {
	public static class Extensions {
		public static void Each<T>(this IEnumerable<T> source, Action<T> action) {
			Each(source, (i, v) => action(v));
		}
		public static void Each<T>(this IEnumerable<T> source, Action<int, T> action) {
			var e = source.GetEnumerator();
			for (int i = 0; e.MoveNext(); i++) {
				action(i, e.Current);
			}
		}

	}

	public interface IDbQuery {
		IEnumerable<IDictionary<string, object>> Select(string sql);
		IEnumerable<T> Select<T>(string sql, Func<IDictionary<string, object>, T> objectGetter);
		IEnumerable<dynamic> SelectExpando(string sql);
		void Insert(string table, object values);
		bool Exists(string sql);
	}

	public interface IDbConnection {
		void Query(Action<IDbQuery> action);
	}

	public class SqlConnectionFactory {
		public static IDbConnection Create(string connString) {
			return new SqlDbConnection(connString);
		}

		private class SqlDbConnection : IDbConnection {
			private readonly string _connString;

			public SqlDbConnection(string connString) {
				_connString = connString;
			}

			public void Query(Action<IDbQuery> action) {
				using (var conn = new SqlConnection(_connString)) {
					conn.Open();
					var q = new SqlDbQuery(conn);
					action(q);
				}
			}
		}

		private class SqlDbQuery : IDbQuery {
			private readonly SqlConnection _conn;

			public SqlDbQuery(SqlConnection conn) {
				_conn = conn;
			}

			private T ExecuteReader<T>(string sql, Func<SqlDataReader,T> func) {
				using (var cmd = new SqlCommand(sql, _conn)) {
					using (var rdr = cmd.ExecuteReader()) {
						return func(rdr);
					}
				}
			}
			private void ExecuteReader(string sql, Action<SqlDataReader> action) {
				using (var cmd = new SqlCommand(sql, _conn)) {
					using (var rdr = cmd.ExecuteReader()) {
						action(rdr);
					}
				}
			}

			public IEnumerable<IDictionary<string, object>> Select(string sql) {
				return ExecuteReader(sql, rdr => {
				                   	var fields = new string[rdr.FieldCount];
				                   	for (int i = 0; i < fields.Length; i++) {
				                   		fields[i] = rdr.GetName(i);
				                   	}
				                   	var result = new List<IDictionary<string, object>>();
				                   	while (rdr.Read()) {
				                   		var row = new Dictionary<string, object>();
				                   		fields.Each((i, v) => row[v] = rdr.GetValue(i));
				                   		result.Add(row);
				                   	}
				                   	return result;
				                   });
			}

			public IEnumerable<T> Select<T>(string sql, Func<IDictionary<string, object>, T> objectGetter) {
				return Select(sql).Select(objectGetter);
			}

			public IEnumerable<dynamic> SelectExpando(string sql) {
				return Select(sql).Select(row => {
				                          	var e = new ExpandoObject();
				                          	row.Each(field => ((IDictionary<string, object>) e)[field.Key] = field.Value);
				                          	return e;
				                          });

                }

			public void Insert(string table, object values) {

				using (var cmd = new SqlCommand()) {
					cmd.Connection = _conn;
					cmd.Parameters.AddRange(values.GetType().GetFields().Select(fi => new SqlParameter(fi.Name, fi.GetValue(values))).ToArray());
					string text = "insert into " + table;
					text += String.Join(",", values.GetType().GetFields(BindingFlags.Public).Select(fi => fi.Name));
				}
			}

			public bool Exists(string sql) {
				return ExecuteReader(sql, rdr => rdr.HasRows );
			}
		}
	}
}
