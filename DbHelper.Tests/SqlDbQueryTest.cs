using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DbHelper.Tests
{
	/// <summary>
	/// Summary description for UnitTest1
	/// </summary>
	[TestClass]
	public class SqlDbQueryTest
	{
		public static class QueryHelper {
			public static IEnumerable<IDictionary<string,object>> ExecuteDic(string sql) {
				var c = SqlConnectionFactory.Create(ConnStr);
				IEnumerable<IDictionary<string,object>> result = null;
				c.Query(q => {
				        	result = q.Select(sql);
				        });
				return result;
			}
			public static IEnumerable<dynamic> Execute(string sql) {
				var c = SqlConnectionFactory.Create(ConnStr);
				IEnumerable<dynamic> result = null;
				c.Query(q => {
				        	result = q.SelectExpando(sql);
				        });
				return result;
			}
		}

		public SqlDbQueryTest()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		private const string ConnStr = @"Data Source=localhost\sqlexpress;Initial Catalog=SandBox;Integrated Security=True";
		private static IDbConnection GetConnection() {
			return SqlConnectionFactory.Create(ConnStr);
		}
		private TestContext testContextInstance;

		/// <summary>
		///Gets or sets the test context which provides
		///information about and functionality for the current test run.
		///</summary>
		public TestContext TestContext
		{
			get
			{
				return testContextInstance;
			}
			set
			{
				testContextInstance = value;
			}
		}

		#region Additional test attributes
		//
		// You can use the following additional attributes as you write your tests:
		//
		// Use ClassInitialize to run code before running the first test in the class
		// [ClassInitialize()]
		// public static void MyClassInitialize(TestContext testContext) { }
		//
		// Use ClassCleanup to run code after all tests in a class have run
		// [ClassCleanup()]
		// public static void MyClassCleanup() { }
		//
		// Use TestInitialize to run code before running each test 
		// [TestInitialize()]
		// public void MyTestInitialize() { }
		//
		// Use TestCleanup to run code after each test has run
		// [TestCleanup()]
		// public void MyTestCleanup() { }
		//
		#endregion

		[TestMethod]
		public void Returns_Results() {
			var c = SqlConnectionFactory.Create(ConnStr);
			
			IEnumerable<IDictionary<string, object>> result = null;

			c.Query(q => {
			        	result = q.Select("select Name from [Full]");
			        });

			Assert.IsNotNull(result);
			Assert.IsTrue(result.Any());
		}

		[TestMethod]
		public void SelectExpando_Returns_Results() {
			var c = SqlConnectionFactory.Create(ConnStr);

			IEnumerable<dynamic> result = null;

			c.Query(q => {
			        	result = q.SelectExpando("select Name from [Full]");
			        });

			Assert.IsNotNull(result);
			Assert.IsTrue(result.Any());

			dynamic row = result.FirstOrDefault();

			Assert.IsNotNull(row);
			Assert.IsNotNull(row.Name);

			Console.WriteLine(row.Name);
		}

		[TestMethod]
		public void Expando_Handles_Unnamed_Columns() {
			var result = QueryHelper.Execute("select count(*) from [Full]");

			Assert.IsNotNull(result);
			Assert.IsNotNull(result.First());
		}

		[TestMethod]
		public void Dictionary_Handles_Unnamed_Columns() {
			var result = QueryHelper.ExecuteDic("select count(*) from [Full]");

			Assert.IsNotNull(result);
			Assert.IsNotNull(result.First());

			Console.WriteLine(result.First().Keys.First());
			Console.WriteLine(result.First().Values.First());
		}
	}
}
