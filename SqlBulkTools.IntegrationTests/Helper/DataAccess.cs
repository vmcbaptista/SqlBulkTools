using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using SprocMapperLibrary;
using SqlBulkTools.TestCommon.Model;

namespace SqlBulkTools.IntegrationTests.Helper
{
    public class DataAccess
    {
        public List<Book> GetBookList(string isbn = null)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var books = conn.Select()
                    .AddSqlParameter("@Isbn", isbn)
                    .ExecuteReader<Book>(conn, "dbo.GetBooks", true)
                    .ToList();

                return books;
            }
        }

        public int GetBookCount()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var bookCount = conn.Procedure()
                    .ExecuteScalar<int>(conn, "dbo.GetBookCount");
                return bookCount;
            }
        }

        public List<SchemaTest1> GetSchemaTest1List()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var schemaTestList = conn.Select()
                    .AddSqlParameter("@Schema", "dbo")
                    .ExecuteReader<SchemaTest1>(conn, "dbo.GetSchemaTest", true)
                    .ToList();

                return schemaTestList;
            }
        }

        public List<SchemaTest2> GetSchemaTest2List()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var schemaTestList = conn.Select()
                    .AddSqlParameter("@Schema", "AnotherSchema")
                    .ExecuteReader<SchemaTest2>(conn, "dbo.GetSchemaTest", true)
                    .ToList();

                return schemaTestList;
            }
        }

        public List<CustomColumnMappingTest> GetCustomColumnMappingTests()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var customColumnMappingTests = conn                  
                    .Select()
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnXIsDifferent, "ColumnX")
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                    .ExecuteReader<CustomColumnMappingTest>(conn, "dbo.GetCustomColumnMappingTests")                                        
                    .ToList();

                return customColumnMappingTests;
            }
        }

        public List<ReservedColumnNameTest> GetReservedColumnNameTests()
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                var reservedColumnNameTests = conn
                    .Select()
                    .ExecuteReader<ReservedColumnNameTest>(conn, "dbo.GetReservedColumnNameTests")
                    .ToList();

                return reservedColumnNameTests;
            }
        }

        public void ReseedBookIdentity(int idStart)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            {
                conn.Procedure()
                    .AddSqlParameter("@IdStart", idStart)
                    .ExecuteNonQuery(conn, "dbo.ReseedBookIdentity");
            }
        }
    }
}
