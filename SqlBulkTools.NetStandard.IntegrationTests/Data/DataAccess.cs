using System.Collections.Generic;
using System.Configuration;
using Microsoft.Data.SqlClient;
using System.Linq;
using SqlBulkTools.TestCommon.Model;
using SqlBulkTools.NetStandard.IntegrationTests;
using Microsoft.Extensions.Configuration;
using Crane;
using Crane.SqlServer;

namespace SqlBulkTools.IntegrationTests.Data
{
    public class DataAccess
    {
        public List<Book> GetBookList(string isbn = null)
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            
            var books = conn.Query()
                .AddSqlParameter("@Isbn", isbn)
                .ExecuteReader<Book>("dbo.GetBooks")
                .ToList();

            return books;
            
        }

        public int GetBookCount()
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                var bookCount = conn.Query()
                    .ExecuteScalar<int>("dbo.GetBookCount");
                return bookCount;
            }
        }

        public List<SchemaTest1> GetSchemaTest1List()
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                var schemaTestList = conn.Query()
                    .AddSqlParameter("@Schema", "dbo")
                    .ExecuteReader<SchemaTest1>("dbo.GetSchemaTest")
                    .ToList();

                return schemaTestList;
            }
        }

        public List<SchemaTest2> GetSchemaTest2List()
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                var schemaTestList = conn.Query()
                    .AddSqlParameter("@Schema", "AnotherSchema")
                    .ExecuteReader<SchemaTest2>("dbo.GetSchemaTest")
                    .ToList();

                return schemaTestList;
            }
        }

        public List<CustomColumnMappingTest> GetCustomColumnMappingTests()
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                var customColumnMappingTests = conn                  
                    .Query()
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.NaturalIdTest, "NaturalId")
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnXIsDifferent, "ColumnX")
                    .CustomColumnMapping<CustomColumnMappingTest>(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                    .ExecuteReader<CustomColumnMappingTest>("dbo.GetCustomColumnMappingTests")                                        
                    .ToList();

                return customColumnMappingTests;
            }
        }

        public List<ReservedColumnNameTest> GetReservedColumnNameTests()
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                var reservedColumnNameTests = conn
                    .Query()
                    .ExecuteReader<ReservedColumnNameTest>("dbo.GetReservedColumnNameTests")
                    .ToList();

                return reservedColumnNameTests;
            }
        }

        public int GetComplexTypeModelCount()
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                return conn.Query()
                    .ExecuteScalar<int>("dbo.GetComplexModelCount");
            }
        }

        public void ReseedBookIdentity(int idStart)
        {
           SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                conn.Command()
                    .AddSqlParameter("@IdStart", idStart)
                    .ExecuteNonQuery("dbo.ReseedBookIdentity");
            }
        }

        public List<CustomIdentityColumnNameTest> GetCustomIdentityColumnNameTestList()
        {
            SqlServerAccess conn = new SqlServerAccess(ConfigurationHelpers.GetConfiguration().GetConnectionString("SqlBulkToolsTest"));
            {
                return conn.Query()
                    .CustomColumnMapping<CustomIdentityColumnNameTest>(x => x.Id, "ID_COMPANY")
                    .ExecuteReader<CustomIdentityColumnNameTest>("dbo.GetCustomIdentityColumnNameTestList")
                    .ToList();
            }
        }
    }
}
