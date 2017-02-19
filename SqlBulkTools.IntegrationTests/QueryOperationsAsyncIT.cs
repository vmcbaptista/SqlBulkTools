using System.Collections.Generic;
using System.Configuration;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using NUnit.Framework;
using SqlBulkTools.Core;
using SqlBulkTools.Enumeration;
using SqlBulkTools.IntegrationTests.Data;
using SqlBulkTools.IntegrationTests.Model;
using TestContext = SqlBulkTools.IntegrationTests.Data.TestContext;

namespace SqlBulkTools.IntegrationTests
{
    [TestFixture]
    class QueryOperationsAsyncIT
    {

        private BookRandomizer _randomizer;
        private TestContext _db;

        [OneTimeSetUp]
        public void Setup()
        {
            _db = new TestContext();
            _randomizer = new BookRandomizer();
            Database.SetInitializer(new DatabaseInitialiser());
            _db.Database.Initialize(true);
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _db.Dispose();
        }

        [Test]
        public async Task SqlBulkTools_UpdateQuery_SetPriceOnSingleEntity()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookToTest = books[5];
            bookToTest.Price = 50;
            var isbn = bookToTest.ISBN;
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                        
                    // Update price to 100

                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { Price = 100 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.IsTrue(updatedRecords == 1);
            Assert.AreEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
        }

        [Test]
        public async Task SqlBulkTools_UpdateQuery_SetPriceAndDescriptionOnSingleEntity()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookToTest = books[5];
            bookToTest.Price = 50;
            var isbn = bookToTest.ISBN;

            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Update price to 100

                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book()
                        {
                            Price = 100,
                            Description = "Somebody will want me now! Yay"
                        })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }



            Assert.IsTrue(updatedRecords == 1);
            Assert.AreEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
            Assert.AreEqual("Somebody will want me now! Yay", _db.Books.Single(x => x.ISBN == isbn).Description);
        }

        [Test]
        public async Task SqlBulkTools_UpdateQuery_MultipleConditionsTrue()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 20)
                {
                    books[i].Price = 15;
                }
                else
                    books[i].Price = 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { Price = 100, WarehouseId = 5 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.WarehouseId)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .And(x => x.Price == 15)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, updatedRecords);
            Assert.AreEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
            Assert.AreEqual(5, _db.Books.Single(x => x.ISBN == isbn).WarehouseId);
        }

        [Test]
        public async Task SqlBulkTools_UpdateQuery_MultipleConditionsFalse()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 20)
                {
                    books[i].Price = 15;
                }
                else
                    books[i].Price = 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    // Update price to 100

                    updatedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { Price = 100, WarehouseId = 5 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.WarehouseId)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .And(x => x.Price == 16)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(updatedRecords == 0);
        }

        [Test]
        public async Task SqlBulkTools_DeleteQuery_DeleteSingleEntity()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookIsbn = books[5].ISBN;
            int deletedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    deletedRecords = await bulk.Setup<Book>()
                        .ForDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .Where(x => x.ISBN == bookIsbn)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }


            Assert.IsTrue(deletedRecords == 1);
            Assert.AreEqual(29, _db.Books.Count());
        }

        [Test]
        public async Task SqlBulkTools_DeleteQuery_DeleteWhenNotNullWithSchema()
        {
            _db.SchemaTest2.RemoveRange(_db.SchemaTest2.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            List<SchemaTest2> col = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(col)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);


                    await bulk.Setup<SchemaTest2>()
                        .ForDeleteQuery()
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .Delete()
                        .Where(x => x.ColumnA != null)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }


            Assert.AreEqual(0, _db.SchemaTest2.Count());
        }



        [Test]
        public async Task SqlBulkTools_DeleteQuery_DeleteWhenNullWithWithSchema()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            List<SchemaTest2> col = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = null });
            }

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<SchemaTest2>()
                        .ForCollection(col)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    await bulk.Setup<SchemaTest2>()
                        .ForDeleteQuery()
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .Delete()
                        .Where(x => x.ColumnA == null)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(0, _db.SchemaTest2.Count());
        }

        [Test]
        public async Task SqlBulkTools_DeleteQuery_DeleteWithMultipleConditions()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 6)
                {
                    books[i].Price = 1 + (i * 100);
                    books[i].WarehouseId = 1;
                    books[i].Description = null;
                }
            }

            int deletedRecords = 0;

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    await bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .CommitAsync(conn);

                    deletedRecords = await bulk.Setup<Book>()
                        .ForDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .Where(x => x.WarehouseId == 1)
                        .And(x => x.Price >= 100)
                        .And(x => x.Description == null)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.AreEqual(5, deletedRecords);
            Assert.AreEqual(25, _db.Books.Count());
        }

        [Test]
        public async Task SqlBulkTools_Insert_ManualAddColumn()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            int insertedRecords = 0;
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    insertedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book() { BestSeller = true, Description = "Greatest dad in the world", Title = "Hello World", ISBN = "1234567", Price = 23.99M })
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.BestSeller)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Price)
                        .Insert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public async Task SqlBulkTools_Insert_AddAllColumns()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            int insertedRecords = 0;
            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    insertedRecords = await bulk.Setup<Book>()
                        .ForObject(new Book()
                        {
                            BestSeller = true,
                            Description = "Greatest dad in the world",
                            Title = "Hello World",
                            ISBN = "1234567",
                            Price = 23.99M
                        })
                        .WithTable("Books")
                        .AddAllColumns()
                        .Insert()
                        .SetIdentityColumn(x => x.Id)
                        .CommitAsync(conn);

                }

                trans.Complete();
            }

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public async Task SqlBulkTools_Upsert_AddAllColumns()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            using (TransactionScope tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection con = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {                                     
                    var bulk = new BulkOperations();
                    await bulk.Setup<Book>()
                    .ForObject(new Book()
                    {
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello World",
                        ISBN = "1234567",
                        Price = 23.99M
                    })
                    .WithTable("Books")
                    .AddAllColumns()
                    .Upsert()
                    .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                    .MatchTargetOn(x => x.Id)
                    .CommitAsync(con);
                }

                tx.Complete();
            }

            Assert.AreEqual(1, _db.Books.Count());
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public async Task SqlBulkTools_Upsert_AddAllColumnsWithExistingRecord()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection con = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    Book book = new Book()
                    {
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello World",
                        ISBN = "1234567",
                        Price = 23.99M
                    };

                    await bulk.Setup<Book>()
                        .ForObject(book)
                        .WithTable("Books")
                        .AddAllColumns()
                        .Insert()
                        .SetIdentityColumn(x => x.Id, ColumnDirectionType.InputOutput)
                        .CommitAsync(con);

                    await bulk.Setup<Book>()
                    .ForObject(new Book()
                    {
                        Id = book.Id,
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello Greggo",
                        ISBN = "1234567",
                        Price = 23.99M
                    })
                    .WithTable("Books")
                    .AddAllColumns()
                    .Upsert()
                    .SetIdentityColumn(x => x.Id, ColumnDirectionType.Input)
                    .MatchTargetOn(x => x.Id)
                    .CommitAsync(con);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, _db.Books.Count());
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.Title == "Hello Greggo"));
        }

        [Test]
        public async Task SqlBulkTools_Insert_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalId = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalId = 1,
                ColumnXIsDifferent = $"ColumnX 1",
                ColumnYIsDifferentInDatabase = 1
            };

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .Insert()
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.First().ColumnXIsDifferent == "ColumnX 1");
        }

        [Test]
        public async Task SqlBulkTools_Upsert_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalId = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 3
            };

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()                        
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")                        
                        .Upsert()
                        .MatchTargetOn(x => x.NaturalId)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.First().ColumnYIsDifferentInDatabase == 3);
        }

        [Test]
        public async Task SqlBulkTools_Update_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalId = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalId = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 1
            };

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            using (TransactionScope trans = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .Insert()
                        .CommitAsync(conn);

                    customColumn.ColumnXIsDifferent = "updated";


                    await bulk.Setup<CustomColumnMappingTest>()
                        .ForObject(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .Update()
                        .Where(x => x.NaturalId == 1)
                        .CommitAsync(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.First().ColumnXIsDifferent == "updated");
        }
    }
}