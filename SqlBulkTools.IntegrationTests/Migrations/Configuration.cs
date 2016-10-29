using SqlBulkTools.IntegrationTests.Data;

namespace SqlBulkTools.IntegrationTests.Migrations
{
    using System.Data.Entity.Migrations;

    internal sealed class Configuration : DbMigrationsConfiguration<TestContext>
    {
        public Configuration()
        {
            AutomaticMigrationsEnabled = true;
        }

        protected override void Seed(TestContext context)
        {

        }
    }
}
