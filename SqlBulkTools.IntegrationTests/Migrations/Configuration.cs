using System.Data.Entity.Migrations;
using SqlBulkTools.IntegrationTests2.Data;

namespace SqlBulkTools.IntegrationTests2.Migrations
{
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
