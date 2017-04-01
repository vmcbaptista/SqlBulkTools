using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SqlBulkTools.TestCommon.Model
{
    public class CustomColumnMappingTest
    {
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        [Key]
        public int NaturalIdTest { get; set; }

        [Column("ColumnX"), StringLength(256)]
        public string ColumnXIsDifferent { get; set; }

        [Column("ColumnY")]
        public int ColumnYIsDifferentInDatabase { get; set; }
    }
}
